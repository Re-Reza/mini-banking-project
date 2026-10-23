// serverA.js
// اجرا:
// node --env-file=.env serverA.js
// node --env-file=.env serverA.js
// تنظیمات اختیاری در Linux:
// PORT=5000 MAX_CLIENTS=30 DB_PATH=./bank.sqlite node serverA.js
//
// قرارداد پیام: هر درخواست یک JSON است که با newline تمام می‌شود.
// {"action":"withdraw","account":"A1001","amount":100}\n
//
// مبالغ: عدد صحیح مثبت در واحد پول انتخابی پروژه.
// در حالت Raft، request_id اختیاری کلاینت برای جلوگیری از اجرای دوباره ذخیره می‌شود.

import net from 'node:net';
import { DatabaseSync } from 'node:sqlite';
import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { appendFileSync, mkdirSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { RaftNode } from './raft.js';

// ---------------- تنظیمات ----------------

function positiveInteger(value, fallback, name, max) {
  const number = value === undefined ? fallback : Number(value);

  if (!Number.isSafeInteger(number) || number < 1 || number > max) {
    throw new Error(`Invalid configuration: ${name}`);
  }

  return number;
}

const HOST = process.env.HOST ?? '0.0.0.0';
const PORT = positiveInteger(process.env.PORT, 5000, 'PORT', 65535);
// هویت گره باید در اجرای چند سرور یکتا باشد؛ شناسه در گزارش‌ها کمک می‌کند
// درخواست‌ها و پاسخ‌های هر عضو خوشه از هم قابل تشخیص باشند.
const NODE_ID = process.env.NODE_ID ?? `node-${PORT}`;
const CONSENSUS_MODE = process.env.CONSENSUS_MODE ?? 'raft';
const RAFT_PORT = positiveInteger(process.env.RAFT_PORT, PORT + 1000, 'RAFT_PORT', 65535);
let PEERS = [];
if (CONSENSUS_MODE === 'raft') {
  try { PEERS = JSON.parse(process.env.RAFT_PEERS ?? '[]'); }
  catch { throw new Error('RAFT_PEERS must be a JSON array of {id,host,port} objects.'); }
  if (!Array.isArray(PEERS) || PEERS.length < 2 || PEERS.some(peer =>
    !peer || typeof peer.id !== 'string' || typeof peer.host !== 'string' ||
    !Number.isInteger(peer.port) || peer.port < 1 || peer.port > 65535
  )) throw new Error('Raft mode requires RAFT_PEERS to list all other members (at least two).');
  if ((PEERS.length + 1) % 2 === 0 || new Set(PEERS.map(peer => peer.id)).size !== PEERS.length) {
    throw new Error('Raft requires an odd number of members and unique peer IDs.');
  }
  if (PEERS.some(peer => peer.id === NODE_ID)) throw new Error('RAFT_PEERS must not repeat NODE_ID.');
} else if (CONSENSUS_MODE !== 'standalone') {
  throw new Error('CONSENSUS_MODE must be raft or standalone.');
}

const MAX_CLIENTS = positiveInteger(
  process.env.MAX_CLIENTS, 30, 'MAX_CLIENTS', 10000
);

const DB_PATH = resolve(process.env.DB_PATH ?? './bank.sqlite');
const LOG_PATH = resolve(process.env.LOG_PATH ?? './logs/requests.jsonl');
const MAX_MESSAGE_BYTES = 8192;
const IDLE_TIMEOUT_MS = 120000;
const MAX_MONEY = Number.MAX_SAFE_INTEGER;

// فقط برای آزمایش rollback؛ در حالت عادی غیرفعال است.
const ALLOW_TEST_FAULTS = process.env.ALLOW_TEST_FAULTS === '1';

// پوشه و فایل گزارش خودکار ساخته می‌شوند؛ گزارش‌های قبلی حفظ می‌شوند.
// خطای مسیر یا مجوز نوشتن، پیش از پذیرش درخواست مشخص می‌شود.
mkdirSync(dirname(LOG_PATH), { recursive: true });
appendFileSync(LOG_PATH, '', { encoding: 'utf8', flush: true });

// ---------------- راه‌اندازی دیتابیس ----------------

mkdirSync(dirname(DB_PATH), { recursive: true });

const db = new DatabaseSync(DB_PATH);

// WAL و FULL برای ثبت پایدار تراکنش‌های commit‌شده.
// busy_timeout زمان انتظار برای آزادشدن قفل دیتابیس است.
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = FULL;
  PRAGMA busy_timeout = 5000;

  CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY NOT NULL,
    owner_name TEXT NOT NULL,
    balance INTEGER NOT NULL
      CHECK(balance BETWEEN 0 AND 9007199254740991)
  ) STRICT;
`);

// اجرای یک تابع داخل تراکنش.
// بین BEGIN و COMMIT هیچ await یا عملیات شبکه‌ای قرار نمی‌دهیم.
function transaction(operation) {
  db.exec('BEGIN IMMEDIATE');

  try {
    const result = operation();
    db.exec('COMMIT');

    // فقط پس از موفقیت COMMIT نتیجه برگردانده می‌شود.
    return result;
  } catch (error) {
    if (db.isTransaction) {
      db.exec('ROLLBACK');
    }

    throw error;
  }
}

// حساب‌های ساختگی فقط در اولین اجرا ساخته می‌شوند.
// راه‌اندازی مجدد، موجودی حساب‌های موجود را بازنشانی نمی‌کند.
transaction(() => {
  const insert = db.prepare(`
    INSERT OR IGNORE INTO accounts
      (account_id, owner_name, balance)
    VALUES (?, ?, ?)
  `);

  insert.run('A1001', 'Ali', 1000);
  insert.run('A1002', 'Sara', 2000);
  insert.run('A1003', 'Reza', 3000);
});

// ---------------- خطا و اعتبارسنجی ----------------

class RequestError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

function fail(code, message) {
  throw new RequestError(code, message);
}

function validateRequest(request) {
  if (
    request === null ||
    typeof request !== 'object' ||
    Array.isArray(request)
  ) {
    fail('INVALID_REQUEST', 'Request must be a JSON object.');
  }

  for (const field of ['action', 'account']) {
    if (!Object.hasOwn(request, field)) {
      fail('MISSING_FIELD', `Missing field: ${field}`);
    }
  }

  if (!['balance', 'deposit', 'withdraw'].includes(request.action)) {
    fail('UNSUPPORTED_ACTION', 'Unsupported action.');
  }

  if (
    typeof request.account !== 'string' ||
    request.account.trim().length === 0
  ) {
    fail('INVALID_ACCOUNT', 'Account must be a non-empty string.');
  }

  if (request.action !== 'balance') {
    if (!Object.hasOwn(request, 'amount')) {
      fail('MISSING_FIELD', 'Missing field: amount');
    }

    if (
      !Number.isSafeInteger(request.amount) ||
      request.amount <= 0
    ) {
      fail('INVALID_AMOUNT', 'Amount must be a positive safe integer.');
    }
  }

  if (Object.hasOwn(request, 'test_fault')) {
    if (
      !ALLOW_TEST_FAULTS ||
      request.test_fault !== 'after_update' ||
      request.action === 'balance'
    ) {
      fail('INVALID_TEST_FAULT', 'Fault injection is not permitted.');
    }
  }
}

function getAccount(accountId) {
  const account = db.prepare(`
    SELECT account_id, balance
    FROM accounts
    WHERE account_id = ?
  `).get(accountId);

  if (!account) {
    fail('UNKNOWN_ACCOUNT', 'Account does not exist.');
  }

  return account;
}

// ---------------- توابع عملیات بانکی ----------------

// یک SELECT مستقل، برای این استعلام به تراکنش صریح نیاز ندارد.
function balanceInquiry(request) {
  return getAccount(request.account);
}

function deposit(request) {
    const account = getAccount(request.account);

    // جلوگیری از عبور موجودی از محدوده عدد صحیح دقیق JavaScript.
    if (request.amount > MAX_MONEY - account.balance) {
      fail('BALANCE_LIMIT', 'Resulting balance exceeds the supported limit.');
    }

    db.prepare(`
      UPDATE accounts
      SET balance = balance + ?
      WHERE account_id = ?
    `).run(request.amount, request.account);

    // خطای عمدی پس از UPDATE و پیش از COMMIT.
    // transaction تمام تغییرات این عملیات را rollback می‌کند.
    if (request.test_fault === 'after_update') {
      fail('TEST_FAILURE', 'Injected failure; transaction rolled back.');
    }

    return getAccount(request.account);
}

function withdraw(request) {
    // برای تشخیص جداگانه خطای حساب نامعتبر.
    getAccount(request.account);

    // بررسی موجودی و کاهش آن در خود دستور SQL انجام می‌شوند.
    const result = db.prepare(`
      UPDATE accounts
      SET balance = balance - ?
      WHERE account_id = ?
        AND balance >= ?
    `).run(request.amount, request.account, request.amount);

    if (result.changes === 0) {
      fail('INSUFFICIENT_FUNDS', 'Insufficient balance.');
    }

    if (request.test_fault === 'after_update') {
      fail('TEST_FAILURE', 'Injected failure; transaction rolled back.');
    }

    return getAccount(request.account);
}

// مرز ماشین حالت بانکی:
// در معماری Raft، ورودی این تابع باید فقط فرمانی باشد که رهبر پس از اجماع
// ثبت کرده است و همین تابع باید روی همهٔ گره‌ها با همان ترتیب اجرا شود.
// قواعد اعتبارسنجی و عملیات بانکی در این تابع بدون تغییر باقی مانده‌اند.
function applyBankCommand(request) {
  validateRequest(request);

  switch (request.action) {
    case 'balance':
      return balanceInquiry(request);

    case 'deposit':
      return CONSENSUS_MODE === 'standalone'
        ? transaction(() => deposit(request)) : deposit(request);

    case 'withdraw':
      return CONSENSUS_MODE === 'standalone'
        ? transaction(() => withdraw(request)) : withdraw(request);
  }
}

// اجرای فرمان commit‌شده و ثبت lastApplied در همان تراکنش SQLite.
// در صورت خطای بانکی، تغییرات عملیات rollback می‌شوند ولی نتیجهٔ رد درخواست
// و شمارهٔ لاگ اعمال‌شده ثبت می‌شود تا گره در replay متوقف یا دوباره‌کاری نکند.
function applyRaftCommand(payload, logIndex) {
  if (payload.type === 'noop') {
    transaction(() => raft.setMeta('lastApplied', logIndex));
    return { status: 'success', request_id: payload.request_id, noop: true };
  }

  db.exec('BEGIN IMMEDIATE');
  const prior = db.prepare('SELECT response_json FROM raft_results WHERE request_id=?').get(payload.request_id);
  if (prior) {
    raft.setMeta('lastApplied', logIndex);
    db.exec('COMMIT');
    return JSON.parse(prior.response_json);
  }
  db.exec('SAVEPOINT bank_command');
  let response;
  try {
    const result = applyBankCommand(payload.request);
    response = {
      status: 'success', request_id: payload.request_id,
      account: result.account_id, balance: result.balance
    };
    db.exec('RELEASE bank_command');
  } catch (error) {
    db.exec('ROLLBACK TO bank_command');
    db.exec('RELEASE bank_command');
    response = {
      status: 'failed', request_id: payload.request_id,
      error: {
        code: error instanceof RequestError ? error.code : 'STORAGE_ERROR',
        message: error instanceof RequestError ? error.message : 'Storage error.'
      }
    };
  }
  db.prepare('INSERT INTO raft_results(request_id,response_json) VALUES(?,?)')
    .run(payload.request_id, JSON.stringify(response));
  raft.setMeta('lastApplied', logIndex);
  db.exec('COMMIT');
  return response;
}

// ---------------- پردازش پیام و ثبت گزارش ----------------

// هر خط فایل یک شیء JSON مستقل شامل درخواست و پاسخ است.
// گزارش هم در فایل ذخیره می‌شود و هم برای نمایش در ترمینال چاپ می‌شود.
function logRequest(entry) {
  const line = JSON.stringify(entry);

  try {
    appendFileSync(LOG_PATH, line + '\n', {
      encoding: 'utf8',
      flush: true
    });
  } catch (error) {
    // خطای گزارش‌گیری نباید تراکنش ثبت‌شده را ناموفق اعلام کند.
    // رکورد همچنان در خروجی برنامه چاپ می‌شود و خطای فایل اعلام می‌شود.
    console.error(JSON.stringify({
      event: 'log_write_error',
      request_id: entry.request_id,
      timestamp: new Date().toISOString(),
      log_path: LOG_PATH,
      message: error.message
    }));
  }

  console.log(line);
}

async function handleMessage(socket, bytes, forcedError = null) {
  const started = performance.now();
  const timestamp = new Date().toISOString();

  // برای هر دریافت، شناسه جدید ساخته می‌شود؛ این deduplication نیست.
  let requestId = randomUUID();
  let request = null;
  let response;

  try {
    if (forcedError) {
      throw forcedError;
    }

    try {
      const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
      request = JSON.parse(text);
    } catch {
      fail('MALFORMED_REQUEST', 'Expected valid UTF-8 JSON.');
    }

    if (request && typeof request === 'object' && !Array.isArray(request) &&
        Object.hasOwn(request, 'request_id')) {
      if (typeof request.request_id !== 'string' || request.request_id.length < 1 || request.request_id.length > 128) {
        fail('INVALID_REQUEST_ID', 'request_id must be a string between 1 and 128 characters.');
      }
      requestId = request.request_id;
    }

    if (CONSENSUS_MODE === 'raft') {
      response = await raft.submit({ type: 'bank', request_id: requestId, request });
    } else {
      const result = applyBankCommand(request);
      response = {
        status: 'success', request_id: requestId,
        account: result.account_id, balance: result.balance
      };
    }
  } catch (error) {
    if (!(error instanceof RequestError)) {
      console.error(JSON.stringify({
        event: 'internal_error',
        request_id: requestId,
        message: error.message
      }));
    }

    response = {
      status: error instanceof RequestError ? 'failed' : 'unknown',
      request_id: requestId,
      error: {
        code: error instanceof RequestError
          ? error.code
          : 'STORAGE_ERROR',
        message: error instanceof RequestError
          ? error.message
          : 'Storage error. Check server state before retrying.'
      }
    };
  }

  // ثبت درخواست و پاسخ، حتی اگر اتصال پیش از ارسال پاسخ قطع شده باشد.
  logRequest({
    event: 'request',
    node_id: NODE_ID,
    request_id: requestId,
    client_ip: socket.remoteAddress ?? 'unknown',
    action: request?.action ?? null,
    account_id: request?.account ?? null,
    timestamp,
    result: response.status,
    latency_ms: performance.now() - started,
    request,
    raw_base64: bytes.toString('base64'),
    response
  });

  if (!socket.destroyed && socket.writable && !socket.writableEnded) {
    socket.write(JSON.stringify(response) + '\n');
  }
}

// ---------------- سرور TCP روی یک پورت ----------------

let stopping = false;

const server = net.createServer({ allowHalfOpen: true }, socket => {
  let buffer = Buffer.alloc(0);
  let closing = false;
  let socketQueue = Promise.resolve();

  const enqueueMessage = (bytes, forcedError = null) => {
    socketQueue = socketQueue
      .then(() => handleMessage(socket, bytes, forcedError))
      .catch(() => {});
  };

  socket.setNoDelay(true);
  socket.setTimeout(IDLE_TIMEOUT_MS);

  socket.on('error', error => {
    console.error(JSON.stringify({
      event: 'socket_error',
      message: error.message
    }));
  });

  // رد پیام بزرگ و نگهداری فقط بخش محدود آن برای گزارش.
  function rejectLargeMessage() {
    closing = true;
    socket.pause();

    const bytes = buffer.subarray(0, MAX_MESSAGE_BYTES);
    buffer = Buffer.alloc(0);

    enqueueMessage(bytes, new RequestError(
      'MESSAGE_TOO_LARGE',
      'Message exceeds the size limit; logged input is truncated.'
    ));

    socket.end();
  }

  function recordIncomplete(message) {
    if (buffer.length === 0) return;

    const bytes = buffer;
    buffer = Buffer.alloc(0);

    enqueueMessage(bytes, new RequestError(
      'INCOMPLETE_REQUEST', message
    ));
  }

  socket.on('data', chunk => {
    if (closing || stopping) return;

    // TCP جریان بایت است؛ هر chunk الزاماً یک درخواست کامل نیست.
    buffer = Buffer.concat([buffer, chunk]);

    while (!closing) {
      const newline = buffer.indexOf(10); // بایت \n

      if (newline === -1) {
        if (buffer.length > MAX_MESSAGE_BYTES) {
          rejectLargeMessage();
        }
        break;
      }

      if (newline > MAX_MESSAGE_BYTES) {
        rejectLargeMessage();
        break;
      }

      const bytes = buffer.subarray(0, newline);
      buffer = buffer.subarray(newline + 1);

      // هر درخواست تا اجماع شبکه‌ای منتظر می‌ماند؛ درخواست‌های هم‌زمان
      // در ترتیب لاگ Raft اعمال می‌شوند.
      enqueueMessage(bytes);

      // جلوگیری از رشد شدید خروجی برای کلاینتی که پاسخ نمی‌خواند.
      if (socket.writableLength > 1024 * 1024) {
        closing = true;
        socket.destroy();
      }
    }
  });
  
  socket.on('end', () => {
    closing = true;
    recordIncomplete('Connection ended before newline.');
    socket.end();
  });

  socket.on('timeout', () => {
    closing = true;
    recordIncomplete('Connection timed out before newline.');
    socket.destroy();
  });

  socket.on('close', () => {
    closing = true;
    recordIncomplete('Connection closed before newline.');
  });
});

// محدودیت اتصال توسط خود Node.js؛ بدون شمارنده یا Set.
server.maxConnections = MAX_CLIENTS;

server.on('drop', info => {
  console.log(JSON.stringify({
    event: 'connection_rejected',
    reason: 'MAX_CONNECTIONS',
    client_ip: info?.remoteAddress ?? 'unknown',
    timestamp: new Date().toISOString()
  }));
});

server.on('error', error => {
  console.error('SERVER_ERROR:', error.message);
  db.close();
  process.exit(1);
});

let raft;
if (CONSENSUS_MODE === 'raft') {
  raft = new RaftNode({
    id: NODE_ID,
    host: HOST,
    port: RAFT_PORT,
    peers: PEERS,
    db,
    applyCommand: applyRaftCommand,
    onClientRequest: payload => raft.proposeAsLeader(payload)
  });
  if (raft.lastIndex() === 0 && Number(raft.meta('lastApplied')) === 0) {
    const initial = db.prepare('SELECT account_id,owner_name,balance FROM accounts ORDER BY account_id').all();
    const expected = [
      { account_id: 'A1001', owner_name: 'Ali', balance: 1000 },
      { account_id: 'A1002', owner_name: 'Sara', balance: 2000 },
      { account_id: 'A1003', owner_name: 'Reza', balance: 3000 }
    ];
    if (JSON.stringify(initial) !== JSON.stringify(expected)) {
      throw new Error('Raft bootstrap requires the untouched seed database on every node. Existing standalone data must be migrated consistently before enabling Raft.');
    }
  }
  raft.start();
}

server.listen(PORT, HOST, () => {
  console.log(JSON.stringify({
    event: 'listening',
    host: HOST,
    port: PORT,
    node_id: NODE_ID,
    consensus: CONSENSUS_MODE,
    raft_port: CONSENSUS_MODE === 'raft' ? RAFT_PORT : null,
    raft_peers: CONSENSUS_MODE === 'raft' ? PEERS.map(peer => peer.id) : [],
    max_clients: MAX_CLIENTS,
    database: DB_PATH,
    log_path: LOG_PATH
  }));
});

// عملیات دیتابیس همگام است؛ handler سیگنال وسط یک تابع بانکی اجرا نمی‌شود.
// اتصال‌های موجود فرصت پایان دارند؛ پس از سه ثانیه برنامه خارج می‌شود.
function shutdown() {
  if (stopping) return;
  stopping = true;
  raft?.close();

  server.close(() => {
    db.close();
    process.exit(0);
  });

  setTimeout(() => {
    db.close();
    process.exit(0);
  }, 3000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
