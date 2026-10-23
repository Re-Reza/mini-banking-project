import net from 'node:net';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { mkdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';

const HELP = `
Usage:
  node client.mjs suite --host SERVER_IP[,SERVER_IP2,...] [--clients 10] [--requests 100]
  node client.mjs suite --host SERVER_IP --test 3
  node client.mjs send --host SERVER_IP --json '{"action":"balance","account":"A1001"}'
  node client.mjs send --host SERVER_IP --raw '{broken json'
  node client.mjs durability-prepare --host SERVER_IP
  node client.mjs durability-check --host SERVER_IP --confirm-restart
  node client.mjs cluster-demo --host IP1,IP2,IP3 [--requests 60] [--interval 250]

Options:
  --host ADDRESS      Required: server VM IP(s), comma-separated (e.g. 192.168.56.102,192.168.56.105).
                      Each entry may also specify a custom port as HOST:PORT.
  --port NUMBER       Default server port when not specified in --host, default 5000.
  --account ID        Test account, default A1003.
  --clients NUMBER    Simultaneous connections, default 10 (at least 2).
  --requests NUMBER   Requests per concurrency test / cluster-demo, default 100 (at least 2).
  --interval NUMBER   Delay (ms) between requests in cluster-demo mode, default 250.
  --test NUMBER|all   Run one suite test (1-7), or all tests (default).
  --timeout NUMBER    Connect/response timeout in milliseconds, default 15000.
  --report PATH       JSON report, default client-report.json (overwritten).
  --checkpoint PATH   Crash-test state, default durability-checkpoint.json.
  --confirm-restart   Confirm you killed and restarted the server before checking.
  --help              Show this help.

Suite tests:
  1 normal operations          2 required error cases
  3 concurrent deposits        4 concurrent withdrawals
  5 competing withdrawals      6 mixed deposits/withdrawals
  7 rollback and concurrent reads

Modes:
  suite               Run automated functional, concurrency, and ACID test suite.
  send                Send a single JSON or raw payload.
  durability-prepare  Apply changes and write checkpoint before SIGKILL.
  durability-check    Verify committed state after server restart.
  cluster-demo        Continuous paced transactions with automatic failover & idempotent retries
                      for demonstrating Bonus 5 (Failover) and Bonus 6 (Split-Brain/Partition).

Exit codes: 0 passed, 1 failed/uncertain, 2 incomplete (rollback test skipped).
`;

const RETRYABLE_CLUSTER_CODES = new Set([
  'NO_LEADER',
  'LEADER_UNAVAILABLE',
  'NO_QUORUM'
]);

const sleep = ms => new Promise(resolveDelay => setTimeout(resolveDelay, ms));

// One connection can carry multiple requests. Replies are matched in FIFO order,
// as required by this server's newline-delimited protocol.
export class BankConnection {
  constructor(config, socketFactory = options => net.createConnection(options)) {
    this.config = config;
    this.socketFactory = socketFactory;
    this.pending = [];
    this.buffer = '';
    this.failure = null;
    this.connected = false;
  }

  async connect() {
    this.socket = this.socketFactory({ host: this.config.host, port: this.config.port });
    this.socket.setEncoding('utf8');
    this.socket.setNoDelay(true);
    this.closed = new Promise(resolveClose => {
      this.socket.once('close', () => {
        this.connected = false;
        this.fail(new Error('Connection closed. A write without a reply has an UNKNOWN outcome.'));
        resolveClose();
      });
    });
    this.socket.on('data', chunk => this.receive(chunk));
    this.socket.on('error', error => this.fail(error));
    await new Promise((resolveConnect, rejectConnect) => {
      const timer = setTimeout(() => this.fail(new Error('Connection timed out.')), this.config.timeout);
      const cleanup = () => {
        clearTimeout(timer);
        this.socket.off('connect', onConnect);
        this.socket.off('error', onError);
        this.socket.off('close', onClose);
      };
      const onConnect = () => {
        cleanup();
        this.connected = true;
        resolveConnect();
      };
      const onError = error => { cleanup(); rejectConnect(error); };
      const onClose = () => { cleanup(); rejectConnect(this.failure ?? new Error('Connection closed.')); };
      this.socket.once('connect', onConnect);
      this.socket.once('error', onError);
      this.socket.once('close', onClose);
    });
    return this;
  }

  fail(error) {
    this.failure ??= error;
    for (const job of this.pending.splice(0)) {
      clearTimeout(job.timer);
      job.reject(this.failure);
    }
    if (this.socket && !this.socket.destroyed) this.socket.destroy();
  }

  receive(chunk) {
    this.buffer += chunk;
    let newline;
    while ((newline = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, newline);
      this.buffer = this.buffer.slice(newline + 1);
      const job = this.pending.shift();
      if (!job) {
        this.fail(new Error('Unexpected response with no pending request.'));
        return;
      }
      clearTimeout(job.timer);
      try {
        const response = JSON.parse(line);
        assert.ok(response && typeof response === 'object' && !Array.isArray(response));
        assert.equal(typeof response.request_id, 'string');
        assert.ok(['success', 'failed', 'unknown'].includes(response.status));
        job.resolve(response);
      } catch (error) {
        job.reject(error);
        this.fail(error);
        return;
      }
    }
    if (this.buffer.length > 1024 * 1024) this.fail(new Error('Response exceeds size limit.'));
  }

  request(value, { raw = false } = {}) {
    if (this.failure || !this.connected) {
      return Promise.reject(this.failure ?? new Error('Not connected.'));
    }
    const payload = raw ? value : JSON.stringify(value);
    const bytes = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
    return new Promise((resolveReply, rejectReply) => {
      const job = {
        resolve: resolveReply,
        reject: rejectReply,
        timer: setTimeout(() => {
          this.fail(new Error('Response timed out. Write outcome is UNKNOWN; no automatic retry.'));
        }, this.config.timeout)
      };
      this.pending.push(job);
      try {
        this.socket.write(Buffer.concat([bytes, Buffer.from('\n')]));
      } catch (error) {
        this.fail(error);
      }
    });
  }

  async close() {
    if (!this.socket || this.socket.destroyed) {
      if (this.closed) await this.closed;
      return;
    }
    const timer = setTimeout(() => this.socket.destroy(), 1000);
    this.socket.end();
    await this.closed;
    clearTimeout(timer);
  }
}

function safeInteger(value, name, min = 1, max = Number.MAX_SAFE_INTEGER) {
  const number = Number(value);
  assert.ok(Number.isSafeInteger(number) && number >= min && number <= max,
    `${name} must be an integer from ${min} to ${max}.`);
  return number;
}

export function parseTargets(hostArg, defaultPort) {
  const parts = String(hostArg ?? '').split(',').map(s => s.trim()).filter(Boolean);
  assert.ok(parts.length >= 1, 'Provide --host with at least one server VM IP address.');
  return parts.map(entry => {
    const colon = entry.lastIndexOf(':');
    if (colon !== -1 && !entry.includes(']')) {
      const host = entry.slice(0, colon).trim();
      const port = safeInteger(entry.slice(colon + 1).trim(), 'target port', 1, 65535);
      assert.ok(host.length > 0, `Invalid host entry: ${entry}`);
      return { host, port, label: `${host}:${port}` };
    }
    return { host: entry, port: defaultPort, label: `${entry}:${defaultPort}` };
  });
}

function attachRequestId(request, options = {}) {
  if (options.raw || request === null || typeof request !== 'object' || Array.isArray(request)) {
    return request;
  }
  if (Object.hasOwn(request, 'request_id')) return request;
  return { request_id: randomUUID(), ...request };
}

function saveJson(path, data, exclusive = false) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(data, null, 2) + '\n', {
    encoding: 'utf8', flag: exclusive ? 'wx' : 'w', flush: true
  });
}

export function createContext(config, socketFactory) {
  const targets = config.targets ?? parseTargets(config.host, config.port);
  let preferredTargetIndex = 0;

  const report = {
    started_at: new Date().toISOString(),
    mode: config.mode,
    server: {
      host: targets[0].host,
      port: targets[0].port,
      targets: targets.map(t => t.label)
    },
    account: config.account,
    clients: config.clients,
    requests_per_test: config.requests,
    selected_test: config.mode === 'suite' ? (config.test ?? 'all') : null,
    tests: [],
    exchanges: []
  };

  const context = {
    config,
    report,
    targets,
    get activeTarget() {
      return targets[preferredTargetIndex];
    },
    open(targetOverride = null) {
      const target = targetOverride ?? targets[preferredTargetIndex];
      const conn = new BankConnection({
        ...config,
        host: target.host,
        port: target.port
      }, socketFactory);
      conn.target = target;
      return conn.connect();
    },
    async exchange(connection, request, options = {}) {
      const prepared = attachRequestId(request, options);
      const record = {
        sent_at: new Date().toISOString(),
        target: connection.target?.label ?? `${config.host}:${config.port}`,
        request: options.raw ? null : prepared,
        ...(options.raw ? {
          raw_base64: Buffer.from(request).toString('base64')
        } : {})
      };
      report.exchanges.push(record);
      const started = performance.now();
      try {
        record.response = await connection.request(prepared, options);
        return record.response;
      } catch (error) {
        record.transport_error = error.message;
        record.outcome = 'unknown';
        throw error;
      } finally {
        record.latency_ms = performance.now() - started;
      }
    },
    // Sends a single request. When multiple cluster targets are configured,
    // automatically fails over across nodes using the SAME request_id so
    // committed transactions are never applied twice.
    async once(request, options = {}) {
      const prepared = attachRequestId(request, options);
      const isCluster = targets.length > 1 && !options.raw;
      const maxAttempts = isCluster ? Math.max(targets.length * 8, 12) : 1;
      let lastError = null;
      let lastResponse = null;

      for (let attempt = 0; attempt < maxAttempts; attempt++) {
        const idx = (preferredTargetIndex + attempt) % targets.length;
        const target = targets[idx];
        let connection;
        try {
          connection = await context.open(target);
          const response = await context.exchange(connection, prepared, options);
          if (
            isCluster &&
            response.status === 'failed' &&
            RETRYABLE_CLUSTER_CODES.has(response.error?.code)
          ) {
            lastResponse = response;
            if (options.onFailover) {
              options.onFailover({
                attempt: attempt + 1,
                from: target.label,
                reason: response.error.code,
                request_id: prepared?.request_id
              });
            }
            await sleep(250);
            continue;
          }
          preferredTargetIndex = idx;
          return response;
        } catch (error) {
          lastError = error;
          if (!isCluster) throw error;
          if (options.onFailover) {
            options.onFailover({
              attempt: attempt + 1,
              from: target.label,
              reason: error.message,
              request_id: prepared?.request_id
            });
          }
          await sleep(250);
        } finally {
          if (connection) await connection.close();
        }
      }

      if (lastResponse) return lastResponse;
      throw lastError ?? new Error('All cluster targets failed to process the request.');
    },
    async balance() {
      return success(await context.once({ action: 'balance', account: config.account }));
    },
    async change(action, amount, extra = {}) {
      return context.once({ action, account: config.account, amount, ...extra });
    },
    async test(name, operation) {
      const record = { name, started_at: new Date().toISOString() };
      report.tests.push(record);
      const started = performance.now();
      try {
        const details = await operation();
        Object.assign(record, details, { status: details?.skipped ? 'SKIP' : 'PASS' });
        console.log(`[${record.status}] ${name}: ${JSON.stringify(details ?? {})}`);
      } catch (error) {
        record.status = 'FAIL';
        record.error = error.message;
        console.error(`[FAIL] ${name}: ${error.message}`);
        throw error;
      } finally {
        record.duration_ms = performance.now() - started;
      }
    }
  };
  return context;
}

function success(response) {
  assert.equal(response.status, 'success', JSON.stringify(response));
  assert.ok(Number.isSafeInteger(response.balance) && response.balance >= 0,
    'Invalid balance in success response.');
  return response.balance;
}

function failed(response, code) {
  assert.equal(response.status, 'failed', JSON.stringify(response));
  assert.equal(response.error?.code, code, JSON.stringify(response));
}

// Open all workers first, then release them together. Each worker keeps a
// connection open and sends another request only after receiving its reply.
// If a node fails or loses leadership in cluster mode, the worker transparently
// reconnects to another healthy node and retries with the same request_id.
export async function concurrent(context, requests) {
  const preparedRequests = requests.map(req => attachRequestId(req));
  const connections = [];
  const isCluster = context.targets.length > 1;

  try {
    const workerCount = Math.min(context.config.clients, preparedRequests.length);
    const opened = await Promise.allSettled(
      Array.from({ length: workerCount }, async (_, workerIndex) => {
        const target = context.targets[workerIndex % context.targets.length];
        try {
          const connection = await context.open(target);
          connections.push(connection);
          return connection;
        } catch (error) {
          if (!isCluster) throw error;
          const fallback = await context.open();
          connections.push(fallback);
          return fallback;
        }
      })
    );
    const failure = opened.find(result => result.status === 'rejected');
    if (failure) throw failure.reason;

    let next = 0;
    const results = new Array(preparedRequests.length);

    const workers = await Promise.allSettled(connections.map(async initialConnection => {
      let currentConnection = initialConnection;
      while (next < preparedRequests.length) {
        const index = next++;
        const req = preparedRequests[index];
        try {
          const response = await context.exchange(currentConnection, req);
          if (
            isCluster &&
            response.status === 'failed' &&
            RETRYABLE_CLUSTER_CODES.has(response.error?.code)
          ) {
            results[index] = await context.once(req);
            try {
              currentConnection = await context.open();
              connections.push(currentConnection);
            } catch {
              // Keep using context.once fallback on next iteration if needed.
            }
          } else {
            results[index] = response;
          }
        } catch (error) {
          if (!isCluster) throw error;
          // Failover with the exact same request_id so no transaction is lost or duplicated.
          results[index] = await context.once(req);
          try {
            currentConnection = await context.open();
            connections.push(currentConnection);
          } catch {
            // If opening a persistent connection fails right now, next loop will retry via once().
          }
        }
      }
    }));

    const workerFailure = workers.find(result => result.status === 'rejected');
    if (workerFailure) throw workerFailure.reason;
    return results;
  } finally {
    await Promise.all(connections.map(connection => connection.close()));
  }
}

export async function runSuite(context) {
  const { config } = context;
  const { account, requests: count } = config;
  const unit = 1;
  const op = (action, amount = unit) => ({ action, account, amount });
  const selected = config.test ?? 'all';
  const runTest = (number, name, operation) => {
    if (selected === 'all' || selected === String(number)) {
      return context.test(name, operation);
    }
  };
  const start = await context.balance();
  assert.ok(start <= Number.MAX_SAFE_INTEGER - count - 1000,
    'Choose a test account with enough room below the balance limit.');

  await runTest(1, 'Normal operations and persistent connection', async () => {
    const connection = await context.open();
    try {
      const before = success(await context.exchange(connection, { action: 'balance', account }));
      const deposited = success(await context.exchange(connection, op('deposit', 7)));
      const after = success(await context.exchange(connection, op('withdraw', 7)));
      assert.equal(deposited, before + 7);
      assert.equal(after, before);
      // Two messages in flight on one connection also test newline framing.
      const replies = await Promise.all([
        context.exchange(connection, { action: 'balance', account }),
        context.exchange(connection, { action: 'balance', account })
      ]);
      replies.forEach(reply => assert.equal(success(reply), before));
      return { before, after, same_connection_requests: 5 };
    } finally {
      await connection.close();
    }
  });

  await runTest(2, 'Required error cases leave the account unchanged', async () => {
    const before = await context.balance();
    const cases = [
      [{ action: 'balance', account: '__UNKNOWN_TEST_ACCOUNT__' }, 'UNKNOWN_ACCOUNT'],
      [op('withdraw', before + 1), 'INSUFFICIENT_FUNDS'],
      [op('deposit', -1), 'INVALID_AMOUNT'],
      [op('withdraw', 0), 'INVALID_AMOUNT'],
      [op('deposit', 'abc'), 'INVALID_AMOUNT'],
      [op('deposit', 1.5), 'INVALID_AMOUNT'],
      [op('transfer'), 'UNSUPPORTED_ACTION'],
      [{ action: 'withdraw', account }, 'MISSING_FIELD'],
      [{ action: 'balance' }, 'MISSING_FIELD'],
      [{ account }, 'MISSING_FIELD'],
      [null, 'INVALID_REQUEST']
    ];
    for (const [request, code] of cases) failed(await context.once(request), code);
    failed(await context.once('{broken json', { raw: true }), 'MALFORMED_REQUEST');
    failed(await context.once(Buffer.from([255, 254]), { raw: true }), 'MALFORMED_REQUEST');
    const after = await context.balance();
    assert.equal(after, before);
    return { cases: cases.length + 2, before, after };
  });

  await runTest(3, 'Concurrent deposits: no lost updates', async () => {
    const before = await context.balance();
    const replies = await concurrent(context, Array.from({ length: count }, () => op('deposit')));
    const balances = replies.map(success).sort((a, b) => a - b);
    assert.deepEqual(balances, Array.from({ length: count }, (_, i) => before + i + 1));
    const after = await context.balance();
    assert.equal(after, before + count);
    assert.equal(success(await context.change('withdraw', count)), before);
    return { requests: count, before, expected: before + count, actual: after, restored: before };
  });

  await runTest(4, 'Concurrent withdrawals: no lost updates', async () => {
    const before = await context.balance();
    assert.equal(success(await context.change('deposit', count)), before + count);
    const replies = await concurrent(context, Array.from({ length: count }, () => op('withdraw')));
    const balances = replies.map(success).sort((a, b) => a - b);
    assert.deepEqual(balances, Array.from({ length: count }, (_, i) => before + i));
    const after = await context.balance();
    assert.equal(after, before);
    return { requests: count, funded_balance: before + count, expected: before, actual: after };
  });

  await runTest(5, 'Competing withdrawals: no overdraft', async () => {
    const original = await context.balance();
    if (original < 2) success(await context.change('deposit', 2));
    const before = await context.balance();
    const amount = Math.floor(before / 2) + 1;
    const replies = await concurrent(context, Array.from({ length: count }, () => op('withdraw', amount)));
    const accepted = replies.filter(reply => reply.status === 'success');
    const rejected = replies.filter(reply => reply.status !== 'success');
    accepted.forEach(success);
    rejected.forEach(reply => failed(reply, 'INSUFFICIENT_FUNDS'));
    assert.equal(accepted.length, 1, 'Exactly one withdrawal can succeed with this starting balance.');
    const after = await context.balance();
    assert.equal(after, before - amount);
    success(await context.change('deposit', amount));
    if (original < 2) success(await context.change('withdraw', 2));
    assert.equal(await context.balance(), original);
    return { before, amount, successful: accepted.length, rejected: rejected.length, actual: after };
  });

  await runTest(6, 'Mixed concurrent deposits and withdrawals', async () => {
    const before = await context.balance();
    const operations = Array.from({ length: count }, (_, i) => op(i % 2 ? 'withdraw' : 'deposit'));
    const deposits = Math.ceil(count / 2);
    const withdrawals = Math.floor(count / 2);
    success(await context.change('deposit', withdrawals));
    const replies = await concurrent(context, operations);
    replies.forEach(success);
    const after = await context.balance();
    assert.equal(after, before + deposits);
    assert.equal(success(await context.change('withdraw', deposits)), before);
    return { deposits, withdrawals, expected: before + deposits, actual: after, restored: before };
  });

  await runTest(7, 'Atomicity and isolation: rollback and concurrent reads', async () => {
    const before = await context.balance();
    const probe = await context.change('deposit', 7, { test_fault: 'after_update' });
    if (probe.error?.code === 'INVALID_TEST_FAULT') {
      failed(probe, 'INVALID_TEST_FAULT');
      assert.equal(await context.balance(), before);
      return { skipped: true, reason: 'Restart server with ALLOW_TEST_FAULTS=1 to test failure AFTER UPDATE.' };
    }
    failed(probe, 'TEST_FAILURE');
    assert.equal(await context.balance(), before);
    // Ensure a withdrawal reaches UPDATE even if the account initially has zero.
    if (before === 0) success(await context.change('deposit', 1));
    const baseline = before === 0 ? 1 : before;
    failed(await context.change('withdraw', 1, { test_fault: 'after_update' }), 'TEST_FAILURE');
    const operations = Array.from({ length: count }, (_, i) => i % 2
      ? { action: 'balance', account }
      : { ...op('deposit', 13), test_fault: 'after_update' });
    const replies = await concurrent(context, operations);
    replies.forEach((reply, i) => {
      if (i % 2) assert.equal(success(reply), baseline, 'Read observed uncommitted balance.');
      else failed(reply, 'TEST_FAILURE');
    });
    assert.equal(await context.balance(), baseline);
    if (before === 0) success(await context.change('withdraw', 1));
    assert.equal(await context.balance(), before);
    return { requests: count, expected: before, actual: before, injected_failure: 'after_update' };
  });

  assert.equal(await context.balance(), start, 'Suite did not restore the starting balance.');
  context.report.initial_balance = start;
  context.report.final_balance = start;
  context.report.durability = 'NOT TESTED: use durability-prepare, kill/restart server, then durability-check.';
  console.log(`Starting/final balance: ${start}. All completed tests restored the account.`);
  console.log(context.report.durability);
}

// Continuous paced workload designed for live Bonus 5 (Failover) and Bonus 6 (Split-Brain) demos.
// While this runs, you can kill the primary server or cut the network between VMs;
// the client logs every failover event and verifies exact final balances across nodes.
export async function runClusterDemo(context) {
  const { config, targets } = context;
  const { account, requests: count, interval } = config;
  assert.ok(targets.length >= 2, 'cluster-demo requires at least 2 server addresses in --host (e.g. IP1,IP2,IP3).');

  await context.test('Cluster failover & partition resilience demo', async () => {
    const startBalance = await context.balance();
    console.log(`[CLUSTER-DEMO] Account=${account} | Initial balance=${startBalance} | Targets=${targets.map(t => t.label).join(', ')}`);
    console.log(`[CLUSTER-DEMO] Sending ${count} paced transactions (interval=${interval}ms).`);
    console.log('[CLUSTER-DEMO] >>> You may now stop the Primary server or cut the network link between VMs! <<<');

    let expectedBalance = startBalance;
    let failoverEvents = 0;

    for (let i = 1; i <= count; i++) {
      const action = i % 3 === 0 ? 'withdraw' : 'deposit';
      const amount = 10;
      const req = {
        request_id: randomUUID(),
        action,
        account,
        amount
      };

      const response = await context.once(req, {
        onFailover: info => {
          failoverEvents++;
          console.log(`  [FAILOVER #${failoverEvents}] Node ${info.from} unavailable (${info.reason}) -> retrying request_id=${info.request_id} on next node...`);
        }
      });

      const newBalance = success(response);
      expectedBalance = action === 'deposit' ? expectedBalance + amount : expectedBalance - amount;
      assert.equal(
        newBalance,
        expectedBalance,
        `Balance mismatch at step ${i} (request_id=${req.request_id}): expected ${expectedBalance}, got ${newBalance}`
      );

      console.log(
        `  [${i}/${count}] ${action.toUpperCase()} ${amount} | status=success | balance=${newBalance} | served_by=${context.activeTarget.label} | req=${req.request_id.slice(0, 8)}`
      );

      if (i < count && interval > 0) {
        await sleep(interval);
      }
    }

    // Verify all reachable nodes in the cluster agree on the final balance (no split-brain drift).
    const nodeBalances = {};
    for (const target of targets) {
      let conn;
      try {
        conn = await context.open(target);
        const res = await context.exchange(conn, {
          request_id: randomUUID(),
          action: 'balance',
          account
        });
        if (res.status === 'success') {
          nodeBalances[target.label] = res.balance;
          assert.equal(res.balance, expectedBalance, `Node ${target.label} has diverged balance!`);
        } else {
          nodeBalances[target.label] = `unreachable_leader (${res.error?.code})`;
        }
      } catch (err) {
        nodeBalances[target.label] = `down (${err.message})`;
      } finally {
        if (conn) await conn.close();
      }
    }

    console.log(`[CLUSTER-DEMO] Completed! Expected=${expectedBalance} | Node states=${JSON.stringify(nodeBalances)}`);
    return {
      initial_balance: startBalance,
      final_balance: expectedBalance,
      transactions_committed: count,
      failover_events: failoverEvents,
      node_balances: nodeBalances
    };
  });
}

export async function prepareDurability(context) {
  const { config } = context;
  assert.ok(!existsSync(config.checkpoint), 'Checkpoint already exists; use a new --checkpoint path.');
  // Check the parent directory before changing the account.
  mkdirSync(dirname(config.checkpoint), { recursive: true });
  const before = await context.balance();
  assert.ok(before <= Number.MAX_SAFE_INTEGER - 7);
  const deposited = await context.change('deposit', 7);
  assert.equal(success(deposited), before + 7);
  const withdrawn = await context.change('withdraw', 3);
  assert.equal(success(withdrawn), before + 4);
  const checkpoint = {
    version: 1,
    created_at: new Date().toISOString(),
    server: { host: config.host, port: config.port },
    account: config.account,
    initial_balance: before,
    expected_balance: before + 4,
    acknowledged_responses: [deposited, withdrawn]
  };
  saveJson(config.checkpoint, checkpoint, true);
  context.report.checkpoint = checkpoint;
  console.log(`Checkpoint saved: ${config.checkpoint}`);
  console.log(`Confirmed balance: ${checkpoint.expected_balance}`);
  console.log('Now kill the server process with SIGKILL, restart it with the SAME DB_PATH,');
  console.log('then run durability-check --confirm-restart. Do not run other writes in between.');
}

export async function checkDurability(context) {
  const { config } = context;
  assert.ok(config.confirmRestart, 'Add --confirm-restart only AFTER killing and restarting the server.');
  const checkpoint = JSON.parse(readFileSync(config.checkpoint, 'utf8'));
  assert.equal(checkpoint.version, 1);
  assert.equal(checkpoint.server.host, config.host, 'Checkpoint belongs to a different server host.');
  assert.equal(checkpoint.server.port, config.port);
  assert.equal(checkpoint.account, config.account);
  assert.ok(Number.isSafeInteger(checkpoint.expected_balance) && checkpoint.expected_balance >= 0);
  await context.test('Durability: previously acknowledged balance survives restart', async () => {
    const actual = await context.balance();
    assert.equal(actual, checkpoint.expected_balance);
    return {
      expected: checkpoint.expected_balance, actual,
      restart_confirmation: 'User confirmed SIGKILL/restart; client cannot independently verify a restart.'
    };
  });
}

export async function main(argv = process.argv.slice(2), socketFactory) {
  const { values, positionals } = parseArgs({
    args: argv, allowPositionals: true,
    options: {
      host: { type: 'string' }, port: { type: 'string', default: '5000' },
      account: { type: 'string', default: 'A1003' },
      clients: { type: 'string', default: '10' }, requests: { type: 'string', default: '100' },
      interval: { type: 'string', default: '250' },
      test: { type: 'string', default: 'all' },
      timeout: { type: 'string', default: '15000' },
      report: { type: 'string', default: 'client-report.json' },
      checkpoint: { type: 'string', default: 'durability-checkpoint.json' },
      json: { type: 'string' }, raw: { type: 'string' },
      'confirm-restart': { type: 'boolean', default: false },
      help: { type: 'boolean', short: 'h', default: false }
    }
  });
  if (values.help) { console.log(HELP); return 0; }
  assert.equal(positionals.length, 1, HELP);
  const mode = positionals[0];
  assert.ok(['suite', 'send', 'durability-prepare', 'durability-check', 'cluster-demo'].includes(mode), HELP);
  assert.ok(values.host?.trim(), 'Provide --host with the server VM IP address(es).');
  assert.ok(values.account.trim(), 'Account cannot be empty.');
  assert.ok(/^(all|[1-7])$/.test(values.test), '--test must be all or a number from 1 to 7.');
  assert.ok(mode === 'suite' || values.test === 'all', '--test is only available in suite mode.');

  const defaultPort = safeInteger(values.port, 'port', 1, 65535);
  const targets = parseTargets(values.host, defaultPort);

  const config = {
    mode,
    host: values.host.trim(),
    port: defaultPort,
    targets,
    account: values.account,
    test: values.test,
    clients: safeInteger(values.clients, 'clients', 2, 1000),
    requests: safeInteger(values.requests, 'requests', 2, 100000),
    interval: safeInteger(values.interval, 'interval', 0, 60000),
    timeout: safeInteger(values.timeout, 'timeout', 1, 2147483647),
    report: resolve(values.report),
    checkpoint: resolve(values.checkpoint),
    confirmRestart: values['confirm-restart']
  };
  assert.notEqual(config.report, config.checkpoint, 'Report and checkpoint paths must differ.');
  let manual;
  if (mode === 'send') {
    assert.ok((values.json !== undefined) !== (values.raw !== undefined),
      'For send, provide exactly one of --json or --raw.');
    manual = values.raw !== undefined ? values.raw : JSON.parse(values.json);
  }
  const context = createContext(config, socketFactory);
  let exitCode = 0;
  try {
    if (mode === 'suite') await runSuite(context);
    else if (mode === 'cluster-demo') await runClusterDemo(context);
    else if (mode === 'durability-prepare') await prepareDurability(context);
    else if (mode === 'durability-check') await checkDurability(context);
    else {
      const response = await context.once(manual, { raw: values.raw !== undefined });
      console.log(JSON.stringify(response, null, 2));
      if (response.status !== 'success') exitCode = 1;
    }
    if (context.report.tests.some(test => test.status === 'SKIP')) exitCode = 2;
  } catch (error) {
    context.report.error = error.message;
    console.error(`FAILED: ${error.message}`);
    console.error('No write will be retried. Check server balances/logs before repeating a failed run.');
    exitCode = 1;
  } finally {
    context.report.finished_at = new Date().toISOString();
    context.report.status = exitCode === 0 ? 'PASS' : exitCode === 2 ? 'INCOMPLETE' : 'FAIL';
    saveJson(config.report, context.report);
    console.log(`Report: ${config.report}`);
  }
  return exitCode;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().then(code => { process.exitCode = code; }).catch(error => {
    console.error(error.message);
    process.exitCode = 1;
  });
}