import net from 'node:net';
import { randomInt } from 'node:crypto';

// Small, fixed-membership Raft implementation for this assignment.
// It intentionally has no membership changes or log compaction.
export class RaftNode {
  constructor({ id, host, port, peers, db, applyCommand, onClientRequest }) {
    this.id = id;
    this.host = host;
    this.port = port;
    this.peers = peers;
    this.db = db;
    this.applyCommand = applyCommand;
    this.onClientRequest = onClientRequest;
    this.members = [{ id, host, port }, ...peers.filter(p => p.id !== id)];
    this.majority = Math.floor(this.members.length / 2) + 1;
    this.role = 'follower';
    this.leaderId = null;
    this.matchIndex = new Map();
    this.pending = new Map();
    this.proposalQueue = Promise.resolve();
    this.stopped = false;
    this.initStorage();
  }

  initStorage() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS raft_meta (
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT NOT NULL
      ) STRICT;
      CREATE TABLE IF NOT EXISTS raft_log (
        log_index INTEGER PRIMARY KEY NOT NULL,
        term INTEGER NOT NULL,
        command TEXT NOT NULL
      ) STRICT;
      CREATE TABLE IF NOT EXISTS raft_results (
        request_id TEXT PRIMARY KEY NOT NULL,
        response_json TEXT NOT NULL
      ) STRICT;
    `);
    const put = this.db.prepare('INSERT OR IGNORE INTO raft_meta(key,value) VALUES(?,?)');
    for (const [key, value] of Object.entries({
      currentTerm: '0', votedFor: '', commitIndex: '0', lastApplied: '0'
    })) put.run(key, value);
  }

  meta(key) {
    return this.db.prepare('SELECT value FROM raft_meta WHERE key=?').get(key).value;
  }
  setMeta(key, value) {
    this.db.prepare('UPDATE raft_meta SET value=? WHERE key=?').run(String(value), key);
  }
  get term() { return Number(this.meta('currentTerm')); }
  lastIndex() {
    return Number(this.db.prepare('SELECT COALESCE(MAX(log_index),0) AS n FROM raft_log').get().n);
  }
  logTerm(index) {
    if (index === 0) return 0;
    return Number(this.db.prepare('SELECT term FROM raft_log WHERE log_index=?').get(index)?.term ?? -1);
  }

  start() {
    this.server = net.createServer(socket => this.acceptPeer(socket));
    this.server.on('error', error => console.error(JSON.stringify({ event: 'raft_socket_error', node_id: this.id, message: error.message })));
    this.server.listen(this.port, this.host, () => this.resetElectionTimer());
    this.applyCommitted();
  }

  resetElectionTimer() {
    clearTimeout(this.electionTimer);
    if (this.stopped || this.role === 'leader') return;
    this.electionTimer = setTimeout(() => this.startElection(), randomInt(700, 1250));
    this.electionTimer.unref();
  }

  async startElection() {
    if (this.stopped) return;
    this.role = 'candidate';
    this.leaderId = null;
    const term = this.term + 1;
    this.db.exec('BEGIN IMMEDIATE');
    this.setMeta('currentTerm', term);
    this.setMeta('votedFor', this.id);
    this.db.exec('COMMIT');
    this.resetElectionTimer();
    let votes = 1;
    const lastIndex = this.lastIndex();
    const lastTerm = this.logTerm(lastIndex);
    await Promise.all(this.peers.map(async peer => {
      const reply = await this.rpc(peer, { type: 'requestVote', term, candidateId: this.id, lastIndex, lastTerm });
      if (reply?.term > this.term) this.observeTerm(reply.term);
      else if (this.role === 'candidate' && this.term === term && reply?.voteGranted) votes++;
    }));
    if (this.role === 'candidate' && this.term === term && votes >= this.majority) {
      this.becomeLeader();
    }
  }

  becomeLeader() {
    this.role = 'leader';
    this.leaderId = this.id;
    this.matchIndex.clear();
    for (const peer of this.peers) this.matchIndex.set(peer.id, 0);
    console.log(JSON.stringify({ event: 'raft_role', node_id: this.id, role: this.role, term: this.term }));
    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => this.replicateAll(), 180);
    this.heartbeatTimer.unref();
    this.replicateAll();
  }

  observeTerm(term) {
    if (term <= this.term) return;
    this.db.exec('BEGIN IMMEDIATE');
    this.setMeta('currentTerm', term);
    this.setMeta('votedFor', '');
    this.db.exec('COMMIT');
    this.role = 'follower';
    this.leaderId = null;
    clearInterval(this.heartbeatTimer);
    this.resetElectionTimer();
  }

  async rpc(peer, message) {
    return new Promise(resolve => {
      const socket = net.createConnection({ host: peer.host, port: peer.port });
      let data = '';
      const timeout = setTimeout(() => { socket.destroy(); resolve(null); }, 450);
      timeout.unref();
      socket.setEncoding('utf8');
      socket.on('connect', () => socket.write(JSON.stringify(message) + '\n'));
      socket.on('data', chunk => {
        data += chunk;
        const end = data.indexOf('\n');
        if (end !== -1) {
          clearTimeout(timeout); socket.destroy();
          try { resolve(JSON.parse(data.slice(0, end))); } catch { resolve(null); }
        }
      });
      socket.on('error', () => { clearTimeout(timeout); resolve(null); });
      socket.on('close', () => { clearTimeout(timeout); });
    });
  }

  acceptPeer(socket) {
    let data = '';
    socket.setEncoding('utf8');
    socket.setTimeout(5000, () => socket.destroy());
    socket.on('data', chunk => {
      data += chunk;
      if (data.length > 1024 * 1024) return socket.destroy();
      const end = data.indexOf('\n');
      if (end === -1) return;
      socket.pause();
      let msg;
      try { msg = JSON.parse(data.slice(0, end)); }
      catch { return socket.end(JSON.stringify({ error: 'BAD_JSON' }) + '\n'); }
      this.onPeerMessage(msg).then(reply => socket.end(JSON.stringify(reply) + '\n'))
        .catch(() => socket.end(JSON.stringify({ error: 'RAFT_ERROR' }) + '\n'));
    });
  }

  async onPeerMessage(msg) {
    if (msg.type === 'clientRequest') {
      if (this.role === 'leader') return this.onClientRequest(msg.payload);
      const leader = this.members.find(p => p.id === this.leaderId);
      if (!leader) return { status: 'failed', error: { code: 'NO_LEADER', message: 'No Raft leader is currently known.' } };
      return (await this.rpc(leader, msg)) ?? { status: 'failed', error: { code: 'LEADER_UNAVAILABLE', message: 'The Raft leader is unavailable.' } };
    }
    if (msg.type === 'requestVote') {
      if (msg.term > this.term) this.observeTerm(msg.term);
      const upToDate = msg.lastTerm > this.logTerm(this.lastIndex()) ||
        (msg.lastTerm === this.logTerm(this.lastIndex()) && msg.lastIndex >= this.lastIndex());
      const votedFor = this.meta('votedFor');
      const voteGranted = msg.term === this.term && upToDate && (!votedFor || votedFor === msg.candidateId);
      if (voteGranted) {
        this.db.exec('BEGIN IMMEDIATE'); this.setMeta('votedFor', msg.candidateId); this.db.exec('COMMIT');
        this.resetElectionTimer();
      }
      return { term: this.term, voteGranted };
    }
    if (msg.type === 'appendEntries') {
      if (msg.term < this.term) return { term: this.term, success: false, matchIndex: this.lastIndex() };
      if (msg.term > this.term) this.observeTerm(msg.term);
      this.role = 'follower'; this.leaderId = msg.leaderId;
      clearInterval(this.heartbeatTimer); this.resetElectionTimer();
      if (this.logTerm(msg.prevIndex) !== msg.prevTerm) return { term: this.term, success: false, matchIndex: this.lastIndex() };
      this.db.exec('BEGIN IMMEDIATE');
      try {
        for (const entry of msg.entries) {
          const existing = this.db.prepare('SELECT term FROM raft_log WHERE log_index=?').get(entry.index);
          if (existing && Number(existing.term) !== entry.term) {
            if (entry.index <= Number(this.meta('commitIndex'))) throw new Error('attempt to replace committed log entry');
            this.db.prepare('DELETE FROM raft_log WHERE log_index>=?').run(entry.index);
          }
          this.db.prepare('INSERT OR IGNORE INTO raft_log(log_index,term,command) VALUES(?,?,?)')
            .run(entry.index, entry.term, entry.command);
        }
        this.db.exec('COMMIT');
      } catch (error) { if (this.db.isTransaction) this.db.exec('ROLLBACK'); throw error; }
      if (msg.leaderCommit > Number(this.meta('commitIndex'))) {
        this.db.exec('BEGIN IMMEDIATE'); this.setMeta('commitIndex', Math.min(msg.leaderCommit, this.lastIndex())); this.db.exec('COMMIT');
        this.applyCommitted();
      }
      return { term: this.term, success: true, matchIndex: this.lastIndex() };
    }
    return { error: 'UNKNOWN_RPC' };
  }

  async replicatePeer(peer) {
    if (this.role !== 'leader') return;
    let next = (this.matchIndex.get(peer.id) ?? 0) + 1;
    let prevIndex = next - 1;
    const entries = this.db.prepare('SELECT log_index AS idx,term,command FROM raft_log WHERE log_index>=? ORDER BY log_index').all(next)
      .map(row => ({ index: Number(row.idx), term: Number(row.term), command: row.command }));
    const reply = await this.rpc(peer, { type: 'appendEntries', term: this.term, leaderId: this.id,
      prevIndex, prevTerm: this.logTerm(prevIndex), entries, leaderCommit: Number(this.meta('commitIndex')) });
    if (reply?.term > this.term) return this.observeTerm(reply.term);
    if (this.role !== 'leader') return;
    if (reply?.success) this.matchIndex.set(peer.id, reply.matchIndex);
    else if ((this.matchIndex.get(peer.id) ?? 0) > 0) this.matchIndex.set(peer.id, this.matchIndex.get(peer.id) - 1);
  }

  async replicateAll() {
    if (this.role !== 'leader') return;
    await Promise.all(this.peers.map(peer => this.replicatePeer(peer)));
    if (this.role !== 'leader') return;
    const last = this.lastIndex();
    for (let index = last; index > Number(this.meta('commitIndex')); index--) {
      if (this.logTerm(index) !== this.term) continue;
      const replicated = 1 + [...this.matchIndex.values()].filter(value => value >= index).length;
      if (replicated >= this.majority) {
        this.db.exec('BEGIN IMMEDIATE'); this.setMeta('commitIndex', index); this.db.exec('COMMIT');
        this.applyCommitted();
        break;
      }
    }
  }

  applyCommitted() {
    while (Number(this.meta('lastApplied')) < Number(this.meta('commitIndex'))) {
      const index = Number(this.meta('lastApplied')) + 1;
      const entry = this.db.prepare('SELECT command,term FROM raft_log WHERE log_index=?').get(index);
      if (!entry) break;
      let result;
      try { result = this.applyCommand(JSON.parse(entry.command), index); }
      catch (error) {
        // An unexpected state-machine failure must stop application here.
        // Continuing could make replicas apply later bank commands to different states.
        console.error(JSON.stringify({ event: 'raft_apply_error', node_id: this.id, index, message: error.message }));
        break;
      }
      const pending = this.pending.get(index);
      if (pending) { this.pending.delete(index); pending(result); }
    }
  }

  submit(payload) {
    if (this.role !== 'leader') {
      const leader = this.members.find(p => p.id === this.leaderId);
      if (!leader) return Promise.resolve({ status: 'failed', error: { code: 'NO_LEADER', message: 'No Raft leader is currently known.' } });
      return this.rpc(leader, { type: 'clientRequest', payload }).then(reply => reply ??
        { status: 'failed', error: { code: 'LEADER_UNAVAILABLE', message: 'The Raft leader is unavailable.' } });
    }
    const operation = this.proposalQueue.then(() => this.proposeAsLeader(payload));
    this.proposalQueue = operation.catch(() => {});
    return operation;
  }

  async proposeAsLeader(payload) {
    if (this.role !== 'leader') return this.submit(payload);
    const index = this.lastIndex() + 1;
    this.db.prepare('INSERT INTO raft_log(log_index,term,command) VALUES(?,?,?)')
      .run(index, this.term, JSON.stringify(payload));
    const resultPromise = new Promise(resolve => this.pending.set(index, resolve));
    const deadline = Date.now() + 3000;
    while (this.role === 'leader' && Number(this.meta('commitIndex')) < index && Date.now() < deadline) {
      await this.replicateAll();
      if (Number(this.meta('commitIndex')) < index) await new Promise(resolve => setTimeout(resolve, 40));
    }
    if (Number(this.meta('commitIndex')) < index) {
      this.pending.delete(index);
      return { status: 'failed', error: { code: 'NO_QUORUM', message: 'No majority is available; the operation was not confirmed.' } };
    }
    this.applyCommitted();
    return resultPromise;
  }

  close() {
    this.stopped = true;
    clearTimeout(this.electionTimer); clearInterval(this.heartbeatTimer);
    this.server?.close();
  }
}
