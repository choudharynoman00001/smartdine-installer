// SmartDine <> Subscription Control Center connector (contract scc-1.1, updated freeze candidate).
// Isolated-build implementation. Single-install product: exactly ONE account, never fake
// multi-tenancy. Secret = SCC_KEY_ID/SCC_SECRET env (vault flow) ONLY - never generated,
// never in DB/logs/responses/forms. Unset = connector disabled (404), fail closed.
import crypto from 'node:crypto';
import { verifyRequest, sha256hex, CONTRACT } from './lib/contract.mjs';

const nowSec = () => Math.floor(Date.now() / 1000);
const iso = ms => new Date(ms).toISOString(); // ISO-8601 UTC Z with milliseconds

// DB nonce store: atomic addIfAbsent via primary key, persisted (survives restart),
// corrupt/locked DB makes every call throw -> verifyRequest fails closed 503.
class DbNonceStore {
  constructor(db, floor) { this.db = db; this.floor = floor; }
  addIfAbsent(keyId, nonce, exp) {
    this.db.prepare('DELETE FROM scc_nonces WHERE expires_at < ?').run(nowSec());
    try {
      this.db.prepare('INSERT INTO scc_nonces(k, expires_at) VALUES(?,?)').run(keyId + ':' + nonce, exp);
      return true;
    } catch (e) {
      if (String(e.message).includes('UNIQUE')) return false; // replay: exactly one wins
      throw e; // real store failure -> 503
    }
  }
}

export function initScc(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS scc_entitlement(
    external_ref TEXT PRIMARY KEY, local_id TEXT, name TEXT NOT NULL DEFAULT '',
    suspended INTEGER NOT NULL DEFAULT 0,
    valid_until TEXT, grace_until TEXT,
    state_changed_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS scc_requests(
    request_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL, response TEXT NOT NULL, status INTEGER NOT NULL, at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS scc_nonces(k TEXT PRIMARY KEY, expires_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS scc_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);`);
  const meta = (k, mk) => {
    const row = db.prepare('SELECT value FROM scc_meta WHERE key = ?').get(k);
    if (row) return row.value;
    const v = mk();
    db.prepare('INSERT INTO scc_meta(key, value) VALUES(?,?)').run(k, v);
    return v;
  };
  meta('install_id', () => crypto.randomUUID()); // the install's own stable id, not a secret
  meta('not_before', () => String(nowSec()));    // store-floor: first run/restore rejects older timestamps
  db.prepare('DELETE FROM scc_requests WHERE at < ?').run(Date.now() - 30 * 86400e3);
}

const getMeta = (db, k) => db.prepare('SELECT value FROM scc_meta WHERE key = ?').get(k)?.value;
const theAccount = db => db.prepare('SELECT * FROM scc_entitlement ORDER BY rowid LIMIT 1').get(); // single-install: one account

function effectiveState(e) {
  if (!e) return 'absent';
  if (e.suspended) return 'suspended';
  const vu = e.valid_until ? Date.parse(e.valid_until) : null;
  const gu = e.grace_until ? Date.parse(e.grace_until) : null;
  const end = Math.max(vu ?? -Infinity, gu ?? -Infinity);
  if (end !== -Infinity && Date.now() > end) return 'expired';
  return 'active';
}
const pub = e => ({ external_ref: e.external_ref, state: effectiveState(e), state_changed_at: e.state_changed_at, valid_until: e.valid_until ?? null, grace_until: e.grace_until ?? null });

export function sccSuspended(db) {
  const e = theAccount(db);
  return !!e && !!e.suspended;
}

const fail = (send, res, status, code, message) => (send(res, status, { error: code, message }), true);
const REF_OK = /^[A-Za-z0-9_.-]{1,64}$/;

export async function handleScc(req, res, u, rawBody, db, send, clearSessions) {
  const p = u.pathname;
  if (!p.startsWith('/_scc/v1/')) return false;
  // connector disabled when unconfigured: fail closed, nothing to attack
  if (!process.env.SCC_SECRET) return fail(send, res, 404, 'not_found', 'connector not configured');
  const keys = { [process.env.SCC_KEY_ID || 'sd-local']: process.env.SCC_SECRET };
  let v;
  try {
    v = verifyRequest({
      secrets: keys, nonces: new DbNonceStore(db, Number(getMeta(db, 'not_before') || 0)),
      method: req.method, path: p, query: u.search.replace(/^\?/, ''), body: rawBody, headers: req.headers,
    });
  } catch { return fail(send, res, 503, 'nonce_store_unavailable', 'nonce store error'); }
  if (!v.ok) return fail(send, res, v.status, v.code, v.code);

  // candidate bounds: bodies are small control messages - reject oversized payloads before parsing
  if (rawBody && Buffer.byteLength(rawBody) > 16384) return fail(send, res, 400, 'bad_json', 'body too large for this API');
  let body = {};
  if (rawBody) { try { body = JSON.parse(rawBody); } catch { return fail(send, res, 400, 'bad_json', 'JSON body required'); } }
  const ref = p.startsWith('/_scc/v1/accounts/') ? p.split('/')[4] || '' : null;
  const sub = ref ? (p.split('/')[5] || '') : '';
  if (ref && !REF_OK.test(ref)) return fail(send, res, 400, 'bad_path', 'invalid external_ref');

  const mutating = req.method !== 'GET';
  const requestId = body.request_id != null ? String(body.request_id) : null;
  if (mutating && !requestId) return fail(send, res, 400, 'request_id_required', 'request_id required');
  // contract: request_id is a UUID
  if (mutating && !/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(requestId))
    return fail(send, res, 400, 'bad_json', 'request_id must be a UUID');
  if (mutating) {
    const prev = db.prepare('SELECT * FROM scc_requests WHERE request_id = ?').get(requestId);
    if (prev) {
      if (prev.payload_hash !== sha256hex(rawBody)) return fail(send, res, 409, 'request_id_conflict', 'request_id reused with a different payload');
      res.setHeader('Idempotent-Replay', 'true');
      send(res, prev.status, JSON.parse(prev.response));
      return true;
    }
  }
  const record = (status, obj) => {
    if (mutating) db.prepare('INSERT INTO scc_requests(request_id, payload_hash, response, status, at) VALUES(?,?,?,?,?)')
      .run(requestId, sha256hex(rawBody), JSON.stringify(obj), status, Date.now());
  };

  if (p === '/_scc/v1/health' && req.method === 'GET') {
    send(res, 200, { ok: true, product: 'smartdine', version: 1, contract: CONTRACT });
    return true;
  }
  if (p === '/_scc/v1/accounts' && req.method === 'GET') {
    const e = theAccount(db);
    send(res, 200, { accounts: e ? [pub(e)] : [], next_cursor: null }); // single install: at most one
    return true;
  }
  if (!ref) return fail(send, res, 404, 'not_found', 'unknown endpoint');
  const ent = db.prepare('SELECT * FROM scc_entitlement WHERE external_ref = ?').get(ref);

  if (req.method === 'GET' && !sub) {
    if (!ent) return send(res, 200, { external_ref: ref, state: 'absent', state_changed_at: null, valid_until: null }), true;
    return send(res, 200, pub(ent)), true;
  }
  if (req.method === 'PUT' && !sub) {
    if (!body.name) return fail(send, res, 400, 'bad_json', 'name required');
    const vu = body.valid_until != null ? Date.parse(body.valid_until) : null;
    const gu = body.grace_until != null ? Date.parse(body.grace_until) : null;
    if (body.valid_until != null && !Number.isFinite(vu)) return fail(send, res, 400, 'bad_json', 'valid_until must be an ISO-8601 instant');
    if (body.grace_until != null && !Number.isFinite(gu)) return fail(send, res, 400, 'bad_json', 'grace_until must be an ISO-8601 instant');
    if (vu != null && gu != null && gu < vu) return fail(send, res, 400, 'bad_json', 'grace_until must be >= valid_until');
    const installId = getMeta(db, 'install_id');
    const existing = theAccount(db);
    if (existing && existing.external_ref !== ref) return fail(send, res, 409, 'ref_conflict', 'single-install product: one account already bound');
    if (body.local_id != null && String(body.local_id) !== installId) return fail(send, res, 409, 'ref_conflict', 'local_id does not match this install');
    if (ent && ent.local_id && body.local_id != null && String(body.local_id) !== ent.local_id) return fail(send, res, 409, 'ref_conflict', 'ref already bound');
    db.prepare(`INSERT INTO scc_entitlement(external_ref, local_id, name, valid_until, grace_until, suspended, state_changed_at)
      VALUES(?,?,?,?,?,COALESCE((SELECT suspended FROM scc_entitlement WHERE external_ref=?),0),?)
      ON CONFLICT(external_ref) DO UPDATE SET name=excluded.name, valid_until=excluded.valid_until, grace_until=excluded.grace_until`)
      .run(ref, installId, String(body.name).slice(0, 120),
        vu != null ? iso(vu) : null, gu != null ? iso(gu) : (vu != null ? iso(vu) : null), ref, iso(Date.now()));
    const e = db.prepare('SELECT * FROM scc_entitlement WHERE external_ref = ?').get(ref);
    const out = pub(e);
    record(200, out);
    return send(res, 200, out), true;
  }
  if (req.method === 'POST' && (sub === 'suspend' || sub === 'restore')) {
    if (!ent) return fail(send, res, 404, 'unknown_account', 'unknown account');
    if (sub === 'suspend' && !body.reason) return fail(send, res, 400, 'bad_json', 'reason required');
    if (sub === 'restore') {
      // expiry is checked independently of the suspended flag: a suspended+expired
      // account reads 'suspended' but restore still 409s until renewed (never silently extends)
      const end = Math.max(ent.valid_until ? Date.parse(ent.valid_until) : -Infinity,
        ent.grace_until ? Date.parse(ent.grace_until) : -Infinity);
      if (end !== -Infinity && Date.now() > end)
        return fail(send, res, 409, 'entitlement_expired', 'renew valid_until before restore');
    }
    db.prepare('UPDATE scc_entitlement SET suspended = ?, state_changed_at = ? WHERE external_ref = ?')
      .run(sub === 'suspend' ? 1 : 0, iso(Date.now()), ref);
    const e = db.prepare('SELECT * FROM scc_entitlement WHERE external_ref = ?').get(ref); // read back from own store
    const out = { state: effectiveState(e), state_changed_at: e.state_changed_at };
    record(200, out);
    return send(res, 200, out), true;
  }
  if (req.method === 'POST' && sub === 'revoke-sessions') {
    if (!ent) return fail(send, res, 404, 'unknown_account', 'unknown account');
    const out = { revoked: clearSessions() }; // single install: this account's sessions = all live sessions
    record(200, out);
    return send(res, 200, out), true;
  }
  return fail(send, res, 404, 'not_found', 'unknown endpoint');
}
