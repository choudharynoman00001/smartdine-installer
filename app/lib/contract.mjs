// SCC contract v1.1 reference: signing, verification, nonce store. Zero deps. Builders can copy this file.
import crypto from 'node:crypto';
export const CONTRACT = 'scc-1.1';
export const SKEW_SEC = 300;
export const NONCE_TTL_SEC = 900; // > 2x skew, so a replay inside the valid window is always caught
const enc = (s) => encodeURIComponent(s).replace(/[!'()*]/g, (c) => '%' + c.charCodeAt(0).toString(16).toUpperCase());
export function canonicalQuery(search) {
  const p = new URLSearchParams(search || '');
  return [...p.entries()].map(([k, v]) => [enc(k), enc(v)]).sort((a, b) => (a[0] === b[0] ? (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0) : a[0] < b[0] ? -1 : 1)).map(([k, v]) => k + '=' + v).join('&');
}
export const sha256hex = (buf) => crypto.createHash('sha256').update(buf || '').digest('hex');
// path = raw pathname exactly as sent (not decoded). query = raw search string (with or without '?'). body = raw bytes/string.
export function canonicalString({ method, path, query, timestamp, nonce, body }) {
  return [String(method).toUpperCase(), path, canonicalQuery((query || '').replace(/^\?/, '')), String(timestamp), nonce, sha256hex(body)].join('\n');
}
export function sign(secret, parts) {
  return crypto.createHmac('sha256', secret).update(canonicalString(parts)).digest('hex');
}
export function signedHeaders({ keyId, secret, method, path, query = '', body = '', now = Math.floor(Date.now() / 1000), nonce = crypto.randomBytes(16).toString('hex') }) {
  return { 'X-SCC-Key-Id': keyId, 'X-SCC-Timestamp': String(now), 'X-SCC-Nonce': nonce, 'X-SCC-Signature': sign(secret, { method, path, query, timestamp: now, nonce, body }) };
}
// Nonce store contract: addIfAbsent(keyId,nonce,expiresAt) -> true if newly stored, false if replay. MUST be atomic and persistent.
// Any read/write failure must THROW (verifyRequest then fails closed with 503). A corrupt file is never treated as empty.
export class FileNonceStore {
  constructor(file, fs) {
    this.file = file; this.fs = fs; this.m = new Map(); this.floor = 0;
    let raw = null;
    try { raw = fs.readFileSync(file, 'utf8'); } catch (e) { if (e.code !== 'ENOENT') throw e; }
    if (raw === null) { this.floor = Math.floor(Date.now() / 1000); this._save(); } // no store (first run or restored without it): reject older timestamps
    else { for (const [k, v] of Object.entries(JSON.parse(raw))) this.m.set(k, v); } // JSON.parse throws on corruption -> caller must not start
  }
  addIfAbsent(keyId, nonce, exp, now = Date.now() / 1000) {
    this.prune(now); const k = keyId + ':' + nonce;
    if (this.m.has(k)) return false;
    this.m.set(k, exp);
    try { this._save(); } catch (e) { this.m.delete(k); throw e; }
    return true;
  }
  prune(now) { let ch = false; for (const [k, e] of this.m) if (e < now) { this.m.delete(k); ch = true; } if (ch) { try { this._save(); } catch {} } }
  _save() { this.fs.writeFileSync(this.file + '.tmp', JSON.stringify(Object.fromEntries(this.m))); this.fs.renameSync(this.file + '.tmp', this.file); }
}
// secrets: {keyId: secret}. Each keyId is scoped to ONE product (and ideally one environment); nonces are namespaced by keyId.
// notBefore (unix s): reject any timestamp earlier than this. Set it to the restore/first-start time whenever the nonce store was lost or restored from backup.
export function verifyRequest({ secrets, nonces, method, path, query, body, headers, now = Math.floor(Date.now() / 1000), notBefore = 0 }) {
  const h = (n) => headers[n.toLowerCase()];
  const keyId = h('x-scc-key-id'), ts = h('x-scc-timestamp'), nonce = h('x-scc-nonce'), sig = h('x-scc-signature');
  if (!keyId || !ts || !nonce || !sig) return { ok: false, status: 401, code: 'missing_auth' };
  const secret = secrets[keyId];
  if (!secret) return { ok: false, status: 401, code: 'unknown_key' };
  if (!path.startsWith('/_scc/v1')) return { ok: false, status: 401, code: 'bad_path' };
  if (!/^\d{9,12}$/.test(ts) || Math.abs(now - Number(ts)) > SKEW_SEC) return { ok: false, status: 401, code: 'stale_timestamp' };
  if (!/^[0-9a-f]{32,64}$/.test(nonce)) return { ok: false, status: 401, code: 'bad_nonce' };
  const want = sign(secret, { method, path, query, timestamp: ts, nonce, body });
  const a = Buffer.from(want), b = Buffer.from(String(sig));
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return { ok: false, status: 401, code: 'bad_signature' };
  const floor = Math.max(notBefore, nonces.floor || 0);
  if (Number(ts) < floor) return { ok: false, status: 401, code: 'before_store_floor' };
  try { if (!nonces.addIfAbsent(keyId, nonce, now + NONCE_TTL_SEC)) return { ok: false, status: 401, code: 'nonce_replayed' }; }
  catch { return { ok: false, status: 503, code: 'nonce_store_unavailable' }; }
  return { ok: true, keyId };
}
