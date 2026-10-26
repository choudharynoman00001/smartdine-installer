// SmartDine POS - zero-dependency Node.js API server.
// Blueprint scope: session auth (role PIN), QR public ordering with rate limit,
// orders + order_items + order_events with idempotency keys and price snapshots,
// payments with partial / void / refund + manager approval, inventory with recipe
// deduction on served, expenses, audit logs, reports, SSE realtime, static PWA.
// Embedded SQLite for local phase; swap path to PostgreSQL/Supabase is in README.
import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openDb, migrate } from './db.js';
import { initScc, handleScc, sccSuspended } from './scc.mjs';
import { seedBase, seedDemo, verifyPin } from './seed.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_FILE = process.env.SMARTDINE_DB || path.join(__dirname, 'data', 'smartdine.db');
const PORT = Number(process.env.PORT || 8080);
const SESSION_TTL_MS = 12 * 3600 * 1000;

// ---------- setup ----------
fs.mkdirSync(path.dirname(DB_FILE), { recursive: true });
const DB_PATH = DB_FILE;
const db = openDb(DB_FILE);
migrate(db);
initScc(db);
seedBase(db); // structural data only, no credentials
if (process.env.SMARTDINE_DEMO === '1') seedDemo(db); // demo users/PINs only in explicit demo mode

const getSettings = () => db.prepare('SELECT * FROM restaurants WHERE id = 1').get();

// ---------- sessions ----------
const sessions = new Map(); // token -> {userId, name, role, expires}
const loginLocks = new Map(); // sha256(pin) -> {count, until}
const pinChangeLocks = new Map(); // userId -> {count, until}
function createSession(user) {
  const token = crypto.randomBytes(24).toString('hex');
  sessions.set(token, { userId: user.id, name: user.name, role: user.role, expires: Date.now() + SESSION_TTL_MS });
  return token;
}
function getSession(req) {
  const auth = req.headers.authorization || '';
  let token = auth.startsWith('Bearer ') ? auth.slice(7) : null;
  if (!token) {
    const m = /(?:^|;\s*)sd_session=([a-f0-9]{48})/.exec(req.headers.cookie || '');
    if (m) token = m[1];
  }
  if (!token) return null;
  const s = sessions.get(token);
  if (!s) return null;
  if (s.expires < Date.now()) { sessions.delete(token); return null; }
  s.expires = Date.now() + SESSION_TTL_MS;
  return { ...s, token };
}

// ---------- SSE realtime hub ----------
const sseClients = new Set();
function broadcast(type, data) {
  const msg = `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) { try { res.write(msg); } catch { sseClients.delete(res); } }
}
setInterval(() => { for (const res of sseClients) { try { res.write(': ping\n\n'); } catch { sseClients.delete(res); } } }, 25000).unref();

// ---------- rate limiting (in-memory sliding window) ----------
const rlBuckets = new Map();
function rateLimit(key, max, windowMs) {
  const now = Date.now();
  let arr = rlBuckets.get(key);
  if (!arr) { arr = []; rlBuckets.set(key, arr); }
  while (arr.length && arr[0] <= now - windowMs) arr.shift();
  if (arr.length >= max) return false;
  arr.push(now);
  return true;
}
setInterval(() => { const cutoff = Date.now() - 3600e3; for (const [k, a] of rlBuckets) if (!a.length || a[a.length - 1] < cutoff) rlBuckets.delete(k); }, 600e3).unref();

// ---------- helpers ----------
function audit(actorId, actorName, action, detail = '') {
  db.prepare('INSERT INTO audit_logs(actor_user_id,actor_name,action,detail,at) VALUES(?,?,?,?,?)')
    .run(actorId ?? null, actorName || '', action, String(detail).slice(0, 500), Date.now());
}
function send(res, code, obj, headers = {}) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', ...headers });
  res.end(body);
}
const err = (res, code, message, extra) => send(res, code, { error: message, ...(extra || {}) });
function readRawBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > 1e6) { reject(new Error('body too large')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > 262144) { reject(new Error('body too large')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => { try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}); } catch { reject(new Error('invalid JSON')); } });
    req.on('error', reject);
  });
}
const now = () => Date.now();
const round = n => Math.round(n);

function computeTotals(subtotal, discount, settings) {
  // Tax and service charge apply to the discounted base (standard POS rule).
  const base = Math.max(0, subtotal - discount);
  const tax = round(base * settings.tax_percent / 100);
  const sc = round(base * settings.service_charge_percent / 100);
  return { tax, serviceCharge: sc, total: base + tax + sc };
}

const NO_FINANCE = ['kitchen', 'waiter'];
const ORDER_OPEN = ['new', 'preparing', 'ready', 'served', 'billed'];
// Recompute an order's money fields from its current items, keeping the discount.
function recomputeOrder(orderId) {
  const o = db.prepare('SELECT * FROM orders WHERE id = ?').get(orderId);
  if (!o) return;
  const subtotal = db.prepare('SELECT COALESCE(SUM(qty * price_snapshot),0) t FROM order_items WHERE order_id = ?').get(orderId).t;
  const { tax, serviceCharge, total } = computeTotals(subtotal, o.discount || 0, getSettings());
  db.prepare('UPDATE orders SET subtotal=?, tax=?, service_charge=?, total=? WHERE id=?').run(subtotal, tax, serviceCharge, total, orderId);
}
function orderWithItems(id, role) {
  const o = db.prepare('SELECT * FROM orders WHERE id = ?').get(id);
  if (!o) return null;
  o.items = db.prepare('SELECT * FROM order_items WHERE order_id = ? ORDER BY id').all(id);
  o.events = db.prepare('SELECT * FROM order_events WHERE order_id = ? ORDER BY id').all(id);
  o.payments = db.prepare('SELECT * FROM payments WHERE order_id = ? ORDER BY id').all(id);
  o.paid_total = o.payments.filter(p => p.status === 'confirmed').reduce((s, p) => s + p.amount, 0);
  o.table_label = o.table_id ? (db.prepare('SELECT label FROM restaurant_tables WHERE id = ?').get(o.table_id) || {}).label || '' : '';
  if (role && NO_FINANCE.includes(role)) {
    // kitchen/waiter see prep data only (PDF role scope): no money, no payments
    delete o.subtotal; delete o.tax; delete o.service_charge; delete o.discount;
    delete o.total; delete o.paid_total; delete o.payments;
    for (const it of o.items) delete it.price_snapshot;
  }
  return o;
}

function refreshTableStatus(tableId) {
  if (!tableId) return;
  const open = db.prepare("SELECT status FROM orders WHERE table_id = ? AND status NOT IN ('paid','cancelled') ORDER BY created_at DESC").all(tableId);
  let st = 'available';
  if (open.some(o => o.status === 'billed')) st = 'bill_pending';
  else if (open.some(o => o.status === 'ready')) st = 'order_ready';
  else if (open.length) st = 'occupied';
  db.prepare('UPDATE restaurant_tables SET status = ? WHERE id = ?').run(st, tableId);
}

const TRANSITIONS = {
  preparing: { from: ['new'], roles: ['kitchen', 'manager', 'owner'] },
  ready: { from: ['preparing'], roles: ['kitchen', 'manager', 'owner'] },
  delivered: { from: ['ready'], roles: ['waiter', 'manager', 'owner'] },
  billed: { from: ['delivered'], roles: ['cashier', 'manager', 'owner'] },
  cancelled: { from: ['new', 'preparing', 'ready', 'delivered', 'billed'], roles: ['manager', 'owner'] },
};

function setOrderStatus(order, status, actor, reason = '') {
  const t = TRANSITIONS[status];
  if (!t) return { code: 400, message: 'unknown status' };
  if (!t.roles.includes(actor.role)) return { code: 403, message: `role ${actor.role} cannot set ${status}` };
  if (!t.from.includes(order.status)) return { code: 409, message: `cannot move ${order.status} -> ${status}` };
  if (status === 'cancelled' && !reason) return { code: 400, message: 'cancel reason required' };
  db.exec('BEGIN IMMEDIATE');
  try {
    const ts = now();
    db.prepare('UPDATE orders SET status = ?, delivered_at = CASE WHEN ? = \'delivered\' THEN ? ELSE delivered_at END WHERE id = ?')
      .run(status, status, ts, order.id);
    db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
      .run(order.id, status, actor.userId ?? null, actor.name || '', reason, ts);
    if (status === 'delivered') deductInventory(order.id, actor);
    refreshTableStatus(order.table_id);
    audit(actor.userId, actor.name, `order.${status}`, `order ${order.id}${reason ? ': ' + reason : ''}`);
    db.exec('COMMIT');
  } catch (e) { db.exec('ROLLBACK'); throw e; }
  broadcast('order', { id: order.id, status });
  return null;
}

function deductInventory(orderId, actor) {
  const ref = 'order:' + orderId;
  if (db.prepare("SELECT COUNT(*) n FROM stock_movements WHERE ref = ? AND kind = 'usage'").get(ref).n) return; // exactly once
  const items = db.prepare('SELECT menu_item_id, qty FROM order_items WHERE order_id = ?').all(orderId);
  const ts = now();
  for (const it of items) {
    const rows = db.prepare('SELECT ingredient_id, qty FROM recipes WHERE menu_item_id = ?').all(it.menu_item_id);
    for (const r of rows) {
      const used = r.qty * it.qty;
      const ing = db.prepare('SELECT stock_qty, name FROM ingredients WHERE id = ?').get(r.ingredient_id);
      // movement records the FULL required quantity (truth of what the recipe needed)
      const short = Math.max(0, used - ing.stock_qty);
      db.prepare("INSERT INTO stock_movements(ingredient_id,kind,qty,ref,note,actor_user_id,created_at) VALUES(?,?,?,?,?,?,?)")
        .run(r.ingredient_id, 'usage', -used, ref, short ? `served; SHORT ${short}` : 'served', actor.userId ?? null, ts);
      // stock itself never goes below zero; the shortfall is explicit, not conserved away
      db.prepare('UPDATE ingredients SET stock_qty = MAX(0, stock_qty - ?) WHERE id = ?').run(used, r.ingredient_id);
      if (short > 0) {
        db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
          .run(orderId, db.prepare('SELECT status FROM orders WHERE id = ?').get(orderId).status,
            actor.userId ?? null, actor.name || '', `STOCK SHORTAGE: ${ing.name} short ${short} (needed ${used}, had ${ing.stock_qty})`, ts);
        audit(actor.userId ?? null, actor.name || '', 'stock.shortage', `${ing.name}: needed ${used}, had ${ing.stock_qty}, short ${short} (order ${orderId})`);
        broadcast('order', { id: orderId, shortage: true });
      }
    }
  }
}

function createOrder(body, source, actor) {
  const items = Array.isArray(body.items) ? body.items : [];
  if (!items.length) return { code: 400, message: 'items required' };
  if (items.length > 60) return { code: 400, message: 'too many items' };
  if (body.idempotency_key != null && String(body.idempotency_key).length > 80) return { code: 400, message: 'idempotency_key too long' };
  if (body.idempotency_key) {
    const existing = db.prepare('SELECT id FROM orders WHERE idempotency_key = ?').get(String(body.idempotency_key));
    if (existing) return { code: 200, order: orderWithItems(existing.id, body.__role), deduped: true };
  }
  const settings = getSettings();
  let tableId = null;
  if (source === 'qr') {
    const t = db.prepare('SELECT * FROM restaurant_tables WHERE qr_token = ?').get(body.__qrToken);
    if (!t) return { code: 404, message: 'table not found' };
    tableId = t.id;
    // Scope QR idempotency keys to this table so a key from another table cannot read back its order.
    if (body.idempotency_key) body.idempotency_key = 't' + tableId + ':' + String(body.idempotency_key).slice(0, 64);
  } else if (body.table_id != null) {
    const t = db.prepare('SELECT id FROM restaurant_tables WHERE id = ?').get(Number(body.table_id));
    if (!t) return { code: 404, message: 'table not found' };
    tableId = t.id;
  }
  const orderType = ['dine-in', 'takeaway', 'delivery'].includes(body.order_type) ? body.order_type : (source === 'qr' ? 'dine-in' : 'takeaway');
  if (source === 'qr' && orderType !== 'dine-in') return { code: 400, message: 'QR orders are dine-in' };

  db.exec('BEGIN IMMEDIATE');
  try {
    const ts = now();
    const id = crypto.randomUUID();
    let subtotal = 0;
    const lines = [];
    for (const it of items) {
      const qty = Number(it.qty);
      if (!Number.isInteger(qty) || qty <= 0 || qty > 50) { db.exec('ROLLBACK'); return { code: 400, message: 'invalid qty' }; }
      const mi = db.prepare('SELECT * FROM menu_items WHERE id = ?').get(Number(it.menu_item_id));
      if (!mi || !mi.available) { db.exec('ROLLBACK'); return { code: 400, message: 'menu item unavailable: ' + it.menu_item_id }; }
      subtotal += mi.price * qty;
      lines.push({ mi, qty, notes: String(it.notes || '').slice(0, 200) });
    }
    const { tax, serviceCharge, total } = computeTotals(subtotal, 0, settings);
    db.prepare(`INSERT INTO orders(id,table_id,source,status,order_type,customer_name,note,subtotal,tax,service_charge,discount,total,idempotency_key,created_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,0,?,?,?)`)
      .run(id, tableId, source, 'new', orderType, String(body.customer_name || '').slice(0, 80),
        String(body.note || '').slice(0, 300), subtotal, tax, serviceCharge, total,
        body.idempotency_key ? String(body.idempotency_key) : null, ts);
    const ins = db.prepare('INSERT INTO order_items(order_id,menu_item_id,name_snapshot,price_snapshot,qty,notes,station) VALUES(?,?,?,?,?,?,?)');
    for (const l of lines) ins.run(id, l.mi.id, l.mi.name_en, l.mi.price, l.qty, l.notes, String(l.mi.station || 'main').slice(0, 30));
    db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
      .run(id, 'new', actor ? actor.userId : null, actor ? actor.name : 'customer (QR)', '', ts);
    refreshTableStatus(tableId);
    audit(actor ? actor.userId : null, actor ? actor.name : 'customer (QR)', 'order.create', `order ${id} ${source} total=${total}`);
    db.exec('COMMIT');
    broadcast('order', { id, status: 'new' });
    return { code: 201, order: orderWithItems(id, body.__role) };
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch {}
    if (String(e.message).includes('UNIQUE') && body.idempotency_key) {
      const existing = db.prepare('SELECT id FROM orders WHERE idempotency_key = ?').get(String(body.idempotency_key));
      if (existing) return { code: 200, order: orderWithItems(existing.id, body.__role), deduped: true };
    }
    throw e;
  }
}

// ---------- RBAC ----------
const STAFF = ['owner', 'manager', 'cashier', 'waiter', 'kitchen'];
const MANAGE = ['owner', 'manager'];
const MONEY = ['owner', 'manager', 'cashier'];
const POS_CREATE = ['owner', 'manager', 'cashier', 'waiter'];
const has = (s, roles) => s && roles.includes(s.role);

// ---------- static ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.webmanifest': 'application/manifest+json', '.ico': 'image/x-icon', '.woff2': 'font/woff2' };
function serveStatic(req, res, urlPath) {
  const roots = [path.join(__dirname, 'web', 'dist'), path.join(__dirname, 'assets')];
  let rel;
  try { rel = decodeURIComponent(urlPath.split('?')[0]); }
  catch { res.writeHead(400, { 'content-type': 'application/json' }); return res.end(JSON.stringify({ error: 'bad request' })); }
  if (rel === '/') rel = '/index.html';
  if (rel.includes('..')) { res.writeHead(400); return res.end('bad path'); }
  for (const root of roots) {
    const fp = path.join(root, rel);
    if (fp.startsWith(root) && fs.existsSync(fp) && fs.statSync(fp).isFile()) {
      res.writeHead(200, { 'content-type': MIME[path.extname(fp)] || 'application/octet-stream', 'cache-control': rel.startsWith('/assets/') || fp.includes(`${path.sep}assets${path.sep}`) ? 'public, max-age=86400' : 'no-cache' });
      return fs.createReadStream(fp).pipe(res);
    }
  }
  // SPA fallback when a built frontend exists
  const idx = path.join(__dirname, 'web', 'dist', 'index.html');
  if (fs.existsSync(idx) && !path.extname(rel)) {
    res.writeHead(200, { 'content-type': MIME['.html'], 'cache-control': 'no-cache' });
    return fs.createReadStream(idx).pipe(res);
  }
  res.writeHead(404, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found', hint: 'frontend not built yet - run the web build' }));
}

// ---------- router ----------
const server = http.createServer(async (req, res) => {
  res.setHeader('content-security-policy', "default-src 'self'; img-src 'self' data:; connect-src 'self'; style-src 'self'; script-src 'self'; font-src 'self'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'");
  res.setHeader('x-content-type-options', 'nosniff');
  res.setHeader('x-frame-options', 'DENY');
  res.setHeader('referrer-policy', 'no-referrer');
  const u = new URL(req.url, 'http://x');
  const p = u.pathname;
  const ip = req.socket.remoteAddress || '?';
  try {
    if (p.startsWith('/_scc/v1/')) {
      const raw = await readRawBody(req).catch(() => null);
      if (raw == null) return err(res, 413, 'body too large');
      return handleScc(req, res, u, raw, db, send, () => { const n = sessions.size; sessions.clear(); return n; });
    }
    if (!p.startsWith('/api/')) { if (req.method === 'GET') return serveStatic(req, res, req.url); return err(res, 404, 'not found'); }
    // ---- QR public endpoints (rate limited, no session) ----
    // ---- first-run setup: localhost-only, atomic, one-time owner creation (no default credentials) ----
    if (p === '/api/setup' && req.method === 'GET') {
      return send(res, 200, { setup_required: db.prepare('SELECT COUNT(*) n FROM users').get().n === 0 });
    }
    if (p === '/api/setup' && req.method === 'POST') {
      const ra = req.socket.remoteAddress || '';
      if (!['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(ra))
        return err(res, 403, 'setup must be done on this computer');
      // a proxied "loopback" is not loopback: refuse setup through any forwarding layer
      if (req.headers['x-forwarded-for'] || req.headers['x-real-ip'] || req.headers['forwarded'])
        return err(res, 403, 'setup must be done on this computer directly, not through a proxy');
      const b = await readBody(req);
      if (!b.name || !/^\d{4,8}$/.test(String(b.pin || ''))) return err(res, 400, 'owner name and 4-8 digit pin required');
      db.exec('BEGIN IMMEDIATE'); // atomic claim: exactly one setup wins, even under parallel requests
      try {
        if (db.prepare('SELECT COUNT(*) n FROM users').get().n > 0) { db.exec('ROLLBACK'); return err(res, 409, 'setup already completed'); }
        const { hashPin } = await import('./seed.js');
        const r = db.prepare('INSERT INTO users(name,role,pin_hash,active,created_at) VALUES(?,?,?,1,?)')
          .run(String(b.name).slice(0, 60), 'owner', hashPin(String(b.pin)), now());
        db.exec('COMMIT');
        const user = { id: r.lastInsertRowid, name: String(b.name).slice(0, 60), role: 'owner' };
        audit(user.id, user.name, 'setup.owner_created', 'first-run setup');
        const token = createSession(user);
        res.setHeader('set-cookie', `sd_session=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${SESSION_TTL_MS / 1000}`);
        return send(res, 201, { token, user });
      } catch (e) { db.exec('ROLLBACK'); throw e; }
    }

    if (p.startsWith('/api/qr/')) {
      const parts = p.split('/');
      const token = parts[3] || '';
      if (!/^[a-f0-9]{32}$/.test(token)) return err(res, 404, 'not found');
      if (!rateLimit('qr:' + ip, 60, 60e3)) return err(res, 429, 'rate limit exceeded');
      const table = db.prepare('SELECT id,label,floor,status FROM restaurant_tables WHERE qr_token = ?').get(token);
      if (!table) return err(res, 404, 'table not found');
      if (req.method === 'GET' && parts.length === 4) {
        const s = getSettings();
        const cats = db.prepare('SELECT * FROM categories ORDER BY sort, id').all();
        const items = db.prepare('SELECT * FROM menu_items WHERE available = 1 ORDER BY id').all();
        return send(res, 200, {
          restaurant: { name: s.name, currency: s.currency },
          table: { label: table.label, floor: table.floor },
          categories: cats.map(c => ({ id: c.id, name_en: c.name_en, name_ur: c.name_ur, items: items.filter(i => i.category_id === c.id).map(i => ({ id: i.id, name_en: i.name_en, name_ur: i.name_ur, description: i.description, price: i.price, photo: i.photo })) })),
        });
      }
      // customer order status: scoped to this table's QR token, current session only
      if (req.method === 'GET' && parts[4] === 'orders') {
        const since = now() - 12 * 3600e3; // a dining session, not full history
        const rows = db.prepare(
          "SELECT id, status, created_at FROM orders WHERE table_id = ? AND source = 'qr' AND created_at >= ? AND status != 'cancelled' ORDER BY created_at DESC LIMIT 20"
        ).all(table.id, since);
        const orders = rows.map(o => ({
          id: o.id, status: o.status, created_at: o.created_at,
          items: db.prepare('SELECT name_snapshot AS name, qty FROM order_items WHERE order_id = ? ORDER BY id').all(o.id),
        }));
        return send(res, 200, { table: table.label, orders });
      }
      if (req.method === 'POST' && parts[4] === 'orders') {
        if (sccSuspended(db)) return err(res, 403, 'ordering is temporarily unavailable');
        if (!rateLimit('qro:' + ip, 10, 60e3)) return err(res, 429, 'order rate limit exceeded');
        const body = await readBody(req);
        body.__qrToken = token;
        const r = createOrder(body, 'qr', null);
        if (r.order) return send(res, r.code, r.deduped ? { order: r.order, deduped: true } : { order: r.order });
        return err(res, r.code, r.message);
      }
      return err(res, 404, 'not found');
    }

    // ---- auth ----
    if (p === '/api/auth/login' && req.method === 'POST') {
      if (sccSuspended(db)) return err(res, 403, 'this SmartDine install is suspended by the Subscription Control Center');
      if (!rateLimit('login:' + ip, 20, 60e3)) return err(res, 429, 'too many attempts');
      const { pin } = await readBody(req);
      // per-PIN lockout: 10 consecutive failures on one PIN locks it for 15 min
      const lkKey = crypto.createHash('sha256').update(String(pin || '')).digest('hex');
      const lk = loginLocks.get(lkKey);
      if (lk && lk.until > now()) return err(res, 429, 'too many attempts, try again later');
      const users = db.prepare('SELECT * FROM users WHERE active = 1').all();
      const user = users.find(x => verifyPin(pin, x.pin_hash));
      if (!user) {
        const e = loginLocks.get(lkKey) || { count: 0, until: 0 };
        e.count += 1;
        if (e.count >= 10) { e.until = now() + 15 * 60e3; e.count = 0; }
        loginLocks.set(lkKey, e);
        audit(null, '', 'auth.login_failed', 'bad pin');
        return err(res, 401, 'invalid PIN');
      }
      loginLocks.delete(lkKey);
      const token = createSession(user);
      audit(user.id, user.name, 'auth.login', user.role);
      res.setHeader('set-cookie', `sd_session=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${SESSION_TTL_MS / 1000}`);
      return send(res, 200, { token, user: { id: user.id, name: user.name, role: user.role } });
    }
    if (p === '/api/auth/logout' && req.method === 'POST') {
      const s = getSession(req);
      if (s) { sessions.delete(s.token); audit(s.userId, s.name, 'auth.logout', ''); }
      res.setHeader('set-cookie', 'sd_session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0');
      return send(res, 200, { ok: true });
    }

    // ---- everything below needs a staff session ----
    const s = getSession(req);
    if (p === '/api/health' && req.method === 'GET') return send(res, 200, { ok: true, app: 'SmartDine POS', version: '1.0.0' });
    if (!s || !STAFF.includes(s.role)) return err(res, 401, 'login required');
    // entitlement is re-checked on every request: a suspended install stops existing
    // sessions too, not only new logins (SCC contract: suspended = cannot use the app)
    if (sccSuspended(db)) return err(res, 403, 'this SmartDine install is suspended by the Subscription Control Center');
    if (p === '/api/auth/me') return send(res, 200, { user: { id: s.userId, name: s.name, role: s.role } });
    // self-service PIN change (session user); rejects PINs already used by another active user
    if (p === '/api/auth/change-pin' && req.method === 'POST') {
      const b = await readBody(req);
      if (!/^\d{4,8}$/.test(String(b.pin || ''))) return err(res, 400, 'new 4-8 digit pin required');
      const clk = pinChangeLocks.get(s.userId);
      if (clk && clk.until > now()) return err(res, 429, 'too many attempts, try again later');
      const meRow = db.prepare('SELECT pin_hash FROM users WHERE id = ?').get(s.userId);
      if (!verifyPin(String(b.current_pin || ''), meRow.pin_hash)) {
        const e = pinChangeLocks.get(s.userId) || { count: 0, until: 0 };
        e.count += 1;
        if (e.count >= 5) { e.until = now() + 15 * 60e3; e.count = 0; }
        pinChangeLocks.set(s.userId, e);
        audit(s.userId, s.name, 'auth.change_pin_failed', 'wrong current pin');
        return err(res, 403, 'current PIN incorrect');
      }
      pinChangeLocks.delete(s.userId);
      const clash = db.prepare('SELECT id, name, pin_hash FROM users WHERE active = 1 AND id != ?').all(s.userId).find(x => verifyPin(String(b.pin), x.pin_hash));
      if (clash) return err(res, 409, 'PIN already in use by ' + clash.name);
      const { hashPin } = await import('./seed.js');
      db.prepare('UPDATE users SET pin_hash = ? WHERE id = ?').run(hashPin(String(b.pin)), s.userId);
      audit(s.userId, s.name, 'auth.change_pin', 'self');
      return send(res, 200, { ok: true });
    }

    // SSE realtime
    if (p === '/api/events' && req.method === 'GET') {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
      res.write('event: hello\ndata: {}\n\n');
      sseClients.add(res);
      req.on('close', () => sseClients.delete(res));
      return;
    }

    // ---- orders ----
    if (p === '/api/orders' && req.method === 'GET') {
      const status = u.searchParams.get('status');
      let q = 'SELECT * FROM orders';
      const args = [];
      if (status) { q += ' WHERE status = ?'; args.push(status); }
      q += ' ORDER BY created_at DESC LIMIT 200';
      return send(res, 200, { orders: db.prepare(q).all(...args).map(o => orderWithItems(o.id, s.role)) });
    }
    if (p === '/api/orders' && req.method === 'POST') {
      if (sccSuspended(db)) return err(res, 403, 'this SmartDine install is suspended');
      if (!has(s, POS_CREATE)) return err(res, 403, 'role cannot create orders');
      const body = await readBody(req);
      body.__role = s.role;
      const r = createOrder(body, 'pos', s);
      if (r.order) return send(res, r.code, r.deduped ? { order: r.order, deduped: true } : { order: r.order });
      return err(res, r.code, r.message);
    }
    let m = /^\/api\/orders\/([0-9a-f-]{36})$/.exec(p);
    if (m && req.method === 'GET') {
      const o = orderWithItems(m[1], s.role);
      return o ? send(res, 200, { order: o }) : err(res, 404, 'order not found');
    }

    // ---- waiter ops: move table / merge orders / split bill (blueprint waiter flow) ----
    m = /^\/api\/orders\/([^/]+)\/move$/.exec(p);
    if (m && req.method === 'POST') {
      if (!has(s, ['owner', 'manager', 'waiter'])) return err(res, 403, 'waiter or manager required');
      const o = db.prepare('SELECT * FROM orders WHERE id = ?').get(m[1]);
      if (!o) return err(res, 404, 'order not found');
      if (!ORDER_OPEN.includes(o.status)) return err(res, 409, 'cannot move a ' + o.status + ' order');
      const b = await readBody(req);
      const t = db.prepare('SELECT * FROM restaurant_tables WHERE id = ?').get(Number(b.table_id));
      if (!t) return err(res, 404, 'target table not found');
      const fromTable = o.table_id;
      db.prepare('UPDATE orders SET table_id = ? WHERE id = ?').run(t.id, o.id);
      db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
        .run(o.id, o.status, s.userId, s.name, `moved from table ${fromTable} to ${t.label}`, now());
      audit(s.userId, s.name, 'order.move', `order ${o.id} table ${fromTable} -> ${t.id}`);
      refreshTableStatus(fromTable); refreshTableStatus(t.id);
      broadcast('order', { id: o.id, status: o.status });
      return send(res, 200, { order: orderWithItems(o.id, s.role) });
    }
    if (p === '/api/orders/merge' && req.method === 'POST') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager required');
      const b = await readBody(req);
      const from = db.prepare('SELECT * FROM orders WHERE id = ?').get(String(b.from_id || ''));
      const into = db.prepare('SELECT * FROM orders WHERE id = ?').get(String(b.into_id || ''));
      if (!from || !into) return err(res, 404, 'order not found');
      if (from.id === into.id) return err(res, 400, 'cannot merge an order into itself');
      if (!ORDER_OPEN.includes(from.status) || !ORDER_OPEN.includes(into.status)) return err(res, 409, 'both orders must be open');
      if (from.status === 'billed' || into.status === 'billed') return err(res, 409, 'cannot merge a billed order - settle or unbill first');
      db.exec('BEGIN IMMEDIATE');
      try {
        // money safety INSIDE the transaction: never merge when either side carries a non-void
        // payment - the cancelled source would keep its payment while the target bills again
        const paidCheck = db.prepare("SELECT order_id FROM payments WHERE order_id IN (?,?) AND status != 'void' LIMIT 1")
          .get(from.id, into.id);
        if (paidCheck) { db.exec('ROLLBACK'); return err(res, 409, 'cannot merge orders with recorded payments - refund or void them first'); }
        db.prepare('UPDATE order_items SET order_id = ? WHERE order_id = ?').run(into.id, from.id);
        db.prepare("UPDATE orders SET status = 'cancelled' WHERE id = ?").run(from.id);
        db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
          .run(from.id, 'cancelled', s.userId, s.name, `merged into order ${into.id}`, now());
        // kitchen truthfulness: merged items need preparation - a target that already left the
        // kitchen queue (ready/delivered) goes back to 'preparing' so nothing is silently missed
        if (into.status === 'ready' || into.status === 'delivered') {
          db.prepare("UPDATE orders SET status = 'preparing' WHERE id = ?").run(into.id);
          db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
            .run(into.id, 'preparing', s.userId, s.name, 'reopened to kitchen: merged new items', now());
        }
        db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
          .run(into.id, into.status, s.userId, s.name, `merged from order ${from.id}`, now());
        recomputeOrder(into.id);
        audit(s.userId, s.name, 'order.merge', `${from.id} -> ${into.id}`);
        refreshTableStatus(from.table_id); refreshTableStatus(into.table_id);
        db.exec('COMMIT');
      } catch (e) { db.exec('ROLLBACK'); throw e; }
      broadcast('order', { id: into.id, status: into.status });
      broadcast('order', { id: from.id, status: 'cancelled' });
      return send(res, 200, { order: orderWithItems(into.id, s.role) });
    }
    m = /^\/api\/orders\/([^/]+)\/split$/.exec(p);
    if (m && req.method === 'POST') {
      if (!has(s, MONEY)) return err(res, 403, 'cashier or manager required');
      const o = db.prepare('SELECT * FROM orders WHERE id = ?').get(m[1]);
      if (!o) return err(res, 404, 'order not found');
      if (!ORDER_OPEN.includes(o.status)) return err(res, 409, 'cannot split a ' + o.status + ' order');
      const b = await readBody(req);
      const ids = Array.isArray(b.item_ids) ? b.item_ids.map(Number).filter(Number.isInteger) : [];
      if (!ids.length) return err(res, 400, 'item_ids required');
      const items = db.prepare(`SELECT id FROM order_items WHERE order_id = ? AND id IN (${ids.map(() => '?').join(',')})`).all(o.id, ...ids);
      if (!items.length) return err(res, 404, 'no matching items on this order');
      if (items.length === db.prepare('SELECT COUNT(*) n FROM order_items WHERE order_id = ?').get(o.id).n)
        return err(res, 400, 'cannot split off every item - move the whole order instead');
      const newId = 'o' + now().toString(36) + Math.random().toString(36).slice(2, 7);
      db.exec('BEGIN IMMEDIATE');
      try {
        db.prepare(`INSERT INTO orders(id,table_id,source,status,order_type,customer_name,note,subtotal,tax,service_charge,discount,total,idempotency_key,created_at)
          VALUES(?,?,?,?,?,?,?,0,0,0,0,0,NULL,?)`)
          .run(newId, o.table_id, o.source, o.status, o.order_type, o.customer_name, 'split from ' + o.id, now());
        db.prepare(`UPDATE order_items SET order_id = ? WHERE id IN (${items.map(() => '?').join(',')})`).run(newId, ...items.map(i => i.id));
        recomputeOrder(o.id); recomputeOrder(newId);
        db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
          .run(o.id, o.status, s.userId, s.name, `split ${items.length} item(s) to order ${newId}`, now());
        db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
          .run(newId, 'new', s.userId, s.name, `split from order ${o.id}`, now());
        audit(s.userId, s.name, 'order.split', `${o.id} -> ${newId} (${items.length} items)`);
        db.exec('COMMIT');
      } catch (e) { db.exec('ROLLBACK'); throw e; }
      refreshTableStatus(o.table_id);
      broadcast('order', { id: o.id, status: o.status });
      broadcast('order', { id: newId, status: 'new' });
      return send(res, 201, { order: orderWithItems(newId, s.role), original: orderWithItems(o.id, s.role) });
    }
    m = /^\/api\/orders\/([0-9a-f-]{36})\/status$/.exec(p);
    if (m && req.method === 'POST') {
      const o = db.prepare('SELECT * FROM orders WHERE id = ?').get(m[1]);
      if (!o) return err(res, 404, 'order not found');
      const { status, reason } = await readBody(req);
      const fail = setOrderStatus(o, String(status || ''), s, String(reason || '').slice(0, 200));
      return fail ? err(res, fail.code, fail.message) : send(res, 200, { order: orderWithItems(o.id, s.role) });
    }
    m = /^\/api\/orders\/([0-9a-f-]{36})\/discount$/.exec(p);
    if (m && req.method === 'PUT') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager approval required for discount');
      const o = db.prepare('SELECT * FROM orders WHERE id = ?').get(m[1]);
      if (!o) return err(res, 404, 'order not found');
      if (['paid', 'cancelled'].includes(o.status)) return err(res, 409, 'order closed');
      const { amount } = await readBody(req);
      const d = Number(amount);
      if (!Number.isFinite(d) || d < 0 || d > o.subtotal) return err(res, 400, 'invalid discount');
      const settings = getSettings();
      const t = computeTotals(o.subtotal, round(d), settings);
      db.prepare('UPDATE orders SET discount = ?, tax = ?, service_charge = ?, total = ? WHERE id = ?')
        .run(round(d), t.tax, t.serviceCharge, t.total, o.id);
      audit(s.userId, s.name, 'order.discount', `order ${o.id} discount=${round(d)}`);
      broadcast('order', { id: o.id, status: o.status });
      return send(res, 200, { order: orderWithItems(o.id, s.role) });
    }
    m = /^\/api\/orders\/([0-9a-f-]{36})\/payments$/.exec(p);
    if (m && req.method === 'POST') {
      if (!has(s, MONEY)) return err(res, 403, 'cashier role required for payments');
      const o = db.prepare('SELECT * FROM orders WHERE id = ?').get(m[1]);
      if (!o) return err(res, 404, 'order not found');
      if (['paid', 'cancelled'].includes(o.status)) return err(res, 409, 'order closed');
      const { amount, method, reference } = await readBody(req);
      const amt = round(Number(amount));
      if (!Number.isFinite(amt) || amt <= 0) return err(res, 400, 'invalid amount');
      if (!['cash', 'card', 'digital'].includes(method)) return err(res, 400, 'invalid method');
      db.exec('BEGIN IMMEDIATE');
      try {
        // cash is confirmed at the till; card/digital stay pending until staff confirm
        // the real-world transfer (blueprint: never mark digital paid without verification)
        const initial = method === 'cash' ? 'confirmed' : 'pending';
        db.prepare('INSERT INTO payments(order_id,amount,method,status,reference,recorded_by,recorded_by_name,created_at) VALUES(?,?,?,?,?,?,?,?)')
          .run(o.id, amt, method, initial, String(reference || '').slice(0, 100), s.userId, s.name, now());
        audit(s.userId, s.name, 'payment.add', `order ${o.id} ${method} ${amt} (${initial})`);
        const paid = db.prepare("SELECT COALESCE(SUM(amount),0) t FROM payments WHERE order_id = ? AND status = 'confirmed'").get(o.id).t;
        if (paid >= o.total) {
          db.prepare("UPDATE orders SET status = 'paid' WHERE id = ?").run(o.id);
          db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
            .run(o.id, 'paid', s.userId, s.name, `fully paid (${paid}/${o.total})`, now());
          refreshTableStatus(o.table_id);
          broadcast('order', { id: o.id, status: 'paid' });
        }
        db.exec('COMMIT');
      } catch (e) { db.exec('ROLLBACK'); throw e; }
      return send(res, 201, { order: orderWithItems(o.id, s.role) });
    }
    m = /^\/api\/payments\/(\d+)\/confirm$/.exec(p);
    if (m && req.method === 'POST') {
      if (!has(s, MONEY)) return err(res, 403, 'cashier role required');
      const pay = db.prepare('SELECT * FROM payments WHERE id = ?').get(Number(m[1]));
      if (!pay) return err(res, 404, 'payment not found');
      if (pay.status !== 'pending') return err(res, 409, 'payment is ' + pay.status);
      const o = db.prepare('SELECT * FROM orders WHERE id = ?').get(pay.order_id);
      db.exec('BEGIN IMMEDIATE');
      try {
        db.prepare("UPDATE payments SET status = 'confirmed', status_changed_at = ?, status_changed_by = ? WHERE id = ?").run(now(), s.name, pay.id);
        audit(s.userId, s.name, 'payment.confirm', `payment ${pay.id} order ${pay.order_id}`);
        const paid = db.prepare("SELECT COALESCE(SUM(amount),0) t FROM payments WHERE order_id = ? AND status = 'confirmed'").get(o.id).t;
        if (paid >= o.total && !['paid', 'cancelled'].includes(o.status)) {
          db.prepare("UPDATE orders SET status = 'paid' WHERE id = ?").run(o.id);
          db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
            .run(o.id, 'paid', s.userId, s.name, `fully paid (${paid}/${o.total})`, now());
          refreshTableStatus(o.table_id);
          broadcast('order', { id: o.id, status: 'paid' });
        }
        db.exec('COMMIT');
      } catch (e) { db.exec('ROLLBACK'); throw e; }
      broadcast('payment', { orderId: pay.order_id });
      return send(res, 200, { order: orderWithItems(pay.order_id, s.role) });
    }
    m = /^\/api\/payments\/(\d+)\/(void|refund)$/.exec(p);
    if (m && req.method === 'POST') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager approval required');
      const pay = db.prepare('SELECT * FROM payments WHERE id = ?').get(Number(m[1]));
      if (!pay) return err(res, 404, 'payment not found');
      if (pay.status !== 'confirmed') return err(res, 409, 'payment already ' + pay.status);
      const { reason } = await readBody(req);
      if (!reason) return err(res, 400, 'reason required');
      const newStatus = m[2] === 'void' ? 'voided' : 'refunded';
      const o = db.prepare('SELECT * FROM orders WHERE id = ?').get(pay.order_id);
      db.exec('BEGIN IMMEDIATE');
      try {
        db.prepare('UPDATE payments SET status = ?, status_changed_at = ?, status_changed_by = ? WHERE id = ?').run(newStatus, now(), s.name, pay.id);
        audit(s.userId, s.name, `payment.${m[2]}`, `payment ${pay.id} order ${pay.order_id}: ${String(reason).slice(0, 200)}`);
        // reconcile the order: a voided/refunded payment no longer counts, so a
        // fully-paid order can reopen for the remaining balance
        const paid = db.prepare("SELECT COALESCE(SUM(amount),0) t FROM payments WHERE order_id = ? AND status = 'confirmed'").get(o.id).t;
        if (o.status === 'paid' && paid < o.total) {
          db.prepare("UPDATE orders SET status = 'billed' WHERE id = ?").run(o.id);
          db.prepare('INSERT INTO order_events(order_id,status,actor_user_id,actor_name,reason,at) VALUES(?,?,?,?,?,?)')
            .run(o.id, 'billed', s.userId, s.name, `payment ${pay.id} ${newStatus}; balance ${o.total - paid} due`, now());
          refreshTableStatus(o.table_id);
          broadcast('order', { id: o.id, status: 'billed' });
        }
        db.exec('COMMIT');
      } catch (e) { db.exec('ROLLBACK'); throw e; }
      broadcast('payment', { orderId: pay.order_id });
      return send(res, 200, { order: orderWithItems(pay.order_id, s.role) });
    }

    // ---- tables / floor ----
    if (p === '/api/tables' && req.method === 'GET') {
      const rows = db.prepare('SELECT * FROM restaurant_tables ORDER BY floor, label').all();
      // qr_token is the customer access credential - only manager/owner may read it
      return send(res, 200, { tables: has(s, MANAGE) ? rows : rows.map(({ qr_token, ...rest }) => rest) });
    }
    if (p === '/api/tables' && req.method === 'POST') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager required');
      const { label, floor } = await readBody(req);
      if (!label) return err(res, 400, 'label required');
      const r = db.prepare('INSERT INTO restaurant_tables(label,floor,qr_token,status) VALUES(?,?,?,?)')
        .run(String(label).slice(0, 40), String(floor || '').slice(0, 40), crypto.randomBytes(16).toString('hex'), 'available');
      audit(s.userId, s.name, 'table.create', label);
      return send(res, 201, { table: db.prepare('SELECT * FROM restaurant_tables WHERE id = ?').get(r.lastInsertRowid) });
    }

    // ---- menu ----
    if (p === '/api/menu' && req.method === 'GET') {
      const cats = db.prepare('SELECT * FROM categories ORDER BY sort, id').all();
      const items = db.prepare('SELECT * FROM menu_items ORDER BY id').all();
      return send(res, 200, { categories: cats.map(c => ({ ...c, items: items.filter(i => i.category_id === c.id) })) });
    }
    if (p === '/api/menu/categories' && req.method === 'POST') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager required');
      const b = await readBody(req);
      if (!b.name_en) return err(res, 400, 'name_en required');
      const sort = db.prepare('SELECT COALESCE(MAX(sort),0)+1 s FROM categories').get().s;
      const r = db.prepare('INSERT INTO categories(name_en,name_ur,sort) VALUES(?,?,?)')
        .run(String(b.name_en).slice(0, 60), String(b.name_ur || '').slice(0, 60), sort);
      audit(s.userId, s.name, 'menu.category_create', b.name_en);
      return send(res, 201, { category: db.prepare('SELECT * FROM categories WHERE id = ?').get(r.lastInsertRowid) });
    }
    if (p === '/api/menu/items' && req.method === 'POST') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager required');
      const b = await readBody(req);
      if (!b.name_en || !b.category_id || !Number.isFinite(Number(b.price)) || Number(b.price) < 0 || Number(b.price) > 1e7) return err(res, 400, 'name_en, category_id, valid price required');
      if (!db.prepare('SELECT id FROM categories WHERE id = ?').get(Number(b.category_id))) return err(res, 404, 'category not found');
      const r = db.prepare('INSERT INTO menu_items(category_id,name_en,name_ur,description,price,photo,available,station) VALUES(?,?,?,?,?,?,1,?)')
        .run(Number(b.category_id), String(b.name_en).slice(0, 80), String(b.name_ur || '').slice(0, 120), String(b.description || '').slice(0, 300), round(Number(b.price)), String(b.photo || ''), String(b.station || 'main').slice(0, 30));
      audit(s.userId, s.name, 'menu.item_create', b.name_en);
      return send(res, 201, { item: db.prepare('SELECT * FROM menu_items WHERE id = ?').get(r.lastInsertRowid) });
    }
    m = /^\/api\/menu\/items\/(\d+)$/.exec(p);
    if (m && req.method === 'PUT') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager required');
      const it = db.prepare('SELECT * FROM menu_items WHERE id = ?').get(Number(m[1]));
      if (!it) return err(res, 404, 'item not found');
      const b = await readBody(req);
      db.prepare('UPDATE menu_items SET name_en=?, name_ur=?, description=?, price=?, available=?, station=?, photo=? WHERE id=?')
        .run(b.name_en ?? it.name_en, b.name_ur ?? it.name_ur, b.description ?? it.description,
          b.price != null ? round(Number(b.price)) : it.price, b.available != null ? (b.available ? 1 : 0) : it.available,
          b.station != null ? String(b.station).slice(0, 30) : it.station,
          b.photo != null ? String(b.photo).slice(0, 500000) : it.photo, it.id);
      audit(s.userId, s.name, 'menu.item_update', `item ${it.id}`);
      return send(res, 200, { item: db.prepare('SELECT * FROM menu_items WHERE id = ?').get(it.id) });
    }

    // ---- inventory ----
    if (p === '/api/inventory/ingredients' && req.method === 'GET') {
      const rows = db.prepare('SELECT * FROM ingredients ORDER BY name').all();
      return send(res, 200, { ingredients: rows.map(r => ({ ...r, low_stock: r.stock_qty <= r.reorder_threshold })) });
    }
    if (p === '/api/inventory/ingredients' && req.method === 'POST') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager required');
      const b = await readBody(req);
      if (!b.name) return err(res, 400, 'name required');
      const r = db.prepare('INSERT INTO ingredients(name,unit,stock_qty,reorder_threshold,supplier) VALUES(?,?,?,?,?)')
        .run(String(b.name).slice(0, 80), String(b.unit || 'g').slice(0, 12), Number(b.stock_qty) || 0, Number(b.reorder_threshold) || 0, String(b.supplier || '').slice(0, 80));
      audit(s.userId, s.name, 'ingredient.create', b.name);
      return send(res, 201, { ingredient: db.prepare('SELECT * FROM ingredients WHERE id = ?').get(r.lastInsertRowid) });
    }
    if (p === '/api/inventory/recipes' && req.method === 'GET')
      return send(res, 200, { recipes: db.prepare('SELECT r.*, i.name ingredient_name, i.unit FROM recipes r JOIN ingredients i ON i.id = r.ingredient_id ORDER BY r.menu_item_id').all() });
    if (p === '/api/inventory/recipes' && req.method === 'POST') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager required');
      const b = await readBody(req);
      if (!b.menu_item_id || !b.ingredient_id || !(Number(b.qty) > 0)) return err(res, 400, 'menu_item_id, ingredient_id, qty required');
      if (!db.prepare('SELECT id FROM menu_items WHERE id = ?').get(Number(b.menu_item_id))) return err(res, 404, 'menu item not found');
      if (!db.prepare('SELECT id FROM ingredients WHERE id = ?').get(Number(b.ingredient_id))) return err(res, 404, 'ingredient not found');
      if (Number(b.qty) > 1e6) return err(res, 400, 'qty out of range');
      db.prepare('INSERT INTO recipes(menu_item_id,ingredient_id,qty) VALUES(?,?,?) ON CONFLICT(menu_item_id,ingredient_id) DO UPDATE SET qty = excluded.qty')
        .run(Number(b.menu_item_id), Number(b.ingredient_id), Number(b.qty));
      audit(s.userId, s.name, 'recipe.set', `item ${b.menu_item_id} ing ${b.ingredient_id}`);
      return send(res, 200, { ok: true });
    }
    if (p === '/api/inventory/movements' && req.method === 'GET') {
      const ing = u.searchParams.get('ingredient_id');
      const q = ing ? 'SELECT sm.*, i.name ingredient_name FROM stock_movements sm JOIN ingredients i ON i.id = sm.ingredient_id WHERE sm.ingredient_id = ? ORDER BY sm.id DESC LIMIT 200'
        : 'SELECT sm.*, i.name ingredient_name FROM stock_movements sm JOIN ingredients i ON i.id = sm.ingredient_id ORDER BY sm.id DESC LIMIT 200';
      return send(res, 200, { movements: ing ? db.prepare(q).all(Number(ing)) : db.prepare(q).all() });
    }
    if (p === '/api/inventory/movements' && req.method === 'POST') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager required');
      const b = await readBody(req);
      const kind = String(b.kind || '');
      const qty = Number(b.qty);
      if (!['purchase', 'wastage', 'adjustment'].includes(kind)) return err(res, 400, 'kind must be purchase|wastage|adjustment');
      const ing = db.prepare('SELECT * FROM ingredients WHERE id = ?').get(Number(b.ingredient_id));
      if (!ing) return err(res, 404, 'ingredient not found');
      if (!Number.isFinite(qty)) return err(res, 400, 'qty must be a number');
      if (kind !== 'adjustment' && qty <= 0) return err(res, 400, 'qty must be > 0');
      const delta = kind === 'purchase' ? qty : kind === 'wastage' ? -qty : qty;
      const signed = kind === 'adjustment' ? qty : delta; // adjustment: qty itself is signed
      if (kind === 'adjustment' && signed === 0) return err(res, 400, 'adjustment qty must be signed non-zero');
      const change = kind === 'adjustment' ? signed : delta;
      if (Math.abs(change) > 1e6) return err(res, 400, 'qty out of range');
      if (ing.stock_qty + change < 0) return err(res, 409, `insufficient stock: ${ing.name} has ${ing.stock_qty} ${ing.unit}`);
      db.exec('BEGIN IMMEDIATE');
      try {
        const uc = kind === 'purchase' && b.unit_cost != null ? Number(b.unit_cost) : null;
        if (uc != null && (!Number.isFinite(uc) || uc < 0 || uc > 1e9)) { db.exec('ROLLBACK'); return err(res, 400, 'unit_cost out of range'); }
        db.prepare('INSERT INTO stock_movements(ingredient_id,kind,qty,ref,note,actor_user_id,created_at,unit_cost) VALUES(?,?,?,?,?,?,?,?)')
          .run(ing.id, kind, change, String(b.ref || '').slice(0, 80), String(b.note || '').slice(0, 200), s.userId, now(), uc);
        db.prepare('UPDATE ingredients SET stock_qty = stock_qty + ? WHERE id = ?').run(change, ing.id);
        audit(s.userId, s.name, `stock.${kind}`, `${ing.name} ${kind === 'adjustment' ? signed : delta}`);
        db.exec('COMMIT');
      } catch (e) { db.exec('ROLLBACK'); throw e; }
      const updated = db.prepare('SELECT * FROM ingredients WHERE id = ?').get(ing.id);
      return send(res, 201, { ingredient: { ...updated, low_stock: updated.stock_qty <= updated.reorder_threshold } });
    }

    // ---- expenses ----
    if (p === '/api/expenses' && req.method === 'GET') {
      if (!has(s, MONEY)) return err(res, 403, 'finance role required');
      const from = Number(u.searchParams.get('from')) || 0;
      const to = Number(u.searchParams.get('to')) || Number.MAX_SAFE_INTEGER;
      return send(res, 200, { expenses: db.prepare('SELECT * FROM expenses WHERE created_at BETWEEN ? AND ? ORDER BY id DESC LIMIT 500').all(from, to) });
    }
    if (p === '/api/expenses' && req.method === 'POST') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager required');
      const b = await readBody(req);
      const amt = round(Number(b.amount));
      if (!b.category || !Number.isFinite(amt) || amt <= 0) return err(res, 400, 'category and positive amount required');
      const r = db.prepare('INSERT INTO expenses(category,amount,spent_on,notes,actor_user_id,created_at) VALUES(?,?,?,?,?,?)')
        .run(String(b.category).slice(0, 60), amt, String(b.spent_on || new Date().toISOString().slice(0, 10)).slice(0, 20), String(b.notes || '').slice(0, 300), s.userId, now());
      audit(s.userId, s.name, 'expense.add', `${b.category} ${amt}`);
      return send(res, 201, { expense: db.prepare('SELECT * FROM expenses WHERE id = ?').get(r.lastInsertRowid) });
    }

    // ---- reports ----
    if (p.startsWith('/api/reports/') && !has(s, MONEY)) return err(res, 403, 'finance role required');
    if (p === '/api/reports/summary' && req.method === 'GET') {
      const from = Number(u.searchParams.get('from')) || 0;
      const to = Number(u.searchParams.get('to')) || Number.MAX_SAFE_INTEGER;
      const sales = db.prepare("SELECT COUNT(*) orders, COALESCE(SUM(total),0) revenue, COALESCE(SUM(tax),0) tax, COALESCE(SUM(service_charge),0) service_charge, COALESCE(SUM(discount),0) discounts FROM orders WHERE status = 'paid' AND created_at BETWEEN ? AND ?").get(from, to);
      const byMethod = db.prepare("SELECT method, COALESCE(SUM(amount),0) total, COUNT(*) count FROM payments WHERE status = 'confirmed' AND created_at BETWEEN ? AND ? GROUP BY method").all(from, to);
      const topItems = db.prepare(`SELECT oi.name_snapshot name, SUM(oi.qty) qty, SUM(oi.qty * oi.price_snapshot) revenue FROM order_items oi
        JOIN orders o ON o.id = oi.order_id WHERE o.status = 'paid' AND o.created_at BETWEEN ? AND ? GROUP BY oi.menu_item_id ORDER BY qty DESC LIMIT 10`).all(from, to);
      const expenseTotal = db.prepare('SELECT COALESCE(SUM(amount),0) t FROM expenses WHERE created_at BETWEEN ? AND ?').get(from, to).t;
      const expensesByCat = db.prepare('SELECT category, COALESCE(SUM(amount),0) total FROM expenses WHERE created_at BETWEEN ? AND ? GROUP BY category ORDER BY total DESC').all(from, to);
      const openOrders = db.prepare("SELECT COUNT(*) n FROM orders WHERE status NOT IN ('paid','cancelled')").get().n;
      const lowStock = db.prepare('SELECT COUNT(*) n FROM ingredients WHERE stock_qty <= reorder_threshold').get().n;
      const refunds = db.prepare("SELECT COUNT(*) count, COALESCE(SUM(amount),0) total FROM payments WHERE status = 'refunded' AND COALESCE(status_changed_at, created_at) BETWEEN ? AND ?").get(from, to);
      const refundsByMethod = db.prepare("SELECT method, COUNT(*) count, COALESCE(SUM(amount),0) total FROM payments WHERE status = 'refunded' AND COALESCE(status_changed_at, created_at) BETWEEN ? AND ? GROUP BY method").all(from, to);
      const refundsByStaff = db.prepare("SELECT COALESCE(status_changed_by, recorded_by_name) staff, COUNT(*) count, COALESCE(SUM(amount),0) total FROM payments WHERE status = 'refunded' AND COALESCE(status_changed_at, created_at) BETWEEN ? AND ? GROUP BY staff ORDER BY total DESC").all(from, to);
      // estimated food cost: usage in range x each ingredient's average recorded purchase cost
      const usage = db.prepare("SELECT ingredient_id, SUM(-qty) used FROM stock_movements WHERE kind = 'usage' AND created_at BETWEEN ? AND ? GROUP BY ingredient_id").all(from, to);
      let foodCost = 0, foodCostPriced = 0;
      for (const uRow of usage) {
        const avg = db.prepare("SELECT AVG(unit_cost) c FROM stock_movements WHERE ingredient_id = ? AND kind = 'purchase' AND unit_cost IS NOT NULL").get(uRow.ingredient_id).c;
        if (avg != null) { foodCost += uRow.used * avg; foodCostPriced += uRow.used; }
      }
      return send(res, 200, { summary: { ...sales, aov: sales.orders ? Math.round(sales.revenue / sales.orders) : 0,
        expenses: expenseTotal, expenses_by_category: expensesByCat, net: sales.revenue - expenseTotal,
        refunds: { ...refunds, by_method: refundsByMethod, by_staff: refundsByStaff },
        estimated_food_cost: Math.round(foodCost), estimated_food_cost_note: 'estimate from recorded purchase costs; ingredients without a recorded purchase cost are excluded',
        payments_by_method: byMethod, top_items: topItems, open_orders: openOrders, low_stock_count: lowStock } });
    }
    // sales by day / week / month (non-cancelled orders by creation time)
    if (p === '/api/reports/sales' && req.method === 'GET') {
      const from = Number(u.searchParams.get('from')) || 0;
      const to = Number(u.searchParams.get('to')) || Number.MAX_SAFE_INTEGER;
      const g = String(u.searchParams.get('group') || 'day');
      const fmt = g === 'month' ? '%Y-%m' : g === 'week' ? '%Y-W%W' : '%Y-%m-%d';
      if (!['day', 'week', 'month'].includes(g)) return err(res, 400, 'group must be day|week|month');
      const rows = db.prepare(`SELECT strftime('${fmt}', created_at / 1000, 'unixepoch') period, COUNT(*) orders,
        COALESCE(SUM(total),0) revenue, COALESCE(SUM(discount),0) discounts
        FROM orders WHERE status != 'cancelled' AND created_at BETWEEN ? AND ? GROUP BY period ORDER BY period DESC LIMIT 400`).all(from, to);
      for (const r of rows) r.aov = r.orders ? Math.round(r.revenue / r.orders) : 0;
      return send(res, 200, { group: g, rows });
    }
    // staff activity: audited actions per staff member in range
    if (p === '/api/reports/staff' && req.method === 'GET') {
      const from = Number(u.searchParams.get('from')) || 0;
      const to = Number(u.searchParams.get('to')) || Number.MAX_SAFE_INTEGER;
      const rows = db.prepare(`SELECT actor_name staff, action, COUNT(*) count FROM audit_logs
        WHERE actor_name != '' AND at BETWEEN ? AND ? GROUP BY actor_name, action ORDER BY actor_name, count DESC`).all(from, to);
      return send(res, 200, { rows });
    }
    // CSV export for accounting / spreadsheets
    if (p === '/api/reports/export.csv' && req.method === 'GET') {
      const from = Number(u.searchParams.get('from')) || 0;
      const to = Number(u.searchParams.get('to')) || Number.MAX_SAFE_INTEGER;
      const type = String(u.searchParams.get('type') || 'orders');
      const csvEsc = v => {
        let s2 = String(v ?? '');
        if (/^[=+\-@]/.test(s2)) s2 = "'" + s2; // neutralize spreadsheet formula execution on import
        return /[",\n]/.test(s2) ? '"' + s2.replace(/"/g, '""') + '"' : s2;
      };
      const toCsv = (cols, rows) => [cols.join(','), ...rows.map(r => cols.map(c => csvEsc(r[c])).join(','))].join('\n');
      let csv, name;
      if (type === 'orders') {
        const rows = db.prepare("SELECT id, table_id, source, status, order_type, customer_name, subtotal, tax, service_charge, discount, total, created_at FROM orders WHERE created_at BETWEEN ? AND ? ORDER BY created_at").all(from, to);
        csv = toCsv(['id', 'table_id', 'source', 'status', 'order_type', 'customer_name', 'subtotal', 'tax', 'service_charge', 'discount', 'total', 'created_at'], rows); name = 'orders';
      } else if (type === 'payments') {
        const rows = db.prepare("SELECT id, order_id, amount, method, status, reference, recorded_by_name, created_at, status_changed_at, status_changed_by FROM payments WHERE created_at BETWEEN ? AND ? ORDER BY created_at").all(from, to);
        csv = toCsv(['id', 'order_id', 'amount', 'method', 'status', 'reference', 'recorded_by_name', 'created_at', 'status_changed_at', 'status_changed_by'], rows); name = 'payments';
      } else if (type === 'expenses') {
        const rows = db.prepare("SELECT id, category, amount, note, created_at FROM expenses WHERE created_at BETWEEN ? AND ? ORDER BY created_at").all(from, to);
        csv = toCsv(['id', 'category', 'amount', 'note', 'created_at'], rows); name = 'expenses';
      } else return err(res, 400, 'type must be orders|payments|expenses');
      res.writeHead(200, { 'content-type': 'text/csv; charset=utf-8', 'content-disposition': `attachment; filename="smartdine-${name}.csv"` });
      return res.end(csv);
    }
    if (p === '/api/reports/cash' && req.method === 'GET') {
      const from = Number(u.searchParams.get('from')) || 0;
      const to = Number(u.searchParams.get('to')) || Number.MAX_SAFE_INTEGER;
      const cashIn = db.prepare("SELECT COALESCE(SUM(amount),0) t FROM payments WHERE status = 'confirmed' AND method = 'cash' AND created_at BETWEEN ? AND ?").get(from, to).t;
      const cashRefunded = db.prepare("SELECT COALESCE(SUM(amount),0) t FROM payments WHERE status = 'refunded' AND method = 'cash' AND COALESCE(status_changed_at, created_at) BETWEEN ? AND ?").get(from, to).t;
      const cashExpenses = db.prepare("SELECT COALESCE(SUM(amount),0) t FROM expenses WHERE created_at BETWEEN ? AND ?").get(from, to).t;
      // refunds count from when they happened (status_changed_at), not when the payment was created
      const expected = cashIn - cashRefunded - cashExpenses;
      const latest = db.prepare('SELECT * FROM cash_counts ORDER BY id DESC LIMIT 1').get();
      return send(res, 200, { cash: {
        cash_in: cashIn, cash_refunded: cashRefunded, expenses: cashExpenses,
        expenses_assumed_cash: true, // local phase: expenses are recorded as paid from the drawer
        expected_drawer: expected,
        last_count: latest || null,
        variance: latest ? latest.amount - expected : null,
      } });
    }
    if (p === '/api/reports/cash/count' && req.method === 'POST') {
      const b = await readBody(req);
      const amount = Number(b.amount);
      if (!Number.isFinite(amount) || amount < 0 || amount > 1e9) return err(res, 400, 'counted amount required');
      db.prepare('INSERT INTO cash_counts(amount,note,actor_name,counted_at) VALUES(?,?,?,?)')
        .run(round(amount), String(b.note || '').slice(0, 200), s.name, now());
      audit(s.userId, s.name, 'cash.count', `counted ${round(amount)}`);
      return send(res, 201, { ok: true });
    }
    if (p === '/api/audit' && req.method === 'GET') {
      if (!has(s, MANAGE)) return err(res, 403, 'manager required');
      return send(res, 200, { logs: db.prepare('SELECT * FROM audit_logs ORDER BY id DESC LIMIT 300').all() });
    }

    // ---- backup (owner): downloadable SQLite snapshot; restore = replace the file while stopped (README) ----
    if (p === '/api/backup' && req.method === 'GET') {
      if (s.role !== 'owner') return err(res, 403, 'owner required');
      db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
      const tmp = path.join('/tmp', `sd-backup-${crypto.randomBytes(16).toString('hex')}.db`);
      let data;
      try {
        fs.copyFileSync(DB_PATH, tmp);
        data = fs.readFileSync(tmp);
      } finally {
        try { fs.unlinkSync(tmp); } catch { /* best effort */ }
      }
      audit(s.userId, s.name, 'backup.download', `${data.length} bytes`);
      res.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': `attachment; filename="smartdine-backup-${new Date().toISOString().slice(0, 10)}.db"`,
        'content-length': data.length,
      });
      return res.end(data);
    }

    // ---- settings (owner) ----
    if (p === '/api/settings' && req.method === 'GET') {
      const st = getSettings();
      return send(res, 200, { settings: { name: st.name, currency: st.currency, tax_percent: st.tax_percent, service_charge_percent: st.service_charge_percent, default_language: st.default_language } });
    }
    if (p === '/api/settings' && req.method === 'PUT') {
      if (s.role !== 'owner') return err(res, 403, 'owner required');
      const b = await readBody(req);
      const st = getSettings();
      const tax = b.tax_percent != null ? Number(b.tax_percent) : st.tax_percent;
      const scp = b.service_charge_percent != null ? Number(b.service_charge_percent) : st.service_charge_percent;
      if (!Number.isFinite(tax) || tax < 0 || tax > 100) return err(res, 400, 'tax_percent must be 0-100');
      if (!Number.isFinite(scp) || scp < 0 || scp > 100) return err(res, 400, 'service_charge_percent must be 0-100');
      db.prepare('UPDATE restaurants SET name=?, tax_percent=?, service_charge_percent=?, default_language=?, updated_at=? WHERE id=1')
        .run(String(b.name ?? st.name).slice(0, 80), tax, scp,
          ['en', 'ur'].includes(b.default_language) ? b.default_language : st.default_language, now());
      audit(s.userId, s.name, 'settings.update', JSON.stringify(b).slice(0, 200));
      return send(res, 200, { settings: getSettings() });
    }

    // ---- users (owner) ----
    if (p === '/api/users' && req.method === 'GET') {
      if (s.role !== 'owner') return err(res, 403, 'owner required');
      return send(res, 200, { users: db.prepare('SELECT id,name,role,active,created_at FROM users ORDER BY id').all() });
    }
    if (p === '/api/users' && req.method === 'POST') {
      if (s.role !== 'owner') return err(res, 403, 'owner required');
      const b = await readBody(req);
      if (!b.name || !STAFF.includes(b.role) || !/^\d{4,8}$/.test(String(b.pin || ''))) return err(res, 400, 'name, valid role, 4-8 digit pin required');
      // duplicate PINs make login role attribution ambiguous (first match wins) - reject
      const clash = db.prepare('SELECT name, pin_hash FROM users WHERE active = 1').all().find(x => verifyPin(b.pin, x.pin_hash));
      if (clash) return err(res, 409, 'PIN already in use by ' + clash.name);
      const { hashPin } = await import('./seed.js');
      const r = db.prepare('INSERT INTO users(name,role,pin_hash,active,created_at) VALUES(?,?,?,1,?)').run(String(b.name).slice(0, 60), b.role, hashPin(b.pin), now());
      audit(s.userId, s.name, 'user.create', `${b.name} (${b.role})`);
      return send(res, 201, { user: db.prepare('SELECT id,name,role,active FROM users WHERE id = ?').get(r.lastInsertRowid) });
    }
    m = /^\/api\/users\/(\d+)\/active$/.exec(p);
    if (m && req.method === 'PUT') {
      if (s.role !== 'owner') return err(res, 403, 'owner required');
      const b = await readBody(req);
      const target = db.prepare('SELECT id, role, active FROM users WHERE id = ?').get(Number(m[1]));
      if (!target) return err(res, 404, 'user not found');
      if (!b.active) {
        if (target.id === s.userId) return err(res, 409, 'cannot deactivate your own account');
        if (target.role === 'owner' && db.prepare("SELECT COUNT(*) n FROM users WHERE role = 'owner' AND active = 1").get().n <= 1)
          return err(res, 409, 'cannot deactivate the last active owner');
      }
      if (b.active) {
        // reactivation always sets a FRESH clash-checked PIN: the old one was destroyed at
        // deactivation, so a recycled PIN can never resurrect an ambiguous identity
        if (!/^\d{4,8}$/.test(String(b.pin || ''))) return err(res, 400, 'reactivation requires a new 4-8 digit pin');
        const clash = db.prepare('SELECT name, pin_hash FROM users WHERE active = 1 AND id != ?').all(target.id).find(x => verifyPin(String(b.pin), x.pin_hash));
        if (clash) return err(res, 409, 'PIN already in use by ' + clash.name);
        const { hashPin } = await import('./seed.js');
        db.prepare('UPDATE users SET active = 1, pin_hash = ? WHERE id = ?').run(hashPin(String(b.pin)), target.id);
        for (const [tok, sess] of sessions) if (sess.userId === target.id) sessions.delete(tok);
        audit(s.userId, s.name, 'user.active', `user ${m[1]} -> 1 (pin reset)`);
        return send(res, 200, { ok: true });
      }
      const { hashPin: hr } = await import('./seed.js');
      db.prepare('UPDATE users SET active = 0, pin_hash = ? WHERE id = ?').run('disabled$' + hr(crypto.randomBytes(8).toString('hex')), Number(m[1]));
      for (const [tok, sess] of sessions) if (sess.userId === Number(m[1])) sessions.delete(tok); // kill live sessions on deactivation
      audit(s.userId, s.name, 'user.active', `user ${m[1]} -> ${b.active ? 1 : 0}`);
      return send(res, 200, { ok: true });
    }

    m = /^\/api\/users\/(\d+)\/pin$/.exec(p);
    if (m && req.method === 'PUT') {
      if (s.role !== 'owner') return err(res, 403, 'owner required');
      const target = db.prepare('SELECT id, name FROM users WHERE id = ?').get(Number(m[1]));
      if (!target) return err(res, 404, 'user not found');
      const b = await readBody(req);
      if (!/^\d{4,8}$/.test(String(b.pin || ''))) return err(res, 400, 'new 4-8 digit pin required');
      const clash = db.prepare('SELECT name, pin_hash FROM users WHERE active = 1 AND id != ?').all(target.id).find(x => verifyPin(String(b.pin), x.pin_hash));
      if (clash) return err(res, 409, 'PIN already in use by ' + clash.name);
      const { hashPin } = await import('./seed.js');
      db.prepare('UPDATE users SET pin_hash = ? WHERE id = ?').run(hashPin(String(b.pin)), target.id);
      for (const [tok, sess] of sessions) if (sess.userId === target.id) sessions.delete(tok); // revoke existing sessions
      audit(s.userId, s.name, 'user.pin_reset', `user ${target.id} (${target.name})`);
      return send(res, 200, { ok: true });
    }

    return err(res, 404, 'not found');
  } catch (e) {
    if (e && e.message === 'invalid JSON') return err(res, 400, 'invalid JSON body');
    if (e && e.message === 'body too large') return err(res, 413, 'body too large');
    console.error(e);
    return err(res, 500, 'internal error');
  }
});

if (process.env.SMARTDINE_NO_LISTEN !== '1') {
  server.listen(PORT, () => console.log(`SmartDine API listening on http://localhost:${PORT}`));
}
export { server, db };
