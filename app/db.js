// SmartDine POS - database layer. Schema follows the SmartDine blueprint (13 tables).
// Embedded SQLite for the local build; DDL uses standard SQL types so the same
// schema maps to PostgreSQL/Supabase for the hosted phase.
import { DatabaseSync } from 'node:sqlite';

export function openDb(file = ':memory:') {
  const db = new DatabaseSync(file);
  db.exec(`
CREATE TABLE IF NOT EXISTS restaurants(
  id INTEGER PRIMARY KEY CHECK (id = 1),
  name TEXT NOT NULL,
  currency TEXT NOT NULL DEFAULT 'PKR',
  tax_percent REAL NOT NULL DEFAULT 0,
  service_charge_percent REAL NOT NULL DEFAULT 0,
  default_language TEXT NOT NULL DEFAULT 'en',
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS users(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('owner','manager','cashier','waiter','kitchen')),
  pin_hash TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS restaurant_tables(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  label TEXT NOT NULL,
  floor TEXT NOT NULL DEFAULT '',
  qr_token TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'available' CHECK (status IN ('available','occupied','order_ready','bill_pending'))
);
CREATE TABLE IF NOT EXISTS categories(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name_en TEXT NOT NULL,
  name_ur TEXT NOT NULL DEFAULT '',
  sort INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS menu_items(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  category_id INTEGER NOT NULL REFERENCES categories(id),
  name_en TEXT NOT NULL,
  name_ur TEXT NOT NULL DEFAULT '',
  description TEXT NOT NULL DEFAULT '',
  price INTEGER NOT NULL CHECK (price >= 0),
  photo TEXT NOT NULL DEFAULT '',
  available INTEGER NOT NULL DEFAULT 1
,
  station TEXT NOT NULL DEFAULT 'main'
);
CREATE TABLE IF NOT EXISTS orders(
  id TEXT PRIMARY KEY,
  table_id INTEGER REFERENCES restaurant_tables(id),
  source TEXT NOT NULL CHECK (source IN ('qr','pos')),
  status TEXT NOT NULL DEFAULT 'new' CHECK (status IN ('new','preparing','ready','delivered','billed','paid','cancelled')),
  order_type TEXT NOT NULL DEFAULT 'dine-in' CHECK (order_type IN ('dine-in','takeaway','delivery')),
  customer_name TEXT NOT NULL DEFAULT '',
  note TEXT NOT NULL DEFAULT '',
  subtotal INTEGER NOT NULL DEFAULT 0,
  tax INTEGER NOT NULL DEFAULT 0,
  service_charge INTEGER NOT NULL DEFAULT 0,
  discount INTEGER NOT NULL DEFAULT 0,
  total INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT UNIQUE,
  created_at INTEGER NOT NULL,
  delivered_at INTEGER
);
CREATE TABLE IF NOT EXISTS order_items(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  order_id TEXT NOT NULL REFERENCES orders(id),
  menu_item_id INTEGER NOT NULL,
  name_snapshot TEXT NOT NULL,
  price_snapshot INTEGER NOT NULL,
  qty INTEGER NOT NULL CHECK (qty > 0),
  notes TEXT NOT NULL DEFAULT '',
  station TEXT NOT NULL DEFAULT 'main'
);
CREATE TABLE IF NOT EXISTS order_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  order_id TEXT NOT NULL REFERENCES orders(id),
  status TEXT NOT NULL,
  actor_user_id INTEGER,
  actor_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS payments(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  order_id TEXT NOT NULL REFERENCES orders(id),
  amount INTEGER NOT NULL CHECK (amount >= 0),
  method TEXT NOT NULL CHECK (method IN ('cash','card','digital')),
  status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('pending','confirmed','voided','refunded')),
  reference TEXT NOT NULL DEFAULT '',
  recorded_by INTEGER,
  recorded_by_name TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  status_changed_at INTEGER,
  status_changed_by TEXT
);
CREATE TABLE IF NOT EXISTS ingredients(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  unit TEXT NOT NULL DEFAULT 'g',
  stock_qty REAL NOT NULL DEFAULT 0,
  reorder_threshold REAL NOT NULL DEFAULT 0,
  supplier TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS recipes(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  menu_item_id INTEGER NOT NULL REFERENCES menu_items(id),
  ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),
  qty REAL NOT NULL CHECK (qty > 0),
  UNIQUE (menu_item_id, ingredient_id)
);
CREATE TABLE IF NOT EXISTS stock_movements(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),
  kind TEXT NOT NULL CHECK (kind IN ('purchase','usage','wastage','adjustment')),
  qty REAL NOT NULL,
  ref TEXT NOT NULL DEFAULT '',
  note TEXT NOT NULL DEFAULT '',
  actor_user_id INTEGER,
  created_at INTEGER NOT NULL
,
  unit_cost REAL
);
CREATE TABLE IF NOT EXISTS expenses(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  category TEXT NOT NULL,
  amount INTEGER NOT NULL CHECK (amount >= 0),
  spent_on TEXT NOT NULL,
  notes TEXT NOT NULL DEFAULT '',
  actor_user_id INTEGER,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS cash_counts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  amount INTEGER NOT NULL CHECK (amount >= 0),
  note TEXT NOT NULL DEFAULT '',
  actor_name TEXT NOT NULL DEFAULT '',
  counted_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_logs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  actor_user_id INTEGER,
  actor_name TEXT NOT NULL DEFAULT '',
  action TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  at INTEGER NOT NULL
);
`);
  return db;
}

// Schema migrations. v2: payments gains 'pending' status and status_changed_at.
// v3: payments.status_changed_by, menu_items.station, stock_movements.unit_cost, cash_counts.
export function migrate(db) {
  const v = db.prepare('PRAGMA user_version').get().user_version;
  if (v < 2) migrateV2(db);
  if (v < 3) migrateV3(db);
}
function migrateV2(db) {
  const row = db.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'payments'").get();
  if (row && !row.sql.includes('status_changed_at')) {
    db.exec(`BEGIN;
      CREATE TABLE payments_v2(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT NOT NULL REFERENCES orders(id),
        amount INTEGER NOT NULL CHECK (amount >= 0),
        method TEXT NOT NULL CHECK (method IN ('cash','card','digital')),
        status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('pending','confirmed','voided','refunded')),
        reference TEXT NOT NULL DEFAULT '',
        recorded_by INTEGER,
        recorded_by_name TEXT NOT NULL DEFAULT '',
        created_at INTEGER NOT NULL,
        status_changed_at INTEGER,
  status_changed_by TEXT
      );
      INSERT INTO payments_v2(id,order_id,amount,method,status,reference,recorded_by,recorded_by_name,created_at,status_changed_at)
        SELECT id,order_id,amount,method,status,reference,recorded_by,recorded_by_name,created_at,NULL FROM payments;
      DROP TABLE payments;
      ALTER TABLE payments_v2 RENAME TO payments;
      COMMIT;`);
  }
  db.exec('PRAGMA user_version = 2');
}
function migrateV3(db) {
  const cols = t => db.prepare(`PRAGMA table_info(${t})`).all().map(c => c.name);
  if (!cols('payments').includes('status_changed_by')) db.exec('ALTER TABLE payments ADD COLUMN status_changed_by TEXT');
  if (!cols('menu_items').includes('station')) db.exec("ALTER TABLE menu_items ADD COLUMN station TEXT NOT NULL DEFAULT 'main'");
  if (!cols('stock_movements').includes('unit_cost')) db.exec('ALTER TABLE stock_movements ADD COLUMN unit_cost REAL');
  db.exec(`CREATE TABLE IF NOT EXISTS cash_counts(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    amount INTEGER NOT NULL CHECK (amount >= 0),
    note TEXT NOT NULL DEFAULT '',
    actor_name TEXT NOT NULL DEFAULT '',
    counted_at INTEGER NOT NULL
  );`);
  db.exec('PRAGMA user_version = 3');
}
