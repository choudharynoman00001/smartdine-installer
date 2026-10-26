// Demo seed: 10 tables, 20 menu items, demo staff, demo stock. All data is
// synthetic and labelled demo - never use these credentials in production.
import crypto from 'node:crypto';

export const hashPin = (pin, salt) => {
  const s = salt || crypto.randomBytes(16).toString('hex');
  return 'scrypt$' + s + '$' + crypto.scryptSync(String(pin), Buffer.from(s, 'hex'), 32).toString('hex');
};
export const verifyPin = (pin, stored) => {
  const [alg, salt, hash] = String(stored).split('$');
  if (alg !== 'scrypt') return false;
  const h = crypto.scryptSync(String(pin), Buffer.from(salt, 'hex'), 32).toString('hex');
  return h.length === hash.length && crypto.timingSafeEqual(Buffer.from(h), Buffer.from(hash));
};

// structural seed: restaurant settings + tables only, no users, no credentials - safe for real installs
export function seedBase(db, now = Date.now()) {
  if (db.prepare('SELECT COUNT(*) n FROM restaurants').get().n) return false;
  db.prepare('INSERT INTO restaurants(id,name,currency,tax_percent,service_charge_percent,default_language,updated_at) VALUES(1,?,?,?,?,?,?)')
    .run('SmartDine Restaurant', 'PKR', 5, 0, 'en', now);
  const floors = [['Ground', 6], ['First', 4]]; let tn = 1;
  for (const [floor, count] of floors)
    for (let i = 0; i < count; i++, tn++)
      db.prepare('INSERT INTO restaurant_tables(label,floor,qr_token,status) VALUES(?,?,?,?)')
        .run('T' + tn, floor, crypto.randomBytes(16).toString('hex'), 'available');
  return true;
}

// demo seed: labelled sample data + well-known PINs, ONLY for explicit demo/test mode
export function seedDemo(db, now = Date.now()) {
  if (db.prepare('SELECT COUNT(*) n FROM restaurants').get().n === 0) seedBase(db, now);
  if (db.prepare('SELECT COUNT(*) n FROM users').get().n > 0) return false;
  db.prepare("UPDATE restaurants SET name = 'SmartDine Demo Restaurant' WHERE id = 1").run();
  const users = [
    ['Demo Owner', 'owner', '1111'], ['Demo Manager', 'manager', '2222'],
    ['Demo Cashier', 'cashier', '3333'], ['Demo Waiter', 'waiter', '4444'], ['Demo Chef', 'kitchen', '5555']];
  for (const [name, role, pin] of users)
    db.prepare('INSERT INTO users(name,role,pin_hash,active,created_at) VALUES(?,?,?,1,?)').run(name, role, hashPin(pin), now);
  const cats = [['Starters', 'اسٹارٹرز'], ['Mains', 'مینز'], ['Drinks', 'مشروبات'], ['Desserts', 'میٹھا']];
  const catIds = {};
  cats.forEach(([en, ur], i) => { catIds[en] = db.prepare('INSERT INTO categories(name_en,name_ur,sort) VALUES(?,?,?)').run(en, ur, i).lastInsertRowid; });
  const items = [
    ['Starters', 'Chicken Wings', 'چکن ونگز', 650], ['Starters', 'Samosa (2 pc)', 'سموسہ', 200],
    ['Starters', 'French Fries', 'فرینچ فرائز', 350], ['Starters', 'Chicken Tikka', 'چکن تکہ', 550],
    ['Starters', 'Soup of the Day', 'سوپ', 300],
    ['Mains', 'Chicken Biryani', 'چکن بریانی', 450], ['Mains', 'Beef Pulao', 'بیف پلاؤ', 550],
    ['Mains', 'Chicken Karahi (half)', 'چکن کڑاہی', 1200], ['Mains', 'Daal Fry', 'دال فرائی', 350],
    ['Mains', 'Vegetable Curry', 'سبزی سالن', 400], ['Mains', 'Chicken Burger', 'چکن برگر', 500],
    ['Mains', 'Zinger Burger', 'زنگر برگر', 650], ['Mains', 'Club Sandwich', 'کلب سینڈوچ', 450],
    ['Mains', 'Chicken Shawarma', 'چکن شاورما', 350], ['Mains', 'Naan', 'نان', 50],
    ['Drinks', 'Chai', 'چائے', 120], ['Drinks', 'Fresh Lime', 'فریش لیم', 200],
    ['Drinks', 'Mango Shake', 'مینگو شیک', 350], ['Drinks', 'Soft Drink', 'سافٹ ڈرنک', 100],
    ['Desserts', 'Kheer', 'کھیر', 250]];
  const itemIds = {};
  for (const [cat, en, ur, price] of items)
    itemIds[en] = db.prepare('INSERT INTO menu_items(category_id,name_en,name_ur,price,available) VALUES(?,?,?,?,1)').run(catIds[cat], en, ur, price).lastInsertRowid;
  const ings = [['Chicken', 'g', 5000, 1000, 'Demo Supplier'], ['Rice', 'g', 8000, 1500, 'Demo Supplier'],
    ['Flour', 'g', 10000, 2000, 'Demo Supplier'], ['Potato', 'g', 4000, 800, 'Demo Supplier'],
    ['Tea leaves', 'g', 500, 100, 'Demo Supplier'], ['Milk', 'ml', 3000, 600, 'Demo Supplier'],
    ['Mango', 'g', 1500, 300, 'Demo Supplier'], ['Sugar', 'g', 3000, 500, 'Demo Supplier'],
    ['Spices mix', 'g', 800, 150, 'Demo Supplier'], ['Oil', 'ml', 5000, 1000, 'Demo Supplier'],
    ['Bread bun', 'pc', 30, 10, 'Demo Supplier'], ['Lentils', 'g', 2000, 400, 'Demo Supplier']];
  const ingIds = {};
  for (const [name, unit, qty, thr, sup] of ings)
    ingIds[name] = db.prepare('INSERT INTO ingredients(name,unit,stock_qty,reorder_threshold,supplier) VALUES(?,?,?,?,?)').run(name, unit, qty, thr, sup).lastInsertRowid;
  const recipes = [
    ['Chicken Biryani', [['Chicken', 150], ['Rice', 200], ['Spices mix', 15], ['Oil', 30]]],
    ['Chicken Wings', [['Chicken', 250], ['Spices mix', 10], ['Oil', 40]]],
    ['French Fries', [['Potato', 200], ['Oil', 30]]],
    ['Chai', [['Tea leaves', 5], ['Milk', 100], ['Sugar', 15]]],
    ['Mango Shake', [['Mango', 150], ['Milk', 200], ['Sugar', 20]]],
    ['Chicken Burger', [['Chicken', 120], ['Bread bun', 1], ['Oil', 20]]],
    ['Daal Fry', [['Lentils', 150], ['Spices mix', 10], ['Oil', 25]]]];
  for (const [item, rs] of recipes)
    for (const [ing, qty] of rs)
      db.prepare('INSERT INTO recipes(menu_item_id,ingredient_id,qty) VALUES(?,?,?)').run(itemIds[item], ingIds[ing], qty);
  return true;
}
