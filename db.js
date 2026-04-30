const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(process.env.DB_PATH || path.join(__dirname, 'inventory.db'));

db.exec(`
  CREATE TABLE IF NOT EXISTS inventory_aging (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT,
    sku TEXT,
    fnsku TEXT,
    asin TEXT,
    product_name TEXT,
    condition TEXT,
    available INTEGER DEFAULT 0,
    pending_removal INTEGER DEFAULT 0,
    age_0_90 INTEGER DEFAULT 0,
    age_91_180 INTEGER DEFAULT 0,
    age_181_270 INTEGER DEFAULT 0,
    age_271_365 INTEGER DEFAULT 0,
    age_365_455 INTEGER DEFAULT 0,
    age_455_plus INTEGER DEFAULT 0,
    sold_t7 REAL DEFAULT 0,
    sold_t30 REAL DEFAULT 0,
    sold_t60 REAL DEFAULT 0,
    sold_t90 REAL DEFAULT 0,
    sell_through REAL,
    recommended_action TEXT DEFAULT '',
    recommended_removal_qty INTEGER DEFAULT 0,
    unfulfillable_qty INTEGER DEFAULT 0,
    storage_type TEXT DEFAULT '',
    your_price REAL,
    sales_rank INTEGER,
    estimated_storage_cost REAL,
    supplier TEXT DEFAULT '',
    brand TEXT DEFAULT '',
    row_hash TEXT UNIQUE
  );

  CREATE TABLE IF NOT EXISTS inventory_unfulfillable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT,
    sku TEXT,
    fnsku TEXT,
    asin TEXT,
    product_name TEXT,
    condition TEXT,
    unfulfillable_category TEXT DEFAULT '',
    quantity INTEGER DEFAULT 0,
    brand TEXT DEFAULT '',
    supplier TEXT DEFAULT '',
    row_hash TEXT UNIQUE
  );

  CREATE TABLE IF NOT EXISTS upload_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT,
    file_hash TEXT UNIQUE,
    rows_added INTEGER,
    rows_skipped INTEGER,
    uploaded_at TEXT DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_aging_snapshot ON inventory_aging(snapshot_date);
  CREATE INDEX IF NOT EXISTS idx_aging_sku ON inventory_aging(sku);
  CREATE INDEX IF NOT EXISTS idx_aging_brand ON inventory_aging(brand);
  CREATE INDEX IF NOT EXISTS idx_aging_action ON inventory_aging(recommended_action);
  CREATE INDEX IF NOT EXISTS idx_aging_storage ON inventory_aging(storage_type);
`);

module.exports = db;
