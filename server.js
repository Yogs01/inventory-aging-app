const express = require('express');
const multer  = require('multer');
const XLSX    = require('xlsx');
const crypto  = require('crypto');
const path    = require('path');
const fs      = require('fs');
const db      = require('./db');

const app  = express();
const PORT = process.env.PORT || 3002;

app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const upload = multer({
  dest: process.env.UPLOADS_PATH || path.join(__dirname, 'uploads'),
  limits: { fileSize: 200 * 1024 * 1024 }
});

// ─── helpers ─────────────────────────────────────────────────────────────────
function parseNum(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = parseFloat(String(v).replace(/[^0-9.\-]/g, ''));
  return isNaN(n) ? null : n;
}
function pn(v) { const n = parseNum(v); return n === null ? 0 : n; }

function parseDate(v) {
  if (!v) return null;
  if (typeof v === 'number') {
    const d = XLSX.SSF.parse_date_code(v);
    return `${d.y}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`;
  }
  const m = String(v).trim().match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : String(v).trim();
}

function hash(...p) { return crypto.createHash('md5').update(p.join('|')).digest('hex'); }
function fileHash(fp) { return crypto.createHash('md5').update(fs.readFileSync(fp)).digest('hex'); }

// ─── record builders ─────────────────────────────────────────────────────────
function buildAgingRecord(row) {
  const snap = parseDate(row['snapshot-date'] || row['Snapshot Date'] || '');
  const sku  = String(row['sku'] || row['SKU'] || '').trim();
  const asin = String(row['asin'] || row['ASIN'] || '').trim();
  return {
    snapshot_date:           snap || '',
    sku,
    fnsku:                   String(row['fnsku'] || row['FNSKU'] || '').trim(),
    asin,
    product_name:            String(row['product-name'] || row['Product Name'] || '').trim(),
    condition:               String(row['condition'] || row['Condition'] || '').trim(),
    available:               pn(row['afn-fulfillable-quantity'] || row['available'] || row['Available'] || 0),
    pending_removal:         pn(row['pending-removal-quantity'] || 0),
    age_0_90:                pn(row['inv-age-0-to-90-days']    || row['0-90 Days']   || 0),
    age_91_180:              pn(row['inv-age-91-to-180-days']   || row['91-180 Days']  || 0),
    age_181_270:             pn(row['inv-age-181-to-270-days']  || row['181-270 Days'] || 0),
    age_271_365:             pn(row['inv-age-271-to-365-days']  || row['271-365 Days'] || 0),
    age_365_455:             pn(row['inv-age-365-to-455-days']  || row['inv-age-366-to-455-days'] || row['365-455 Days'] || row['366-455 Days'] || 0),
    age_455_plus:            pn(row['inv-age-455-plus-days']    || row['inv-age-456-plus-days']   || row['455+ Days']   || row['456+ Days']  || 0),
    sold_t7:                 pn(row['units-shipped-t7']  || row['afn-sold-units-past-7-days']  || row['T7']  || 0),
    sold_t30:                pn(row['units-shipped-t30'] || row['afn-sold-units-past-30-days'] || row['T30'] || 0),
    sold_t60:                pn(row['units-shipped-t60'] || row['afn-sold-units-past-60-days'] || row['T60'] || 0),
    sold_t90:                pn(row['units-shipped-t90'] || row['afn-sold-units-past-90-days'] || row['T90'] || 0),
    sell_through:            parseNum(row['sell-through'] || row['Sell Through'] || null),
    recommended_action:      String(row['recommended-action'] || row['Recommended Action'] || '').trim(),
    recommended_removal_qty: pn(row['recommended-removal-quantity'] || row['Removal Qty'] || 0),
    unfulfillable_qty:       pn(row['unfulfillable-quantity'] || row['your-unfulfillable-quantity'] || row['Unfulfillable'] || 0),
    storage_type:            String(row['storage-type'] || row['Storage Type'] || '').trim(),
    your_price:              parseNum(row['your-price'] || row['Price'] || null),
    sales_rank:              parseNum(row['sales-rank'] || row['Sales Rank'] || null),
    estimated_storage_cost:  parseNum(row['estimated-storage-cost-next-month'] || row['estimated-storage-cost-per-unit'] || row['total-estimated-storage-cost'] || null),
    supplier:                String(row['supplier'] || row['Supplier'] || '').trim(),
    brand:                   String(row['brand'] || row['Brand'] || '').trim(),
    row_hash:                hash(snap || '', sku, asin),
  };
}

function buildUnfulfillableRecord(row) {
  const snap = parseDate(row['snapshot-date'] || row['Snapshot Date'] || '');
  const sku  = String(row['sku'] || row['SKU'] || '').trim();
  const asin = String(row['asin'] || row['ASIN'] || '').trim();
  let category = String(row['unfulfillable-category'] || row['Unfulfillable Category'] || row['disposition'] || '').trim();
  return {
    snapshot_date:          snap || '',
    sku,
    fnsku:                  String(row['fnsku'] || row['FNSKU'] || '').trim(),
    asin,
    product_name:           String(row['product-name'] || row['Product Name'] || '').trim(),
    condition:              String(row['condition'] || row['Condition'] || '').trim(),
    unfulfillable_category: category,
    quantity:               parseInt(row['quantity'] || row['Quantity'] || row['qty'] || 0) || 0,
    brand:                  String(row['brand'] || row['Brand'] || '').trim(),
    supplier:               String(row['supplier'] || row['Supplier'] || '').trim(),
    row_hash:               hash(snap || '', sku, asin, category),
  };
}

// ─── prepared statements ─────────────────────────────────────────────────────
const insertAging = db.prepare(`
  INSERT OR IGNORE INTO inventory_aging
  (snapshot_date,sku,fnsku,asin,product_name,condition,available,pending_removal,
   age_0_90,age_91_180,age_181_270,age_271_365,age_365_455,age_455_plus,
   sold_t7,sold_t30,sold_t60,sold_t90,sell_through,
   recommended_action,recommended_removal_qty,unfulfillable_qty,
   storage_type,your_price,sales_rank,estimated_storage_cost,
   supplier,brand,row_hash)
  VALUES
  (@snapshot_date,@sku,@fnsku,@asin,@product_name,@condition,@available,@pending_removal,
   @age_0_90,@age_91_180,@age_181_270,@age_271_365,@age_365_455,@age_455_plus,
   @sold_t7,@sold_t30,@sold_t60,@sold_t90,@sell_through,
   @recommended_action,@recommended_removal_qty,@unfulfillable_qty,
   @storage_type,@your_price,@sales_rank,@estimated_storage_cost,
   @supplier,@brand,@row_hash)
`);

const insertUnfulfillable = db.prepare(`
  INSERT OR IGNORE INTO inventory_unfulfillable
  (snapshot_date,sku,fnsku,asin,product_name,condition,
   unfulfillable_category,quantity,brand,supplier,row_hash)
  VALUES
  (@snapshot_date,@sku,@fnsku,@asin,@product_name,@condition,
   @unfulfillable_category,@quantity,@brand,@supplier,@row_hash)
`);

// ─── in-memory jobs ───────────────────────────────────────────────────────────
const jobs = {};

// ─── POST /api/upload ─────────────────────────────────────────────────────────
app.post('/api/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  const jobId = crypto.randomBytes(8).toString('hex');
  jobs[jobId] = { status: 'processing', progress: 0 };
  res.json({ success: true, jobId });

  setImmediate(() => {
    const job = jobs[jobId];
    try {
      // Support both CSV and Excel files
      const isCsv = req.file.originalname.toLowerCase().endsWith('.csv');
      const wb = isCsv
        ? XLSX.readFile(req.file.path, { type: 'file', raw: false })
        : XLSX.readFile(req.file.path, { cellDates: false });
      console.log(`[${jobId}] type=${isCsv?'csv':'xlsx'} sheets: ${wb.SheetNames.join(', ')}`);

      const readSheet = name => {
        const s = wb.Sheets[name] || null;
        if (!s) return [];
        const raw = XLSX.utils.sheet_to_json(s, { defval: '' });
        return raw.map(r => Object.fromEntries(Object.entries(r).map(([k,v]) => [k.trim(), v])));
      };

      // CSV = always first sheet; Excel = look for Raw Data sheet
      const rawName = isCsv ? wb.SheetNames[0] : (wb.SheetNames.find(n => /raw\s*data|raw/i.test(n)) || wb.SheetNames[0]);
      const unfulfName = isCsv ? null : wb.SheetNames.find(n => /unfulfill/i.test(n));

      const rawRows = readSheet(rawName);
      console.log(`[${jobId}] raw sheet="${rawName}" rows=${rawRows.length}`);
      if (rawRows[0]) console.log(`[${jobId}] cols:`, Object.keys(rawRows[0]).slice(0,12).join(', '));

      let agingAdded = 0, agingSkipped = 0, unfulfAdded = 0;

      // Detect snapshot date from first row and delete old data for that date
      // so re-uploading the same report always gives fresh data
      if (rawRows[0]) {
        const firstSnap = parseDate(rawRows[0]['snapshot-date'] || rawRows[0]['Snapshot Date'] || '');
        if (firstSnap) {
          const deleted = db.prepare('DELETE FROM inventory_aging WHERE snapshot_date = ?').run(firstSnap).changes;
          console.log(`[${jobId}] cleared ${deleted} old rows for snapshot ${firstSnap}`);
        }
      }

      db.transaction(rows => {
        for (const row of rows) {
          const r = insertAging.run(buildAgingRecord(row));
          if (r.changes > 0) agingAdded++; else agingSkipped++;
        }
      })(rawRows);
      job.progress = 80;

      if (unfulfName) {
        const unfRows = readSheet(unfulfName);
        console.log(`[${jobId}] unfulfilable sheet="${unfulfName}" rows=${unfRows.length}`);
        db.transaction(rows => {
          for (const row of rows) {
            const r = insertUnfulfillable.run(buildUnfulfillableRecord(row));
            if (r.changes > 0) unfulfAdded++;
          }
        })(unfRows);
      }

      // Log the upload
      try {
        const fh = fileHash(req.file.path);
        db.prepare('INSERT OR IGNORE INTO upload_log (filename,file_hash,rows_added,rows_skipped) VALUES (?,?,?,?)')
          .run(req.file.originalname, fh, agingAdded, agingSkipped);
      } catch(_) {}

      job.status = 'done';
      job.agingAdded   = agingAdded;
      job.agingSkipped = agingSkipped;
      job.unfulfAdded  = unfulfAdded;
      job.progress = 100;
      console.log(`[${jobId}] done — aging +${agingAdded}, unfulfilable +${unfulfAdded}`);
    } catch(e) {
      console.error(`[${jobId}] error:`, e.message);
      job.status = 'error'; job.error = e.message;
    } finally {
      try { fs.unlinkSync(req.file.path); } catch(_) {}
    }
  });
});

// ─── POST /api/import — batch import from seed script ────────────────────────
app.post('/api/import', (req, res) => {
  const rows = req.body.rows;
  if (!Array.isArray(rows)) return res.status(400).json({ error: 'rows required' });
  let added = 0, skipped = 0;
  db.transaction(rows => {
    for (const r of rows) {
      const x = insertAging.run(r);
      if (x.changes > 0) added++; else skipped++;
    }
  })(rows);
  res.json({ added, skipped });
});

// ─── GET /api/job/:id ─────────────────────────────────────────────────────────
app.get('/api/job/:id', (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'not found' });
  res.json(job);
});

// ─── GET /api/snapshots ───────────────────────────────────────────────────────
app.get('/api/snapshots', (req, res) => {
  const dates = db.prepare(`SELECT DISTINCT snapshot_date FROM inventory_aging WHERE snapshot_date != '' ORDER BY snapshot_date DESC`).all().map(r => r.snapshot_date);
  res.json(dates);
});

// ─── GET /api/stats?snapshot= ────────────────────────────────────────────────
app.get('/api/stats', (req, res) => {
  const snap = req.query.snapshot;
  const cond  = snap ? `snapshot_date = ?` : `snapshot_date = (SELECT MAX(snapshot_date) FROM inventory_aging)`;
  const params = snap ? [snap] : [];

  const totals = db.prepare(`
    SELECT
      COUNT(*) as sku_count,
      SUM(available) as total_units,
      SUM(age_0_90) as age_0_90,
      SUM(age_91_180) as age_91_180,
      SUM(age_181_270) as age_181_270,
      SUM(age_271_365) as age_271_365,
      SUM(age_365_455) as age_365_455,
      SUM(age_455_plus) as age_455_plus,
      SUM(unfulfillable_qty) as unfulfillable,
      SUM(recommended_removal_qty) as removal_qty,
      SUM(estimated_storage_cost) as total_storage_cost,
      SUM(pending_removal) as pending_removal
    FROM inventory_aging WHERE ${cond}
  `).get(...params);

  const byAction = db.prepare(`
    SELECT recommended_action, COUNT(*) as skus, SUM(available) as units, SUM(recommended_removal_qty) as removal_qty
    FROM inventory_aging WHERE ${cond} AND recommended_action != ''
    GROUP BY recommended_action ORDER BY skus DESC
  `).all(...params);

  const byBrand = db.prepare(`
    SELECT brand, COUNT(*) as skus, SUM(available) as units,
      SUM(age_0_90) as age_0_90, SUM(age_91_180) as age_91_180,
      SUM(age_181_270+age_271_365+age_365_455+age_455_plus) as aged_180_plus
    FROM inventory_aging WHERE ${cond} AND brand != ''
    GROUP BY brand ORDER BY units DESC LIMIT 20
  `).all(...params);

  const byStorage = db.prepare(`
    SELECT storage_type, COUNT(*) as skus, SUM(available) as units
    FROM inventory_aging WHERE ${cond} AND storage_type != ''
    GROUP BY storage_type ORDER BY units DESC
  `).all(...params);

  res.json({ totals, byAction, byBrand, byStorage });
});

// ─── GET /api/items ───────────────────────────────────────────────────────────
app.get('/api/items', (req, res) => {
  const snap    = req.query.snapshot || '';
  const page    = parseInt(req.query.page) || 1;
  const limit   = 50;
  const offset  = (page - 1) * limit;
  const search  = req.query.search || '';
  const brand   = req.query.brand || '';
  const action  = req.query.action || '';
  const storage = req.query.storage || '';
  const aged    = req.query.aged || '';

  const latestSnap = db.prepare(`SELECT MAX(snapshot_date) as d FROM inventory_aging`).get()?.d || '';
  const useSnap = snap || latestSnap;

  let where = 'WHERE snapshot_date = ?';
  const params = [useSnap];
  if (search)  { where += ' AND (product_name LIKE ? OR sku LIKE ? OR asin LIKE ?)'; params.push(`%${search}%`,`%${search}%`,`%${search}%`); }
  if (brand)   { where += ' AND brand = ?'; params.push(brand); }
  if (action)  { where += ' AND recommended_action = ?'; params.push(action); }
  if (storage) { where += ' AND storage_type = ?'; params.push(storage); }
  if (aged === '90')  { where += ' AND (age_91_180+age_181_270+age_271_365+age_365_455+age_455_plus) > 0'; }
  if (aged === '180') { where += ' AND (age_181_270+age_271_365+age_365_455+age_455_plus) > 0'; }
  if (aged === '365') { where += ' AND (age_365_455+age_455_plus) > 0'; }

  const total   = db.prepare(`SELECT COUNT(*) as n FROM inventory_aging ${where}`).get(...params).n;
  const records = db.prepare(`
    SELECT * FROM inventory_aging ${where}
    ORDER BY (age_181_270+age_271_365+age_365_455+age_455_plus) DESC, available DESC
    LIMIT ? OFFSET ?
  `).all(...params, limit, offset);

  res.json({ records, total, page, pages: Math.ceil(total / limit) });
});

// ─── GET /api/filters ─────────────────────────────────────────────────────────
app.get('/api/filters', (req, res) => {
  const latestSnap = db.prepare(`SELECT MAX(snapshot_date) as d FROM inventory_aging`).get()?.d || '';
  const brands   = db.prepare(`SELECT DISTINCT brand FROM inventory_aging WHERE brand != '' ORDER BY brand`).all().map(r => r.brand);
  const actions  = db.prepare(`SELECT DISTINCT recommended_action FROM inventory_aging WHERE recommended_action != '' ORDER BY recommended_action`).all().map(r => r.recommended_action);
  const storages = db.prepare(`SELECT DISTINCT storage_type FROM inventory_aging WHERE storage_type != '' ORDER BY storage_type`).all().map(r => r.storage_type);
  const uploads  = db.prepare(`SELECT * FROM upload_log ORDER BY uploaded_at DESC LIMIT 20`).all();
  res.json({ brands, actions, storages, latestSnapshot: latestSnap, uploads });
});

// ─── DELETE /api/reset ────────────────────────────────────────────────────────
app.delete('/api/reset', (req, res) => {
  db.prepare('DELETE FROM inventory_aging').run();
  db.prepare('DELETE FROM inventory_unfulfillable').run();
  db.prepare('DELETE FROM upload_log').run();
  res.json({ success: true });
});

// ─── start ────────────────────────────────────────────────────────────────────
const server = app.listen(PORT, () => {
  console.log(`\n✅ Inventory Aging App running at http://localhost:${PORT}\n`);
});
server.setTimeout(600000);
