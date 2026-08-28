// Pattern 1: Role-based single-writer + multi-read
// Uses better-sqlite3 (production-grade SQLite binding with proper WAL support).
//
// Run on both primary and replica nodes:
//   Primary:  WALSYNC_ROLE=primary DB_PATH=/data/app.db node server.js
//   Replica:  WALSYNC_ROLE=replica PRIMARY_URL=http://primary:3000 DB_PATH=/data/app.db
//
// Key insight for replica reads:
//   walsync writes WAL bytes directly to file (bypassing SQLite's -shm index).
//   SQLite only sees new WAL frames when opening a NEW connection (which rebuilds
//   the -shm). Persistent connections cache -shm in memory and miss external WAL
//   writes. Therefore: replica uses fresh readonly connection per read request.
//   readonly = no checkpoint on close = WAL preserved for next walsync incremental ship.
//   <1ms overhead per connection (SQLite is embedded, no network).

const express = require('express');
const Database = require('better-sqlite3');

const ROLE = process.env.WALSYNC_ROLE || 'primary';
const PRIMARY_URL = process.env.PRIMARY_URL || 'http://localhost:3000';
const DB_PATH = process.env.DB_PATH || '/tmp/walsync-app.db';

// Primary: persistent connection for writes (keeps WAL alive, no checkpoint)
let writeDb = null;
function getWriteDb() {
  if (!writeDb) {
    writeDb = new Database(DB_PATH);
    writeDb.pragma('journal_mode = WAL');
    writeDb.pragma('synchronous = NORMAL');
    writeDb.pragma('wal_autocheckpoint = 0');
    writeDb.exec(`CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY,
      name TEXT,
      city TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )`);
  }
  return writeDb;
}

// Replica: fresh readonly connection per read.
// - Opens new connection → SQLite rebuilds -shm from WAL → sees latest data
// - readonly = no checkpoint on close → WAL preserved for next walsync ship
// - <1ms overhead per connection (SQLite is embedded, no network)
function readQuery(sql, ...params) {
  const db = new Database(DB_PATH, { readonly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

function readOne(sql, ...params) {
  const db = new Database(DB_PATH, { readonly: true });
  try {
    return db.prepare(sql).get(...params);
  } finally {
    db.close();
  }
}

const app = express();
app.use(express.json());

// ── READ: every node handles locally ──
app.get('/api/users', (req, res) => {
  res.json(readQuery('SELECT * FROM users ORDER BY id DESC'));
});

app.get('/api/users/:id', (req, res) => {
  const user = readOne('SELECT * FROM users WHERE id = ?', Number(req.params.id));
  if (!user) return res.status(404).json({ error: 'not found' });
  res.json(user);
});

// ── WRITE: primary handles locally, replica proxies ──
app.post('/api/users', async (req, res) => {
  if (ROLE === 'primary') {
    const { name, city } = req.body;
    const r = getWriteDb().prepare('INSERT INTO users(name, city) VALUES(?, ?)').run(name, city);
    return res.json({ id: r.lastInsertRowid, name, city });
  }
  try {
    const r = await fetch(`${PRIMARY_URL}/api/users`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    res.status(r.status).json(await r.json());
  } catch (err) {
    res.status(503).json({ error: 'primary unavailable', detail: String(err) });
  }
});

app.put('/api/users/:id', async (req, res) => {
  if (ROLE === 'primary') {
    const { name, city } = req.body;
    getWriteDb().prepare('UPDATE users SET name = ?, city = ? WHERE id = ?').run(name, city, Number(req.params.id));
    return res.json({ id: Number(req.params.id), ...req.body });
  }
  try {
    const r = await fetch(`${PRIMARY_URL}/api/users/${req.params.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    res.status(r.status).json(await r.json());
  } catch (err) {
    res.status(503).json({ error: 'primary unavailable', detail: String(err) });
  }
});

app.delete('/api/users/:id', async (req, res) => {
  if (ROLE === 'primary') {
    getWriteDb().prepare('DELETE FROM users WHERE id = ?').run(Number(req.params.id));
    return res.json({ deleted: true, id: Number(req.params.id) });
  }
  try {
    const r = await fetch(`${PRIMARY_URL}/api/users/${req.params.id}`, { method: 'DELETE' });
    res.status(r.status).json(await r.json());
  } catch (err) {
    res.status(503).json({ error: 'primary unavailable', detail: String(err) });
  }
});

app.get('/health', (req, res) => {
  res.json({ role: ROLE, ok: true });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`[${ROLE}] listening on :${PORT}`));
