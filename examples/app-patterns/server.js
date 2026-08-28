// Pattern 1: Role-based single-writer + multi-read
// Uses better-sqlite3 (mature, proper WAL support).
// Run on both primary and replica nodes.
//   Primary:  WALSYNC_ROLE=primary DB_PATH=/data/app.db node server.js
//   Replica:  WALSYNC_ROLE=replica PRIMARY_URL=http://primary:3000 DB_PATH=/data/app.js
const express = require('express');
const Database = require('better-sqlite3');

const ROLE = process.env.WALSYNC_ROLE || 'primary';
const PRIMARY_URL = process.env.PRIMARY_URL || 'http://localhost:3000';
const DB_PATH = process.env.DB_PATH || '/tmp/walsync-app.db';

// Persistent connection — stays open for process lifetime.
// Primary: uses this for writes.
// Replica: prevents last-connection WAL checkpoint (SQLite checkpoints when
//   the last connection closes). Without this, fresh read connections would
//   checkpoint the WAL on close, truncating it and breaking walsync's
//   incremental WAL ships.
const persist = new Database(DB_PATH);
persist.pragma('journal_mode = WAL');
persist.pragma('synchronous = NORMAL');
persist.pragma('wal_autocheckpoint = 0');
persist.exec(`CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  name TEXT,
  city TEXT,
  created_at TEXT DEFAULT (datetime('now'))
)`);

// Fresh connection per read — sees latest WAL from walsync.
// <1ms overhead. persist connection prevents checkpoint on close.
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
    const r = persist.prepare('INSERT INTO users(name, city) VALUES(?, ?)').run(name, city);
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
    persist.prepare('UPDATE users SET name = ?, city = ? WHERE id = ?').run(name, city, Number(req.params.id));
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
    persist.prepare('DELETE FROM users WHERE id = ?').run(Number(req.params.id));
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
