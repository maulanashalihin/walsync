// Persistent writer app: keeps DB connection open, writes multiple rows with delays
const { DatabaseSync } = require('node:sqlite');

const dbPath = process.argv[2] || '/tmp/walsync-test.db';
const db = new DatabaseSync(dbPath);

db.exec('PRAGMA journal_mode = WAL');
db.exec('PRAGMA synchronous = NORMAL');
db.exec('PRAGMA wal_autocheckpoint = 0'); // disable auto-checkpoint

db.exec(`CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  name TEXT,
  city TEXT,
  created_at TEXT DEFAULT (datetime('now'))
)`);

const users = [
  ['Maulana', 'Singapore'],
  ['Budi', 'Jakarta'],
  ['Siti', 'Bandung'],
  ['Andi', 'Surabaya'],
  ['Dewi', 'Medan'],
];

const stmt = db.prepare('INSERT INTO users (name, city) VALUES (?, ?)');

let i = 0;
const interval = setInterval(() => {
  if (i >= users.length) {
    console.log(`[WRITER] Done. ${users.length} rows written. Keeping DB open for 10s...`);
    // Keep DB open so WAL is not checkpointed
    setTimeout(() => {
      const rows = db.prepare('SELECT COUNT(*) as count FROM users').get();
      console.log(`[WRITER] Final count: ${rows.count}`);
      db.close();
      process.exit(0);
    }, 10000);
    clearInterval(interval);
    return;
  }
  const [name, city] = users[i];
  const result = stmt.run(name, city);
  console.log(`[WRITER] Inserted: id=${result.lastInsertRowid}, name=${name}, city=${city}`);
  i++;
}, 1000); // write every 1 second
