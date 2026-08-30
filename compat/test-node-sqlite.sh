#!/bin/bash
# test-node-sqlite.sh — test node:sqlite (built-in) replica read
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Test: node:sqlite (built-in, persistent readonly) ==="
bash "$SCRIPT_DIR/setup.sh"

node --no-warnings -e '
const { DatabaseSync } = require("node:sqlite");
const dbPath = process.argv[1];
const db = new DatabaseSync(dbPath, { readOnly: true });

const rows = db.prepare("SELECT * FROM items ORDER BY id").all();
if (rows.length < 2) { console.log("FAIL: expected >= 2 rows, got", rows.length); process.exit(1); }
if (rows[0].name !== "test-row-1" || rows[0].value !== 42) { console.log("FAIL: row 0 mismatch", rows[0]); process.exit(1); }
if (rows[1].name !== "test-row-2" || rows[1].value !== 99) { console.log("FAIL: row 1 mismatch", rows[1]); process.exit(1); }
console.log("  read 1 OK:", rows.length, "rows");

const count = db.prepare("SELECT COUNT(*) as c FROM items").get();
console.log("  read 2 OK (persistent conn): count =", count.c);

const row = db.prepare("SELECT * FROM items WHERE id = 1").get();
if (!row || row.name !== "test-row-1") { console.log("FAIL: point read mismatch", row); process.exit(1); }
console.log("  read 3 OK (point read):", row.name);

console.log("PASS: node:sqlite — data correct, persistent connection works");
' "$SCRIPT_DIR/test-replica.db"

RESULT=$?
bash "$SCRIPT_DIR/cleanup.sh"
exit $RESULT
