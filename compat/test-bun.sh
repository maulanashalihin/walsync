#!/bin/bash
# test-bun.sh — test bun:sqlite replica read (persistent readonly, no SIGBUS)
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Test: bun:sqlite (persistent readonly) ==="
bash "$SCRIPT_DIR/setup.sh"

bun -e '
const { Database } = require("bun:sqlite");
const dbPath = process.argv[1]; // Bun: argv[1] = first arg (no [eval] entry unlike Node)
const db = new Database(dbPath, { readonly: true });

// Read 1: basic query
const rows = db.prepare("SELECT * FROM items ORDER BY id").all();
if (rows.length < 2) { console.log("FAIL: expected >= 2 rows, got", rows.length); process.exit(1); }
if (rows[0].name !== "test-row-1" || rows[0].value !== 42) { console.log("FAIL: row 0 mismatch", rows[0]); process.exit(1); }
if (rows[1].name !== "test-row-2" || rows[1].value !== 99) { console.log("FAIL: row 1 mismatch", rows[1]); process.exit(1); }
console.log("  read 1 OK:", rows.length, "rows");

// Read 2: same persistent connection (test no SIGBUS on reused connection)
const rows2 = db.prepare("SELECT COUNT(*) as c FROM items").get();
if (rows2.c < 2) { console.log("FAIL: count mismatch", rows2); process.exit(1); }
console.log("  read 2 OK (persistent conn): count =", rows2.c);

// Read 3: point read
const row = db.prepare("SELECT * FROM items WHERE id = 1").get();
if (!row || row.name !== "test-row-1") { console.log("FAIL: point read mismatch", row); process.exit(1); }
console.log("  read 3 OK (point read):", row.name);

console.log("PASS: bun:sqlite — no SIGBUS, data correct, persistent connection works");
' "$SCRIPT_DIR/test-replica.db"

RESULT=$?
bash "$SCRIPT_DIR/cleanup.sh"
exit $RESULT
