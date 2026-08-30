#!/bin/bash
# test-ruby.sh — test Ruby sqlite3 replica read
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Test: Ruby sqlite3 (persistent readonly) ==="
bash "$SCRIPT_DIR/setup.sh"

ruby -e '
require "sqlite3"
db_path = ARGV[0]
db = SQLite3::Database.new(db_path, readonly: true)
db.results_as_hash = true

rows = db.execute("SELECT * FROM items ORDER BY id")
if rows.length < 2 then puts "FAIL: expected >= 2 rows, got #{rows.length}"; exit 1 end
if rows[0]["name"] != "test-row-1" || rows[0]["value"] != 42 then puts "FAIL: row 0 mismatch #{rows[0]}"; exit 1 end
if rows[1]["name"] != "test-row-2" || rows[1]["value"] != 99 then puts "FAIL: row 1 mismatch #{rows[1]}"; exit 1 end
puts "  read 1 OK: #{rows.length} rows"

count = db.get_first_value("SELECT COUNT(*) FROM items")
puts "  read 2 OK (persistent conn): count = #{count}"

row = db.get_first_row("SELECT * FROM items WHERE id = 1")
if !row || row[1] != "test-row-1" then puts "FAIL: point read mismatch #{row}"; exit 1 end
puts "  read 3 OK (point read): #{row[1]}"

db.close
puts "PASS: Ruby sqlite3 — data correct, persistent connection works"
' "$SCRIPT_DIR/test-replica.db"

RESULT=$?
bash "$SCRIPT_DIR/cleanup.sh"
exit $RESULT
