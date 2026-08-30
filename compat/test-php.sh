#!/bin/bash
# test-php.sh — test PHP PDO SQLite replica read
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Test: PHP PDO SQLite (persistent readonly) ==="
bash "$SCRIPT_DIR/setup.sh"

php -r '
$dbPath = $argv[1];
$db = new PDO("sqlite:$dbPath", null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

$rows = $db->query("SELECT * FROM items ORDER BY id")->fetchAll(PDO::FETCH_ASSOC);
if (count($rows) < 2) { echo "FAIL: expected >= 2 rows, got " . count($rows) . "\n"; exit(1); }
if ($rows[0]["name"] !== "test-row-1" || $rows[0]["value"] != 42) { echo "FAIL: row 0 mismatch\n"; var_dump($rows[0]); exit(1); }
if ($rows[1]["name"] !== "test-row-2" || $rows[1]["value"] != 99) { echo "FAIL: row 1 mismatch\n"; var_dump($rows[1]); exit(1); }
echo "  read 1 OK: " . count($rows) . " rows\n";

$count = $db->query("SELECT COUNT(*) as c FROM items")->fetch(PDO::FETCH_ASSOC);
echo "  read 2 OK (persistent conn): count = " . $count["c"] . "\n";

$row = $db->query("SELECT * FROM items WHERE id = 1")->fetch(PDO::FETCH_ASSOC);
if (!$row || $row["name"] !== "test-row-1") { echo "FAIL: point read mismatch\n"; var_dump($row); exit(1); }
echo "  read 3 OK (point read): " . $row["name"] . "\n";

echo "PASS: PHP PDO SQLite — data correct, persistent connection works\n";
' "$SCRIPT_DIR/test-replica.db"

RESULT=$?
bash "$SCRIPT_DIR/cleanup.sh"
exit $RESULT
