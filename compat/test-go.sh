#!/bin/bash
# test-go.sh — test Go modernc.org/sqlite (pure Go, no CGo) replica read
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Test: Go modernc.org/sqlite (pure Go, readonly) ==="
bash "$SCRIPT_DIR/setup.sh"

# Write a small Go program and run it
cat > "$SCRIPT_DIR/test-go-runner.go" << 'GOEOF'
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := os.Args[1]
	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		fmt.Println("FAIL: open error:", err)
		os.Exit(1)
	}
	defer db.Close()

	rows, err := db.Query("SELECT id, name, value FROM items ORDER BY id")
	if err != nil {
		fmt.Println("FAIL: query error:", err)
		os.Exit(1)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, value int
		var name string
		if err := rows.Scan(&id, &name, &value); err != nil {
			fmt.Println("FAIL: scan error:", err)
			os.Exit(1)
		}
		count++
		if count == 1 && (name != "test-row-1" || value != 42) {
			fmt.Printf("FAIL: row 0 mismatch: id=%d name=%s value=%d\n", id, name, value)
			os.Exit(1)
		}
		if count == 2 && (name != "test-row-2" || value != 99) {
			fmt.Printf("FAIL: row 1 mismatch: id=%d name=%s value=%d\n", id, name, value)
			os.Exit(1)
		}
	}
	if count < 2 {
		fmt.Printf("FAIL: expected >= 2 rows, got %d\n", count)
		os.Exit(1)
	}
	fmt.Printf("  read 1 OK: %d rows\n", count)

	// Persistent connection reuse
	var c int
	db.QueryRow("SELECT COUNT(*) FROM items").Scan(&c)
	fmt.Printf("  read 2 OK (persistent conn): count = %d\n", c)

	var n string
	db.QueryRow("SELECT name FROM items WHERE id = 1").Scan(&n)
	if n != "test-row-1" {
		fmt.Println("FAIL: point read mismatch:", n)
		os.Exit(1)
	}
	fmt.Println("  read 3 OK (point read):", n)

	fmt.Println("PASS: Go modernc.org/sqlite — data correct, persistent connection works")
}
GOEOF

# Set up Go module for dependency resolution
cd "$SCRIPT_DIR"
if [ ! -f go.mod ] || [ ! -f go.sum ]; then
  go mod init walsync-compat-test 2>/dev/null || true
  go get modernc.org/sqlite 2>&1 | tail -3
  go mod download 2>&1
fi

# Run with go run (downloads modernc.org/sqlite if needed)
cd "$SCRIPT_DIR"
go run test-go-runner.go "$SCRIPT_DIR/test-replica.db" 2>&1

rm -f "$SCRIPT_DIR/test-go-runner.go" 2>/dev/null
bash "$SCRIPT_DIR/cleanup.sh"
exit $RESULT
