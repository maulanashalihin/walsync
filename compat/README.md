# walsync v1.1.0 Compatibility Tests

Tests that walsync replica reads work correctly across all popular SQLite libraries, and that corruption recovery (checkpoint, salt mismatch, CLI access) functions properly.

## Requirements

- `sqlite3` CLI (for setup + corruption tests)
- `bun` 1.4+ (for bun:sqlite test)
- `node` 18+ (for node:sqlite + better-sqlite3)
- `npm install better-sqlite3` (for better-sqlite3 test)
- `python3` (for Python sqlite3 test)
- `php-cli` + `php-sqlite3` (for PHP PDO test)
- `ruby` + `sqlite3` gem (for Ruby test)
- `go` 1.21+ (for Go modernc.org/sqlite test)

## Run all tests

```bash
bash compat/run-all.sh
```

## Run individual tests

```bash
bash compat/test-bun.sh
bash compat/test-better-sqlite3.sh
bash compat/test-node-sqlite.sh
bash compat/test-python.sh
bash compat/test-php.sh
bash compat/test-ruby.sh
bash compat/test-go.sh
bash compat/test-corruption.sh
```

## What each test does

### Library tests (test-*.sh)
1. Start walsync primary + replica on localhost (ports 19090/19091)
2. Write 2 rows to primary via sqlite3 CLI (WAL mode, autocheckpoint=0)
3. Wait for WAL sync to replica
4. Open **persistent readonly** connection with library X
5. Read all rows, verify data matches
6. Reuse same connection for 2 more reads (test no crash on persistent connection)
7. Cleanup

### Corruption tests (test-corruption.sh)
1. **Checkpoint recovery**: write data → force checkpoint on primary → write more → verify replica sees all rows
2. **Salt mismatch recovery**: truncate replica WAL → write to primary → verify replica recovers via snapshot
3. **CLI access recovery**: run `sqlite3` CLI (readwrite) on replica DB → write to primary → verify replica recovers

## What v1.1.0 fixes vs v1.0.0

| Bug | v1.0.0 | v1.1.0 |
|-----|--------|--------|
| SIGBUS in bun:sqlite | `os.WriteFile` truncates `-shm` → mmap invalid → crash | In-place `WriteAt` byte 0, preserves mmap |
| Silent corruption on checkpoint | No salt validation → WAL at stale offset | Salt header validation → reject → snapshot |
| CLI access on replica | Checkpoint truncates WAL → stuck forever | Salt mismatch detection → auto-snapshot recovery |

## Test Results (2026-08-30, macOS arm64, walsync v1.1.0)

```
RESULTS: 8 passed, 0 failed
ALL TESTS PASSED
```

| Test | Result | Notes |
|------|--------|-------|
| bun:sqlite | PASS | No SIGBUS, persistent readonly connection works |
| better-sqlite3 | PASS | Persistent readonly connection works |
| node:sqlite | PASS | Built-in `node:sqlite`, readonly mode |
| Python sqlite3 | PASS | `sqlite3.connect(uri, mode=ro)` |
| PHP PDO SQLite | PASS | `PDO::ATTR_ERRMODE` + readonly |
| Ruby sqlite3 | PASS | `SQLite3::Database.new(readonly: true)` |
| Go modernc.org/sqlite | PASS | Pure Go, no CGo, `?mode=ro` |
| Corruption: checkpoint | PASS | Primary checkpoint → snapshot → replica sees all rows |
| Corruption: salt mismatch | PASS | Truncate replica WAL → primary ships snapshot |
| Corruption: CLI access | PASS | `sqlite3` CLI on replica → checkpoint → auto-recovery |

### Findings

1. **No SIGBUS in any library** — v1.1.0's in-place `-shm` invalidation fix works across all 7 SQLite libraries. Persistent readonly connections are safe; no need for fresh-connection-per-read workaround that v1.0.0 required.
2. **No silent corruption** — salt validation + snapshot recovery handles all 3 corruption scenarios (checkpoint, salt mismatch, CLI access).
3. **WAL mode warm-up required** — after snapshot, replica DB lacks `-wal`/`-shm` files. Readonly connections fail with `SQLITE_CANTOPEN` until a readwrite connection creates them. Setup script handles this automatically.
4. **Bun/Node argv with `-e`** — both put first CLI arg at `process.argv[1]`. No `[eval]` pseudo-entry.
