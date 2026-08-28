# Contributing to walsync

Thanks for your interest in contributing! This is a young project — all contributions welcome.

## Getting started

```bash
git clone https://github.com/maulanashalihin/walsync.git
cd walsync
go build -o walsync .
```

## Development

### Project structure

```
walsync/
├── main.go          # All code (primary + replica + utilities)
├── examples/        # Sample writer/reader apps
├── go.mod           # Go module definition
└── README.md        # Project documentation
```

### Building

```bash
# Native build
go build -o walsync .

# Cross-compile for Linux
GOOS=linux GOARCH=amd64 go build -o walsync-linux-amd64 .
```

### Testing locally

```bash
# Terminal 1: start replica
./walsync -mode replica -db /tmp/replica.db -listen :9091

# Terminal 2: write to primary DB
node examples/writer-app.js /tmp/primary.db

# Terminal 3: start primary walsync
./walsync -mode primary -db /tmp/primary.db -replicas http://127.0.0.1:9091

# Terminal 4: read from replica
node examples/reader-app.js /tmp/replica.db
```

### Testing multi-server

Deploy `walsync-linux-amd64` to two servers:

```bash
# Server 2 (replica)
./walsync -mode replica -db /data/app.db -listen :9090

# Server 1 (primary)
./walsync -mode primary -db /data/app.db -replicas http://server2-ip:9090
```

## Pull request guidelines

1. **Keep changes focused** — one feature/fix per PR
2. **Test your changes** — verify sync works for INSERT, UPDATE, DELETE
3. **Update README** — if you add features or change behavior
4. **Follow existing style** — match the code style in `main.go`

## Roadmap areas

Areas where help is especially welcome:

- **WAL frame-level shipping** — parse WAL frames, ship only changed pages (avoid full snapshot on checkpoint)
- **TLS + auth** — secure the HTTP transport
- **Automatic failover** — health checks + replica promotion
- **gRPC transport** — lower overhead than HTTP for high-throughput
- **Multi-primary** — conflict resolution (LWW, CRDT, or Merkle tree sync)

## Reporting bugs

Include in your issue:
- OS and architecture
- Go version
- SQLite version (if using sqlite3 CLI)
- Steps to reproduce
- Expected vs actual behavior
- Logs from both primary and replica
