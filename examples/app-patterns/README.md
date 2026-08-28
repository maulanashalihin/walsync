# App patterns example — Pattern 1: Role-based

Working example of single-writer + multi-read with walsync.

## Setup (local, 2 terminals)

```bash
npm install

# Terminal 1: start walsync replica (receives WAL)
./walsync -mode replica -db /tmp/walsync-app-replica.db -listen :9090 &

# Terminal 2: start walsync primary (ships WAL)
./walsync -mode primary -db /tmp/walsync-app-primary.db -replicas localhost:9090 &

# Terminal 3: start app as primary
WALSYNC_ROLE=primary DB_PATH=/tmp/walsync-app-primary.db node server.js &

# Terminal 4: start app as replica (different port)
WALSYNC_ROLE=replica PRIMARY_URL=http://localhost:3000 DB_PATH=/tmp/walsync-app-replica.db PORT=3001 node server.js &
```

## Test

```bash
# Write to replica app (port 3001) — proxies to primary (port 3000)
curl -X POST http://localhost:3001/api/users \
  -H 'Content-Type: application/json' \
  -d '{"name":"Alice","city":"Singapore"}'

# Read from replica app — reads local SQLite (data synced via walsync)
curl http://localhost:3001/api/users

# Read from primary app — reads local SQLite directly
curl http://localhost:3000/api/users

# Check health
curl http://localhost:3000/health  # {"role":"primary","ok":true}
curl http://localhost:3001/health  # {"role":"replica","ok":true}
```

## What happens

```
POST :3001/api/users
  → replica app receives
  → proxies to primary app (:3000)
  → primary writes to local SQLite
  → walsync primary ships WAL to replica
  → walsync replica applies WAL to replica SQLite

GET :3001/api/users
  → replica app reads local SQLite
  → data is there (within ~100ms sync delay)
```
