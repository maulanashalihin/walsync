# walsync — production deployment

## Quick deploy (systemd)

### 1. Install binary

```bash
sudo mkdir -p /var/lib/walsync
sudo cp walsync-linux-amd64 /usr/local/bin/walsync
sudo chmod +x /usr/local/bin/walsync
```

### 2. Primary server

```bash
# Copy service file
sudo cp deploy/walsync-primary.service /etc/systemd/system/

# Edit: set your -db path and -replicas URLs
sudo nano /etc/systemd/system/walsync-primary.service

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable --now walsync-primary

# Check status
sudo systemctl status walsync-primary
sudo journalctl -u walsync-primary -f
```

### 3. Replica server

```bash
# Copy service file
sudo cp deploy/walsync-replica.service /etc/systemd/system/

# Edit: set your -db path and -listen port
sudo nano /etc/systemd/system/walsync-replica.service

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable --now walsync-replica

# Check status
sudo systemctl status walsync-replica
sudo journalctl -u walsync-replica -f

### 3.5. Firewall — REQUIRED before verify

walsync has no auth by design. **You MUST restrict replica port to primary IPs only.** An exposed replica port lets anyone write arbitrary WAL data to your database.

```bash
# UFW (Ubuntu/Debian) — replace PRIMARY_IP with your primary server's IP
sudo ufw allow from PRIMARY_IP to any port 9090
sudo ufw deny 9090
sudo ufw enable

# Verify: only PRIMARY_IP should have access
sudo ufw status numbered | grep 9090
```

```bash
# iptables (any Linux)
sudo iptables -A INPUT -p tcp --dport 9090 -s PRIMARY_IP -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9090 -j DROP
# Persist: sudo apt install iptables-persistent && sudo netfilter-persistent save
```

**Why firewall, not TLS/auth?** Firewall = kernel-level, zero app overhead, already in your OS. TLS = handshake + encrypt/decrypt per WAL ship. Token auth = per-request validation. For trusted server-to-server WAL shipping, firewall is the right tool.

### 4. Verify

```bash
# On replica: check health
curl http://localhost:9090/health

# On primary: write to DB
sqlite3 /var/lib/walsync/app.db "INSERT INTO users(name) VALUES('test');"

# On replica: check data arrived
sqlite3 /var/lib/walsync/app.db "SELECT * FROM users;"
```

## systemd features

The service files include:

- **Auto-restart** on failure (`Restart=on-failure`, 5s backoff)
- **Start on boot** (`WantedBy=multi-user.target`)
- **Network dependency** (`After=network.target`)
- **Security hardening**:
  - `NoNewPrivileges` — no sudo escalation
  - `ProtectSystem=strict` — filesystem read-only except data dir
  - `ProtectHome` — no access to /home
  - `PrivateTmp` — isolated /tmp
  - `ReadWritePaths` — only /var/lib/walsync writable
- **Journal logging** — `journalctl -u walsync-primary -f`

## Docker

```bash
# Build
docker build -t walsync .

# Run replica
docker run -d --name walsync-replica \
  -p 9090:9090 \
  -v /data/walsync:/var/lib/walsync \
  walsync -mode replica -db /var/lib/walsync/app.db -listen :9090

# Run primary
docker run -d --name walsync-primary \
  -v /data/walsync:/var/lib/walsync \
  walsync -mode primary -db /var/lib/walsync/app.db -replicas http://replica-host:9090
```

## Operations

### View logs

```bash
# Follow logs
sudo journalctl -u walsync-primary -f

# Last 100 lines
sudo journalctl -u walsync-primary -n 100

# Since boot
sudo journalctl -u walsync-primary -b
```

### Restart

```bash
sudo systemctl restart walsync-primary
```

### Stop / start

```bash
sudo systemctl stop walsync-primary
sudo systemctl start walsync-primary
```

### Update binary

```bash
sudo systemctl stop walsync-primary
sudo cp walsync-linux-amd64 /usr/local/bin/walsync
sudo systemctl start walsync-primary
```

## Manual failover

When primary goes down, promote a replica manually. No consensus needed — human judgment prevents split-brain.

### Step 1: Confirm primary is down

```bash
# On primary server
sudo systemctl status walsync-primary
# If failed/inactive, check if server itself is reachable
ssh primary-host "systemctl is-active walsync-primary"
```

### Step 2: Promote replica to primary

```bash
# On replica server
sudo systemctl stop walsync-replica

# Verify replica has latest data
sqlite3 /var/lib/walsync/app.db "SELECT COUNT(*) FROM your_table;"

# Start as primary (point to other replicas if any)
sudo walsync -mode primary \
  -db /var/lib/walsync/app.db \
  -replicas other-replica:9090

# Or edit systemd service to switch mode:
# sudo nano /etc/systemd/system/walsync-primary.service
# Change ExecStart to: /usr/local/bin/walsync -mode primary ...
# sudo systemctl daemon-reload
# sudo systemctl enable --now walsync-primary
```

### Step 3: Update app config

Point your app to the new primary:

```bash
# Update app config (environment variable, config file, or DNS)
DB_PRIMARY_HOST=new-primary-ip

# Or update DNS record to point to new primary
# dig app.example.com → should resolve to new primary IP
```

### Step 4: Old primary comes back — demote to replica

```bash
# On old primary server
sudo systemctl stop walsync-primary

# Discard stale data (it may have writes from before outage)
sudo rm /var/lib/walsync/app.db /var/lib/walsync/app.db-wal /var/lib/walsync/app.db-shm

# Start as replica — gets fresh snapshot from new primary
sudo walsync -mode replica \
  -db /var/lib/walsync/app.db \
  -listen :9090

# Or switch systemd service:
# sudo cp deploy/walsync-replica.service /etc/systemd/system/
# sudo systemctl daemon-reload
# sudo systemctl enable --now walsync-replica
```

### Why not automatic?

Automatic failover that's safe requires consensus (Raft/Paxos) to prevent split-brain:

```
Network partition (not a real crash):
  Primary A: still alive, still accepting writes
  Replica B: "primary is down!" → promotes itself
  → Two primaries, data diverges, corruption on merge
```

Consensus solves this with quorum voting (majority must agree to promote). But that's what rqlite, dqlite, and LiteFS do — they're consensus-based replication systems. walsync is WAL shipping, not consensus. Manual failover = human is the quorum.
