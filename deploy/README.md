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
```

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

## Firewall

Replica listens on port 9090 (HTTP). Restrict to primary IPs only:

```bash
# UFW (Ubuntu/Debian)
sudo ufw allow from PRIMARY_IP to any port 9090
sudo ufw deny 9090

# iptables
sudo iptables -A INPUT -p tcp --dport 9090 -s PRIMARY_IP -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9090 -j DROP
```

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
