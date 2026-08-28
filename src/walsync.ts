/**
 * walsync — Live SQLite WAL shipping replication via HTTP
 * 
 * Bun rewrite. HTTP 7.5x faster than gRPC in Bun.
 * Single file, zero external dependencies (bun:sqlite not needed — file I/O only).
 * 
 * Usage:
 *   Primary: bun run walsync.ts --mode primary --db app.db --replicas host:9090
 *   Replica: bun run walsync.ts --mode replica --db app.db --listen :9090
 *   Config:  bun run walsync.ts --config walsync.conf
 */

import { parseArgs } from "util";
import { readFileSync, writeFileSync, existsSync, unlinkSync, renameSync, openSync, writeSync, closeSync, readSync, statSync } from "fs";

// ============================================================
// CLI + Config
// ============================================================

interface Config {
  mode: string;
  db: string;
  replicas: string;
  listen: string;
  metrics: string;
}

function parseArgsCLI(): { config: string | undefined; cli: Partial<Config> } {
  const { values } = parseArgs({
    args: Bun.argv,
    options: {
      mode: { type: "string" },
      db: { type: "string" },
      replicas: { type: "string" },
      listen: { type: "string", default: ":9090" },
      metrics: { type: "string" },
      config: { type: "string" },
    },
    strict: false,
    allowPositionals: true,
  });
  return {
    config: values.config as string | undefined,
    cli: {
      mode: values.mode as string | undefined,
      db: values.db as string | undefined,
      replicas: values.replicas as string | undefined,
      listen: values.listen as string | undefined,
      metrics: values.metrics as string | undefined,
    },
  };
}

function loadConfig(path: string): Config {
  const cfg: Config = { mode: "", db: "", replicas: "", listen: "", metrics: "" };
  const data = readFileSync(path, "utf-8");
  for (const line of data.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eq = trimmed.indexOf("=");
    if (eq < 0) continue;
    const key = trimmed.slice(0, eq).trim();
    let val = trimmed.slice(eq + 1).trim();
    if (val.length >= 2 && val.startsWith('"') && val.endsWith('"')) {
      val = val.slice(1, -1);
    }
    if (key in cfg) (cfg as Record<string, string>)[key] = val;
  }
  console.log(`config loaded from ${path}`);
  return cfg;
}

function mergeConfig(cli: Partial<Config>, cfg: Config): Config {
  return {
    mode: cli.mode ?? cfg.mode ?? "",
    db: cli.db ?? cfg.db ?? "",
    replicas: cli.replicas ?? cfg.replicas ?? "",
    listen: cli.listen ?? cfg.listen ?? ":9090",
    metrics: cli.metrics ?? cfg.metrics ?? "",
  };
}

// ============================================================
// Utilities
// ============================================================

const WAL_HEADER_SIZE = 32;

function fileSize(path: string): number {
  try { return statSync(path).size; } catch { return 0; }
}

function fileModTime(path: string): number {
  try { return statSync(path).mtimeMs; } catch { return 0; }
}

function walSalt(walPath: string): [number, number] {
  try {
    const f = openSync(walPath, "r");
    const header = new Uint8Array(32);
    const n = readSyncCompat(f, header, 0, 32, 0);
    closeSync(f);
    if (n < 24) return [0, 0];
    const salt1 = (header[16] << 24) | (header[17] << 16) | (header[18] << 8) | header[19];
    const salt2 = (header[20] << 24) | (header[21] << 16) | (header[22] << 8) | header[23];
    return [salt1 >>> 0, salt2 >>> 0];
  } catch {
    return [0, 0];
  }
}

// Bun's fs.readSync supports position parameter (same as Node.js)
function readSyncCompat(fd: number, buf: Uint8Array, offset: number, length: number, position: number): number {
  return readSync(fd, buf, offset, length, position);
}

function splitCSV(s: string): string[] {
  return s.split(",").map(s => s.trim()).filter(s => s.length > 0);
}

function ensurePort(addr: string): string {
  // Check if addr already has port
  const parts = addr.split(":");
  if (parts.length >= 2 && parts[parts.length - 1].match(/^\d+$/)) return addr;
  return `${addr}:9090`;
}

function ensureHTTP(addr: string): string {
  if (addr.startsWith("http://") || addr.startsWith("https://")) return addr;
  return `http://${addr}`;
}

function parseListen(addr: string): { hostname: string; port: number } {
  // ":9090" → 0.0.0.0:9090, "0.0.0.0:9090" → 0.0.0.0:9090, "host:9090" → host:9090
  if (addr.startsWith(":")) return { hostname: "0.0.0.0", port: parseInt(addr.slice(1), 10) };
  const idx = addr.lastIndexOf(":");
  if (idx > 0) return { hostname: addr.slice(0, idx), port: parseInt(addr.slice(idx + 1), 10) };
  return { hostname: "0.0.0.0", port: parseInt(addr, 10) || 9090 };
}

// ============================================================
// Metrics — Prometheus-compatible HTTP endpoint
// ============================================================

const metrics = {
  walShips: 0,
  walBytes: 0,
  walErrors: 0,
  snapshots: 0,
  snapshotBytes: 0,
  snapshotErrors: 0,
  lastShipUnix: 0,
};

function metricsIncWal(bytes: number) {
  metrics.walShips++;
  metrics.walBytes += bytes;
  metrics.lastShipUnix = Math.floor(Date.now() / 1000);
}

function metricsIncWalError() { metrics.walErrors++; }
function metricsIncSnapshot(bytes: number) {
  metrics.snapshots++;
  metrics.snapshotBytes += bytes;
  metrics.lastShipUnix = Math.floor(Date.now() / 1000);
}
function metricsIncSnapshotError() { metrics.snapshotErrors++; }

function renderMetrics(): string {
  return `# walsync metrics
walsync_wal_ships_total ${metrics.walShips}
walsync_wal_shipped_bytes_total ${metrics.walBytes}
walsync_wal_ship_errors_total ${metrics.walErrors}
walsync_snapshot_ships_total ${metrics.snapshots}
walsync_snapshot_shipped_bytes_total ${metrics.snapshotBytes}
walsync_snapshot_ship_errors_total ${metrics.snapshotErrors}
walsync_last_ship_timestamp_seconds ${metrics.lastShipUnix}
`;
}

function startMetricsServer(addr: string) {
  const { hostname, port } = parseListen(addr);
  const server = Bun.serve({
    hostname,
    port,
    fetch() {
      return new Response(renderMetrics(), {
        headers: { "Content-Type": "text/plain; version=0.0.4" },
      });
    },
  });
  console.log(`metrics server listening on ${addr}`);
  return server;
}

// ============================================================
// REPLICA MODE — HTTP server, receives WAL from primary
// ============================================================

function runReplica(dbPath: string, listen: string) {
  const walPath = `${dbPath}-wal`;
  const { hostname, port } = parseListen(listen);

  console.log(`walsync replica starting | db=${dbPath} | listen=${listen}`);

  const server = Bun.serve({
    hostname,
    port,
    async fetch(req) {
      const url = new URL(req.url);

      if (req.method === "POST" && url.pathname === "/ship-wal") {
        const offset = parseInt(req.headers.get("x-walsync-offset") || "0", 10);
        let body = await req.bytes();
        if (req.headers.get("content-encoding") === "gzip") {
          body = Bun.gunzipSync(body);
        }

        if (body.length === 0) {
          return Response.json({ ok: true });
        }

        try {
          const fd = openSync(walPath, existsSync(walPath) ? "r+" : "w");
          writeSync(fd, body, 0, body.length, offset);
          closeSync(fd);
          console.log(`WAL received: ${body.length} bytes at offset ${offset}`);
          return Response.json({ ok: true, applied_offset: offset + body.length });
        } catch (e) {
          console.error(`wal write error: ${e}`);
          return Response.json({ ok: false, error: String(e) });
        }
      }
      if (req.method === "POST" && url.pathname === "/ship-snapshot") {
        let body = await req.bytes();
        if (req.headers.get("content-encoding") === "gzip") {
          body = Bun.gunzipSync(body);
        }
        // Format: 8-byte big-endian db_len + db_data + wal_data
        const dv = new DataView(body.buffer, body.byteOffset, body.byteLength);
        const dbLen = Number(dv.getBigUint64(0));
        const dbData = body.subarray(8, 8 + dbLen);
        const walData = body.subarray(8 + dbLen);

        try {
          // Atomic replace
          writeFileSync(`${dbPath}.tmp`, dbData);
          if (existsSync(walPath)) { try { unlinkSync(walPath); } catch {} }
          try { unlinkSync(`${dbPath}-shm`); } catch {}
          renameSync(`${dbPath}.tmp`, dbPath);
          if (walData.length > 0) {
            writeFileSync(walPath, walData);
          }
          console.log(`snapshot received: ${dbData.length} bytes db, ${walData.length} bytes wal`);
          return Response.json({ ok: true });
        } catch (e) {
          console.error(`snapshot write error: ${e}`);
          return Response.json({ ok: false, error: String(e) });
        }
      }

      if (req.method === "GET" && url.pathname === "/health") {
        return Response.json({
          ok: true,
          db_size: fileSize(dbPath),
          wal_size: fileSize(walPath),
        });
      }

      return new Response("not found", { status: 404 });
    },
  });

  console.log(`replica listening on ${listen} (HTTP)`);
  return server;
}

// ============================================================
// PRIMARY MODE — HTTP client, ships WAL to replicas
// ============================================================

async function shipWALHTTP(
  walPath: string,
  offset: number,
  size: number,
  replicaAddrs: string[]
): Promise<void> {
  const dataLen = size - offset;
  if (dataLen <= 0) return;

  // Read WAL bytes from offset
  const fd = openSync(walPath, "r");
  const buf = new Uint8Array(dataLen);
  const n = readSyncCompat(fd, buf, 0, dataLen, offset);
  closeSync(fd);
  const data = buf.subarray(0, n);
  if (data.length === 0) return;

  // Compress with gzip
  const compressed = Bun.gzipSync(data);

  // Ship to all replicas concurrently
  const promises = replicaAddrs.map(async (addr) => {
    const url = `${ensureHTTP(addr)}/ship-wal`;
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "x-walsync-offset": String(offset),
          "Content-Type": "application/octet-stream",
          "Content-Encoding": "gzip",
        },
        body: compressed,
      });
      const ack = await resp.json();
      if (!ack.ok) {
        console.error(`wal ship to ${addr} failed: ${ack.error}`);
      }
    } catch (e) {
      console.error(`wal ship to ${addr} error: ${e}, retrying...`);
      // Retry once
      try {
        const resp = await fetch(url, {
          method: "POST",
          headers: {
            "x-walsync-offset": String(offset),
            "Content-Type": "application/octet-stream",
            "Content-Encoding": "gzip",
          },
          body: compressed,
        });
        const ack = await resp.json();
        if (!ack.ok) {
          console.error(`retry wal ship to ${addr} failed: ${ack.error}`);
          metricsIncWalError();
        }
      } catch (e2) {
        console.error(`retry wal ship to ${addr} failed: ${e2}`);
        metricsIncWalError();
      }
    }
  });

  await Promise.all(promises);
  console.log(`WAL shipped: ${data.length} bytes (compressed ${compressed.length}) from offset ${offset} to ${replicaAddrs.length} replicas`);
  metricsIncWal(data.length);
}

async function shipSnapshotHTTP(
  dbPath: string,
  walPath: string,
  replicaAddrs: string[]
): Promise<void> {
  const dbData = readFileSync(dbPath);
  const walData = existsSync(walPath) ? readFileSync(walPath) : new Uint8Array(0);

  // Pack: 8-byte db_len + db_data + wal_data
  const packed = new Uint8Array(8 + dbData.length + walData.length);
  const dv = new DataView(packed.buffer);
  dv.setBigUint64(0, BigInt(dbData.length));
  packed.set(dbData, 8);
  packed.set(walData, 8 + dbData.length);

  // Compress
  const compressed = Bun.gzipSync(packed);

  const promises = replicaAddrs.map(async (addr) => {
    const url = `${ensureHTTP(addr)}/ship-snapshot`;
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Encoding": "gzip",
        },
        body: compressed,
      });
      const ack = await resp.json();
      if (!ack.ok) {
        console.error(`snapshot to ${addr} failed: ${ack.error}`);
        metricsIncSnapshotError();
      } else {
        console.log(`snapshot shipped to ${addr} (${dbData.length} bytes db, ${walData.length} bytes wal)`);
      }
    } catch (e) {
      console.error(`snapshot to ${addr} error: ${e}, retrying...`);
      try {
        const resp = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/octet-stream",
            "Content-Encoding": "gzip",
          },
          body: compressed,
        });
        const ack = await resp.json();
        if (!ack.ok) {
          console.error(`retry snapshot to ${addr} failed: ${ack.error}`);
          metricsIncSnapshotError();
        }
      } catch (e2) {
        console.error(`retry snapshot to ${addr} failed: ${e2}`);
        metricsIncSnapshotError();
      }
    }
  });

  await Promise.all(promises);
  console.log(`snapshot shipped: ${dbData.length} bytes db, ${walData.length} bytes wal`);
  metricsIncSnapshot(dbData.length + walData.length);
}

async function runPrimary(dbPath: string, replicasCSV: string) {
  const replicaAddrs = splitCSV(replicasCSV).map(ensurePort);
  if (replicaAddrs.length === 0) {
    console.error("primary mode requires at least one replica");
    process.exit(1);
  }

  console.log(`walsync primary starting | db=${dbPath} | replicas=${replicaAddrs.join(", ")}`);
  const walPath = `${dbPath}-wal`;

  // Ship initial snapshot
  console.log("shipping initial snapshot...");
  await shipSnapshotHTTP(dbPath, walPath, replicaAddrs);
  console.log("initial snapshot shipped");

  // Track state
  let lastShippedSize = fileSize(walPath);
  let lastShippedDBMod = fileModTime(dbPath);
  let [lastSalt1, lastSalt2] = walSalt(walPath);

  // Polling loop — 50ms interval
  const POLL_MS = 50;
  let debounceActive = false;

  while (true) {
    await Bun.sleep(POLL_MS);

    const curWalSize = fileSize(walPath);

    // Schedule debounce if WAL grew
    if (curWalSize > lastShippedSize) {
      if (!debounceActive) {
        debounceActive = true;
        await Bun.sleep(POLL_MS); // 50ms debounce
      }
    }

    // Check for checkpoint (DB file modified)
    const curDBMod = fileModTime(dbPath);
    if (curDBMod !== lastShippedDBMod) {
      console.log("checkpoint detected, shipping snapshot...");
      await shipSnapshotHTTP(dbPath, walPath, replicaAddrs);
      lastShippedSize = fileSize(walPath);
      lastShippedDBMod = curDBMod;
      [lastSalt1, lastSalt2] = walSalt(walPath);
      debounceActive = false;
      continue;
    }

    // Check WAL salt change
    const [curSalt1, curSalt2] = walSalt(walPath);
    if (curSalt1 !== lastSalt1 || curSalt2 !== lastSalt2) {
      if (curSalt1 !== 0) {
        console.log("WAL salt changed, shipping snapshot...");
        await shipSnapshotHTTP(dbPath, walPath, replicaAddrs);
        lastShippedSize = fileSize(walPath);
        lastSalt1 = curSalt1;
        lastSalt2 = curSalt2;
        debounceActive = false;
        continue;
      }
    }

    // Fire debounce — ship WAL
    if (debounceActive) {
      const cur = fileSize(walPath);
      if (cur > lastShippedSize) {
        await shipWALHTTP(walPath, lastShippedSize, cur, replicaAddrs);
        lastShippedSize = cur;
      }
      debounceActive = false;
    }
  }
}

// ============================================================
// Main
// ============================================================

const { config: configPath, cli } = parseArgsCLI();
const cfg = configPath ? loadConfig(configPath) : { mode: "", db: "", replicas: "", listen: "", metrics: "" };
const merged = mergeConfig(cli, cfg);

if (!merged.mode) {
  console.error("error: --mode is required (primary or replica)");
  process.exit(1);
}
if (!merged.db) {
  console.error("error: --db is required");
  process.exit(1);
}

// Start metrics server if configured
if (merged.metrics) {
  startMetricsServer(merged.metrics);
}

if (merged.mode === "primary") {
  if (!merged.replicas) {
    console.error("error: primary mode requires --replicas");
    process.exit(1);
  }
  await runPrimary(merged.db, merged.replicas);
} else if (merged.mode === "replica") {
  runReplica(merged.db, merged.listen);
} else {
  console.error(`error: invalid mode '${merged.mode}', use primary or replica`);
  process.exit(1);
}
