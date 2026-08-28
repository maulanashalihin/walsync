// Test gRPC client in Bun — minimal walsync primary
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import { readFileSync, existsSync, statSync } from "fs";

const PROTO_PATH = new URL("../../proto/walsync.proto", import.meta.url).pathname;

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const walsyncProto = grpc.loadPackageDefinition(packageDefinition).walsync;

const dbPath = "/tmp/bun-primary.db";
const walPath = dbPath + "-wal";

const client = new walsyncProto.WalSync(
  "127.0.0.1:9192",
  grpc.credentials.createInsecure(),
);

// Ship initial snapshot
function shipSnapshot() {
  return new Promise((resolve, reject) => {
    const dbData = readFileSync(dbPath);
    let walData = Buffer.alloc(0);
    if (existsSync(walPath)) {
      walData = readFileSync(walPath);
    }
    client.ShipSnapshot(
      { dbData, walData },
      (err, ack) => {
        console.log("ShipSnapshot callback:", { err: err?.message || err, ackOk: ack?.ok, ackErr: ack?.error });
        if (err) { reject(err); return; }
        if (!ack || !ack.ok) { reject(new Error(String(ack?.error || err?.message || "unknown"))); return; }
        console.log(`snapshot shipped: ${dbData.length} bytes db, ${walData.length} bytes wal`);
        resolve();
      }
    );
  });
}

// Ship WAL incrementally
function shipWAL(offset, data) {
  return new Promise((resolve, reject) => {
    client.ShipWal(
      { offset, data: Buffer.from(data) },
      (err, ack) => {
        if (err) { reject(err); return; }
        if (!ack?.ok) { reject(new Error(ack?.error || "unknown error")); return; }
        console.log(`WAL shipped: ${data.length} bytes from offset ${offset}`);
        resolve();
      }
    );
  });
}

// Main
async function main() {
  // Wait for connection
  await new Promise((resolve) => {
    client.waitForReady(Date.now() + 5000, (err) => {
      if (err) {
        console.error("failed to connect:", err);
        process.exit(1);
      }
      resolve();
    });
  });

  console.log("connected to replica");

  // Ship snapshot
  await shipSnapshot();
  console.log("initial snapshot done");

  // Write 3 rows via SQLite
  const { Database } = await import("bun:sqlite");
  const db = new Database(dbPath);
  db.exec("PRAGMA journal_mode=WAL");
  db.exec("PRAGMA synchronous=NORMAL");
  db.exec("CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, name TEXT)");

  const startSize = existsSync(walPath) ? statSync(walPath).size : 0;

  for (let i = 0; i < 3; i++) {
    db.prepare("INSERT INTO items(name) VALUES(?)").run(`bun-item-${i}`);
  }
  console.log("wrote 3 rows to primary");

  // Wait for WAL to settle
  await new Promise((r) => setTimeout(r, 100));

  const endSize = existsSync(walPath) ? statSync(walPath).size : 0;
  if (endSize > startSize) {
    const walData = readFileSync(walPath);
    // Ship from startSize to endSize
    const chunk = walData.subarray(startSize, endSize);
    await shipWAL(startSize, chunk);
  }

  db.close();

  // Verify replica
  await new Promise((r) => setTimeout(r, 500));
  const { Database: RDB } = await import("bun:sqlite");
  const rdb = new RDB("/tmp/bun-replica.db");
  try {
    const rows = rdb.prepare("SELECT * FROM items ORDER BY id").all();
    console.log("replica rows:", JSON.stringify(rows));
  } catch (e) {
    console.log("replica error:", e.message);
  }
  rdb.close();

  client.close();
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
