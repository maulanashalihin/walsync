// Test gRPC server in Bun — minimal walsync replica
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import { writeFileSync, existsSync, unlinkSync, renameSync, openSync, writeSync, closeSync, statSync } from "fs";

const PROTO_PATH = new URL("../../proto/walsync.proto", import.meta.url).pathname;

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const walsyncProto = grpc.loadPackageDefinition(packageDefinition).walsync;

const dbPath = "/tmp/bun-replica.db";
const walPath = dbPath + "-wal";

const server = new grpc.Server();

server.addService(walsyncProto.WalSync.service, {
  ShipSnapshot(call, callback) {
    const snap = call.request;
    try {
      const dbData = snap.dbData instanceof Uint8Array ? snap.dbData : new Uint8Array(snap.dbData || []);
      const walData = snap.walData instanceof Uint8Array ? snap.walData : new Uint8Array(snap.walData || []);

      // Atomic replace
      writeFileSync(dbPath + ".tmp", dbData);
      if (existsSync(walPath)) { try { unlinkSync(walPath); } catch {} }
      try { unlinkSync(dbPath + "-shm"); } catch {}
      renameSync(dbPath + ".tmp", dbPath);
      if (walData.length > 0) {
        writeFileSync(walPath, walData);
      }
      console.log(`snapshot received: ${dbData.length} bytes db, ${walData.length} bytes wal`);
      callback(null, { ok: true, error: "", applied_offset: 0 });
    } catch (e) {
      console.error("snapshot error:", e);
      callback(null, { ok: false, error: String(e), applied_offset: 0 });
    }
  },

  ShipWal(call, callback) {
    const chunk = call.request;
    if (!chunk.data || chunk.data.length === 0) {
      callback(null, { ok: true, error: "", applied_offset: 0 });
      return;
    }
    try {
      const offset = Number(chunk.offset);
      const data = chunk.data instanceof Uint8Array ? chunk.data : new Uint8Array(chunk.data);
      const fd = openSync(walPath, existsSync(walPath) ? "r+" : "w");
      writeSync(fd, data, 0, data.length, offset);
      closeSync(fd);
      console.log(`WAL received: ${data.length} bytes at offset ${offset}`);
      callback(null, { ok: true, error: "", applied_offset: offset + data.length });
    } catch (e) {
      console.error("wal error:", e);
      callback(null, { ok: false, error: String(e), applied_offset: 0 });
    }
  },

  Health(call, callback) {
    const dbSize = existsSync(dbPath) ? statSync(dbPath).size : 0;
    const walSize = existsSync(walPath) ? statSync(walPath).size : 0;
    callback(null, { ok: true, db_size: dbSize, wal_size: walSize });
  },
});

server.bindAsync("127.0.0.1:9192", grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) {
    console.error("failed to bind:", err);
    process.exit(1);
  }
  console.log(`Bun gRPC replica listening on 127.0.0.1:${port}`);
});
