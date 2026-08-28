// Compare HTTP vs gRPC throughput in Bun for WAL shipping
// Same payload: 1MB WAL bytes, measure round-trip time

import { readFileSync } from "fs";

const WAL_PATH = "/tmp/bench-wal.bin";
const walData = readFileSync(WAL_PATH);
console.log(`WAL payload: ${walData.length} bytes`);

const iterations = 100;

// ============================================================
// Test 1: Bun.serve HTTP — POST raw bytes
// ============================================================
console.log("\n=== HTTP (Bun.serve) ===");

const httpServer = Bun.serve({
  port: 9193,
  async fetch(req) {
    if (req.method === "POST" && req.url.endsWith("/ship")) {
      const body = await req.arrayBuffer();
      // Write to /dev/null equivalent (just acknowledge)
      return new Response(JSON.stringify({ ok: true, size: body.byteLength }), {
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("ok");
  },
});

// Warmup
await fetch("http://127.0.0.1:9193/ship", { method: "POST", body: walData });

// Benchmark HTTP
let start = performance.now();
for (let i = 0; i < iterations; i++) {
  const resp = await fetch("http://127.0.0.1:9193/ship", { method: "POST", body: walData });
  const ack = await resp.json();
}
let elapsed = performance.now() - start;
console.log(`HTTP: ${iterations} iterations, ${elapsed.toFixed(1)}ms total, ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations * walData.length / elapsed / 1024 / 1024).toFixed(1)} MB/s`);

httpServer.stop();

// ============================================================
// Test 2: Bun.serve HTTP with gzip
// ============================================================
console.log("\n=== HTTP + gzip (Bun.gzipSync) ===");

const httpGzipServer = Bun.serve({
  port: 9194,
  async fetch(req) {
    if (req.method === "POST" && req.url.endsWith("/ship")) {
      const body = await req.arrayBuffer();
      return new Response(JSON.stringify({ ok: true, size: body.byteLength }), {
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("ok");
  },
});

const compressedData = Bun.gzipSync(walData);
await fetch("http://127.0.0.1:9194/ship", { method: "POST", body: compressedData });

start = performance.now();
for (let i = 0; i < iterations; i++) {
  const c = Bun.gzipSync(walData);
  const resp = await fetch("http://127.0.0.1:9194/ship", { method: "POST", body: c });
  const ack = await resp.json();
}
elapsed = performance.now() - start;
console.log(`HTTP+gzip: ${iterations} iterations, ${elapsed.toFixed(1)}ms total, ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations * walData.length / elapsed / 1024 / 1024).toFixed(1)} MB/s (pre-compress ${compressedData.length} bytes)`);

httpGzipServer.stop();

// ============================================================
// Test 3: gRPC via @grpc/grpc-js
// ============================================================
console.log("\n=== gRPC (@grpc/grpc-js) ===");

const grpc = (await import("@grpc/grpc-js")).default;
const protoLoader = (await import("@grpc/proto-loader")).default;

const PROTO_PATH = new URL("../../proto/walsync.proto", import.meta.url).pathname;
const pkgDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false, longs: String, enums: String, defaults: true, oneofs: true,
});
const proto = grpc.loadPackageDefinition(pkgDef).walsync;

const grpcServer = new grpc.Server();
grpcServer.addService(proto.WalSync.service, {
  ShipWal(call, callback) {
    callback(null, { ok: true, error: "", applied_offset: call.request.data?.length || 0 });
  },
  ShipSnapshot(call, callback) {
    callback(null, { ok: true, error: "", applied_offset: 0 });
  },
  Health(call, callback) {
    callback(null, { ok: true, db_size: 0, wal_size: 0 });
  },
});

await new Promise((resolve) => {
  grpcServer.bindAsync("127.0.0.1:9195", grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error("gRPC bind error:", err); process.exit(1); }
    resolve(port);
  });
});

const client = new proto.WalSync("127.0.0.1:9195", grpc.credentials.createInsecure());

// Wait for ready
await new Promise((resolve, reject) => {
  client.waitForReady(Date.now() + 5000, (err) => {
    if (err) reject(err); else resolve();
  });
});

// Warmup
await new Promise((resolve, reject) => {
  client.ShipWal({ offset: 0, data: walData }, (err, ack) => {
    if (err) reject(err); else resolve();
  });
});

// Benchmark gRPC
start = performance.now();
for (let i = 0; i < iterations; i++) {
  await new Promise((resolve, reject) => {
    client.ShipWal({ offset: 0, data: walData }, (err, ack) => {
      if (err) reject(err); else resolve();
    });
  });
}
elapsed = performance.now() - start;
console.log(`gRPC: ${iterations} iterations, ${elapsed.toFixed(1)}ms total, ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations * walData.length / elapsed / 1024 / 1024).toFixed(1)} MB/s`);

// ============================================================
// Test 4: HTTP with Connection: keep-alive (default in Bun fetch)
// Already tested above — Bun fetch uses keep-alive by default
// ============================================================

// ============================================================
// Summary
// ============================================================
console.log("\n=== Summary ===");
console.log(`Payload: ${walData.length} bytes (1MB)`);
console.log(`Iterations: ${iterations}`);
console.log("See above for per-protocol results");

client.close();
grpcServer.forceShutdown();
process.exit(0);
