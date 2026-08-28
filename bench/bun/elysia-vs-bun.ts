// Benchmark: Elysia vs raw Bun.serve vs Bun.serve minimal
// Same workload: POST 1MB bytes, return JSON ack

import { Elysia } from "elysia";
import { readFileSync } from "fs";

const WAL_PATH = "/tmp/bench-wal.bin";
const walData = readFileSync(WAL_PATH);
const iterations = 200;

console.log(`Payload: ${walData.length} bytes, ${iterations} iterations\n`);

// ============================================================
// 1. Raw Bun.serve — minimal
// ============================================================
console.log("=== Raw Bun.serve ===");
const rawServer = Bun.serve({
  port: 9201,
  async fetch(req) {
    if (req.method === "POST") {
      const body = await req.bytes();
      return Response.json({ ok: true, size: body.length });
    }
    return new Response("ok");
  },
});

await fetch("http://127.0.0.1:9201/", { method: "POST", body: walData });

let start = performance.now();
for (let i = 0; i < iterations; i++) {
  const resp = await fetch("http://127.0.0.1:9201/", { method: "POST", body: walData });
  await resp.json();
}
let elapsed = performance.now() - start;
console.log(`Raw Bun.serve: ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations*walData.length/elapsed/1024/1024).toFixed(1)} MB/s`);
rawServer.stop();

// ============================================================
// 2. Elysia — minimal
// ============================================================
console.log("\n=== Elysia (minimal) ===");
const elysiaApp = new Elysia()
  .post("/", async ({ request }) => {
    const data = await request.bytes();
    return { ok: true, size: data.length };
  })
  .listen(9202);

await fetch("http://127.0.0.1:9202/", { method: "POST", body: walData });

start = performance.now();
for (let i = 0; i < iterations; i++) {
  const resp = await fetch("http://127.0.0.1:9202/", { method: "POST", body: walData });
  await resp.json();
}
elapsed = performance.now() - start;
console.log(`Elysia minimal: ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations*walData.length/elapsed/1024/1024).toFixed(1)} MB/s`);
elysiaApp.stop();

// ============================================================
// 3. Elysia — with body schema (t.Object)
// ============================================================
console.log("\n=== Elysia (with schema validation) ===");
import { t } from "elysia";

const elysiaSchemaApp = new Elysia()
  .post(
    "/ship",
    async ({ body }) => {
      return { ok: true, size: (body as Uint8Array).length };
    },
    {
      body: t.File({ type: "application/octet-stream" }),
    }
  )
  .listen(9203);

await fetch("http://127.0.0.1:9203/ship", { method: "POST", body: walData });

start = performance.now();
for (let i = 0; i < iterations; i++) {
  const resp = await fetch("http://127.0.0.1:9203/ship", { method: "POST", body: walData });
  await resp.json();
}
elapsed = performance.now() - start;
console.log(`Elysia+schema: ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations*walData.length/elapsed/1024/1024).toFixed(1)} MB/s`);
elysiaSchemaApp.stop();

// ============================================================
// 4. Elysia — with gzip body handling
// ============================================================
console.log("\n=== Elysia (gzip body) ===");
const compressedData = Bun.gzipSync(walData);

const elysiaGzipApp = new Elysia()
  .post("/ship", async ({ request }) => {
    const enc = request.headers.get("content-encoding");
    let body = await request.bytes();
    if (enc === "gzip") {
      body = Bun.gunzipSync(body);
    }
    return { ok: true, size: body.length };
  })
  .listen(9204);

await fetch("http://127.0.0.1:9204/ship", {
  method: "POST",
  headers: { "content-encoding": "gzip" },
  body: compressedData,
});

start = performance.now();
for (let i = 0; i < iterations; i++) {
  const resp = await fetch("http://127.0.0.1:9204/ship", {
    method: "POST",
    headers: { "content-encoding": "gzip" },
    body: compressedData,
  });
  await resp.json();
}
elapsed = performance.now() - start;
console.log(`Elysia+gzip: ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations*walData.length/elapsed/1024/1024).toFixed(1)} MB/s (wire: ${compressedData.length} bytes)`);
elysiaGzipApp.stop();

// ============================================================
// 5. Raw Bun.serve — with gzip
// ============================================================
console.log("\n=== Raw Bun.serve (gzip) ===");
const rawGzipServer = Bun.serve({
  port: 9205,
  async fetch(req) {
    if (req.method === "POST") {
      let body = await req.bytes();
      if (req.headers.get("content-encoding") === "gzip") {
        body = Bun.gunzipSync(body);
      }
      return Response.json({ ok: true, size: body.length });
    }
    return new Response("ok");
  },
});

await fetch("http://127.0.0.1:9205/", {
  method: "POST",
  headers: { "content-encoding": "gzip" },
  body: compressedData,
});

start = performance.now();
for (let i = 0; i < iterations; i++) {
  const resp = await fetch("http://127.0.0.1:9205/", {
    method: "POST",
    headers: { "content-encoding": "gzip" },
    body: compressedData,
  });
  await resp.json();
}
elapsed = performance.now() - start;
console.log(`Raw+gzip: ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations*walData.length/elapsed/1024/1024).toFixed(1)} MB/s (wire: ${compressedData.length} bytes)`);
rawGzipServer.stop();

console.log("\n=== Done ===");
process.exit(0);
