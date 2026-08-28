// walsync Bun microbenchmark — same as Go/Rust
// Measures: file_read, read+gzip, read+proto+gzip, gzip_only, proto_only

import { readFileSync } from "fs";

const walPath = "/tmp/bench-wal.bin";

// Manual protobuf encoding (same as Go/Rust benchmarks)
function encodeWalChunk(offset, data) {
  const offsetBytes = encodeVarint(BigInt(offset));
  const lenBytes = encodeVarint(BigInt(data.length));
  const buf = new Uint8Array(1 + offsetBytes.length + 1 + lenBytes.length + data.length);
  let pos = 0;
  buf[pos++] = 0x08;
  buf.set(offsetBytes, pos); pos += offsetBytes.length;
  buf[pos++] = 0x12;
  buf.set(lenBytes, pos); pos += lenBytes.length;
  buf.set(data, pos);
  return buf;
}

function encodeVarint(v) {
  const bytes = [];
  while (v >= 0x80n) {
    bytes.push(Number(v & 0x7fn) | 0x80);
    v >>= 7n;
  }
  bytes.push(Number(v));
  return new Uint8Array(bytes);
}

const iterations = 1000;

// Warmup
const warmupData = readFileSync(walPath);
Bun.gzipSync(warmupData);

// Benchmark 1: File read
let start = performance.now();
for (let i = 0; i < iterations; i++) {
  const data = readFileSync(walPath);
}
let elapsed = performance.now() - start;
console.log(`BUN file_read:     ${iterations} iterations, ${elapsed.toFixed(1)}ms total, ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations*1.0/elapsed*1000).toFixed(1)} MB/s`);

// Benchmark 2: File read + gzip
start = performance.now();
for (let i = 0; i < iterations; i++) {
  const data = readFileSync(walPath);
  const compressed = Bun.gzipSync(data);
}
elapsed = performance.now() - start;
console.log(`BUN read+gzip:     ${iterations} iterations, ${elapsed.toFixed(1)}ms total, ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations*1.0/elapsed*1000).toFixed(1)} MB/s`);

// Benchmark 3: File read + protobuf encode + gzip (full hot path)
start = performance.now();
for (let i = 0; i < iterations; i++) {
  const data = readFileSync(walPath);
  const chunk = encodeWalChunk(0, data);
  const compressed = Bun.gzipSync(chunk);
}
elapsed = performance.now() - start;
console.log(`BUN read+proto+gz: ${iterations} iterations, ${elapsed.toFixed(1)}ms total, ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations*1.0/elapsed*1000).toFixed(1)} MB/s`);

// Benchmark 4: gzip only (data in memory)
const data = readFileSync(walPath);
start = performance.now();
for (let i = 0; i < iterations; i++) {
  const compressed = Bun.gzipSync(data);
}
elapsed = performance.now() - start;
console.log(`BUN gzip_only:     ${iterations} iterations, ${elapsed.toFixed(1)}ms total, ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations*1.0/elapsed*1000).toFixed(1)} MB/s`);

// Benchmark 5: protobuf encode only
start = performance.now();
for (let i = 0; i < iterations; i++) {
  const chunk = encodeWalChunk(0, data);
}
elapsed = performance.now() - start;
console.log(`BUN proto_only:    ${iterations} iterations, ${elapsed.toFixed(1)}ms total, ${(elapsed/iterations).toFixed(3)} ms/op, ${(iterations*1.0/elapsed*1000).toFixed(1)} MB/s`);

// Compression ratio
const compressed = Bun.gzipSync(data);
console.log(`\nCompression: ${data.length} → ${compressed.length} bytes (${(compressed.length/data.length*100).toFixed(1)}% of original)`);
