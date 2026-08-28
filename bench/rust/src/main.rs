use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs;
use std::io::{Read, Write};
use std::time::Instant;

// WalChunk protobuf encoding (manual, equivalent to prost generated code)
// message WalChunk { int64 offset = 1; bytes data = 2; }
fn encode_wal_chunk(offset: i64, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(data.len() + 16);
    // field 1: offset (varint tag=0x08, varint value)
    buf.push(0x08);
    buf.extend_from_slice(&encode_varint(offset as u64));
    // field 2: data (varint tag=0x12, varint length, raw bytes)
    buf.push(0x12);
    buf.extend_from_slice(&encode_varint(data.len() as u64));
    buf.extend_from_slice(data);
    buf
}

fn encode_varint(mut v: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
    buf
}

fn gzip_compress(data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut encoder = GzEncoder::new(&mut buf, Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap();
    buf
}

fn main() {
    let wal_path = "/tmp/bench-wal.bin";

    // Warmup
    let warmup_data = fs::read(wal_path).unwrap();
    let _ = gzip_compress(&warmup_data);

    let iterations = 1000;

    // Benchmark 1: File read
    let start = Instant::now();
    for _ in 0..iterations {
        let _data = fs::read(wal_path).unwrap();
    }
    let elapsed = start.elapsed();
    println!(
        "RUST file_read:     {} iterations, {:?} total, {:.3} ms/op, {:.1} MB/s",
        iterations,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64,
        iterations as f64 / elapsed.as_secs_f64()
    );

    // Benchmark 2: File read + gzip compress
    let start = Instant::now();
    for _ in 0..iterations {
        let data = fs::read(wal_path).unwrap();
        let _compressed = gzip_compress(&data);
    }
    let elapsed = start.elapsed();
    println!(
        "RUST read+gzip:     {} iterations, {:?} total, {:.3} ms/op, {:.1} MB/s",
        iterations,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64,
        iterations as f64 / elapsed.as_secs_f64()
    );

    // Benchmark 3: File read + protobuf encode + gzip compress (full hot path)
    let start = Instant::now();
    for _ in 0..iterations {
        let data = fs::read(wal_path).unwrap();
        let chunk = encode_wal_chunk(0, &data);
        let _compressed = gzip_compress(&chunk);
    }
    let elapsed = start.elapsed();
    println!(
        "RUST read+proto+gz: {} iterations, {:?} total, {:.3} ms/op, {:.1} MB/s",
        iterations,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64,
        iterations as f64 / elapsed.as_secs_f64()
    );

    // Benchmark 4: gzip compress only (data already in memory)
    let data = fs::read(wal_path).unwrap();
    let start = Instant::now();
    for _ in 0..iterations {
        let _compressed = gzip_compress(&data);
    }
    let elapsed = start.elapsed();
    println!(
        "RUST gzip_only:     {} iterations, {:?} total, {:.3} ms/op, {:.1} MB/s",
        iterations,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64,
        iterations as f64 / elapsed.as_secs_f64()
    );

    // Benchmark 5: protobuf encode only
    let start = Instant::now();
    for _ in 0..iterations {
        let _chunk = encode_wal_chunk(0, &data);
    }
    let elapsed = start.elapsed();
    println!(
        "RUST proto_only:    {} iterations, {:?} total, {:.3} ms/op, {:.1} MB/s",
        iterations,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64,
        iterations as f64 / elapsed.as_secs_f64()
    );

    // Report compression ratio
    let compressed = gzip_compress(&data);
    println!(
        "\nCompression: {} → {} bytes ({:.1}% of original)",
        data.len(),
        compressed.len(),
        compressed.len() as f64 / data.len() as f64 * 100.0
    );
}
