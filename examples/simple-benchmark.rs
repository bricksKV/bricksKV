use std::thread::sleep;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use bricksdb::kv::{KVOptions, KV};
use rand::seq::SliceRandom;
use rand::thread_rng;

/// Generate a fixed-length key (32 bytes, zero-padded)
fn make_key(i: u64) -> Vec<u8> {
    let mut key = format!("key_{:012}", i).into_bytes();
    key.resize(32, 0);
    key
}

/// Helper: print per-op latency in milliseconds
fn report(name: &str, n: u64, elapsed_ms: f64) {
    let per_op = elapsed_ms / n as f64;                  // 每次操作耗时（ms）
    let ops_per_sec = n as f64 * 1000.0 / elapsed_ms;   // 每秒操作数
    println!(
        "{:<12} {:>10.6} ms/op, {:>10.2} ops ({} op total, {:>10.2} ms total)",
        name,
        per_op,
        ops_per_sec,
        n,
        elapsed_ms
    );
}

/// Random write benchmark
fn bench_put_random(kv: &KV, n: u64, value: &[u8]) {
    let mut indices: Vec<u64> = (0..n).collect();
    indices.shuffle(&mut thread_rng());

    let start = Instant::now();
    for i in indices {
        kv.put(make_key(i), value.to_vec()).unwrap();
    }
    let elapsed_ms = start.elapsed().as_millis();
    report("write random", n, elapsed_ms as f64);
}

/// Random read benchmark
fn bench_read_random(kv: &KV, n: u64, value: &[u8]) {
    let mut indices: Vec<u64> = (0..n).collect();
    indices.shuffle(&mut thread_rng());

    let start = Instant::now();
    for i in indices {
        let v = kv.get(&make_key(i)).unwrap().unwrap();
        assert_eq!(&v, &value);
    }
    let elapsed_ms = start.elapsed().as_millis();
    report("read random", n, elapsed_ms as f64);
}

fn main() {
    let dir = tempdir().unwrap();
    println!("Benchmarking in {}", dir.path().display());
    let mut kv_options = KVOptions::default();
    kv_options.wal_options.fsync = false; // disable fsync for faster benchmark
    let kv = KV::new(dir.path(), kv_options).unwrap();

    let n: u64 = 1000_000;
    let value = vec![7u8; 32]; // 32b value

    sleep(Duration::from_secs(10));

    // Benchmark: random write
    bench_put_random(&kv, n, &value);

    // Benchmark: random read
    bench_read_random(&kv, n, &value);
}