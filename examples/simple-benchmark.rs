use std::thread::sleep;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use bricksdb::kv::{KVOptions, KV};
use rand::seq::SliceRandom;
use rand::thread_rng;
use sha2::{Sha256, Digest};
use clap::Parser;

/// Benchmark 参数
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 操作次数
    #[arg(short = 'n', long = "n", default_value_t = 1_00_000)]
    n: u64,

    /// Value 长度
    #[arg(short = 'v', long = "value", default_value_t = 4096)]
    value_len: usize,
}

/// 生成固定长度的 key（32 字节，SHA256(i)）
fn make_key(i: u64) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(&i.to_le_bytes());
    hasher.finalize().to_vec()
}

/// 生成确定性的 value（根据 i 派生，长度由 value_len 决定）
fn make_value(i: u64, value_len: usize) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"VAL");
    hasher.update(&i.to_le_bytes());
    let base = hasher.finalize();

    if value_len <= base.len() {
        return base[..value_len].to_vec();
    }

    let mut v = Vec::with_capacity(value_len);
    while v.len() < value_len {
        v.extend_from_slice(&base);
    }
    v.truncate(value_len);
    v
}

/// Helper: 打印每次操作延迟（ms）
fn report(name: &str, n: u64, elapsed_ms: f64) {
    let per_op = elapsed_ms / n as f64;
    let ops_per_sec = n as f64 * 1000.0 / elapsed_ms;
    println!(
        "{:<12} {:>10.6} ms/op, {:>10.2} ops/s ({} op total, {:>10.2} ms total)",
        name,
        per_op,
        ops_per_sec,
        n,
        elapsed_ms
    );
}

/// 随机写入测试
fn bench_put_random(kv: &KV, n: u64, value_len: usize) {
    let mut indices: Vec<u64> = (0..n).collect();
    indices.shuffle(&mut thread_rng());

    let start = Instant::now();
    for i in indices {
        let key = make_key(i);
        let value = make_value(i, value_len);
        kv.put(key, value).unwrap();
    }
    let elapsed_ms = start.elapsed().as_millis();
    report("write random", n, elapsed_ms as f64);
}

/// 随机读取测试（并校验 value 内容）
fn bench_read_random(kv: &KV, n: u64, value_len: usize) {
    let mut indices: Vec<u64> = (0..n).collect();
    indices.shuffle(&mut thread_rng());

    let start = Instant::now();
    for i in indices {
        let key = make_key(i);
        let expected_value = make_value(i, value_len);
        let v = kv.get(&key).unwrap().unwrap();
        assert_eq!(v, expected_value, "Value mismatch at key {}", i);
    }
    let elapsed_ms = start.elapsed().as_millis();
    report("read random", n, elapsed_ms as f64);
}

fn main() {
    // 解析命令行参数
    let args = Args::parse();
    println!("Using n={} value_len={}", args.n, args.value_len);

    let dir = tempdir().unwrap();
    println!("Benchmarking in {}", dir.path().display());

    let mut kv_options = KVOptions::default();
    kv_options.wal_options.fsync = false;
    kv_options.value_store_options.small_page_cache_size = 16 * 1024 * 1024;

    let kv = KV::new(dir.path(), kv_options).unwrap();

    sleep(Duration::from_secs(3));

    bench_put_random(&kv, args.n, args.value_len);
    bench_read_random(&kv, args.n, args.value_len);
}