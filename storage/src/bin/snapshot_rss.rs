use commonware_cryptography::Sha256;
use commonware_runtime::{tokio, Clock, Metrics as _, Runner as _};
use commonware_storage::{
    qmdb::any::{ordered, unordered, FixedConfig},
    translator::EightCap,
};
use commonware_utils::{sequence::FixedBytes, NZU64, NZUsize};
use std::{env, path::PathBuf, time::Duration};

///Usage: usr/bin/time -l cargo run -p commonware-storage --bin snapshot_rss --release --  --ordered  --n 150000000  --updates-per-iter 50000  --sleep-ms 0  --commit-every 1 --storage-dir ./tmp_data

// Tiny RNG (no deps)
#[derive(Clone)]
struct XorShift64 {
    s: u64,
}
impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { s: seed.max(1) }
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.s;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.s = x;
        x
    }
}

fn parse_arg_usize(args: &[String], name: &str, default: usize) -> usize {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_arg_u64(args: &[String], name: &str, default: u64) -> u64 {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn parse_arg_string(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn parse_arg_path(args: &[String], name: &str) -> Option<PathBuf> {
    parse_arg_string(args, name).map(Into::into)
}

fn fill_32(rng: &mut XorShift64) -> [u8; 32] {
    let mut out = [0u8; 32];
    for chunk in out.chunks_exact_mut(8) {
        chunk.copy_from_slice(&rng.next_u64().to_be_bytes());
    }
    out
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // Defaults chosen so it’s easy to scale up while keeping runtime overhead modest.
    let n = parse_arg_usize(&args, "--n", 1_000_000);
    let updates_per_iter = parse_arg_usize(&args, "--updates-per-iter", 50_000);
    let total_updates_limit = parse_arg_u64(&args, "--total-updates", 100 * 50_000);
    let sleep_ms = parse_arg_u64(&args, "--sleep-ms", 0);
    let commit_every = parse_arg_usize(&args, "--commit-every", 1);
    let do_sync = has_flag(&args, "--sync");
    let ordered_mode = has_flag(&args, "--ordered"); // default unordered if not set

    let page_size = parse_arg_usize(&args, "--page-size", 4096);
    let page_cache_pages = parse_arg_usize(&args, "--page-cache-pages", 64);

    let mmr_items_per_blob = parse_arg_u64(&args, "--mmr-items-per-blob", 128 * 1024);
    let log_items_per_blob = parse_arg_u64(&args, "--log-items-per-blob", 128 * 1024);
    let mmr_write_buffer = parse_arg_usize(&args, "--mmr-write-buffer", 1024 * 1024);
    let log_write_buffer = parse_arg_usize(&args, "--log-write-buffer", 1024 * 1024);

    let worker_threads = parse_arg_usize(&args, "--worker-threads", 2);
    let storage_dir = parse_arg_path(&args, "--storage-dir");

    let pid = std::process::id();
    let run_id = format!("snapshot_rss_{pid}");

    eprintln!(
        "pid={} mode={} n={} updates_per_iter={} total_updates={} commit_every={} sync={} sleep_ms={}",
        pid,
        if ordered_mode { "qmdb_any_ordered" } else { "qmdb_any_unordered" },
        n,
        updates_per_iter,
        if total_updates_limit == 0 { "inf".to_string() } else { total_updates_limit.to_string() },
        commit_every.max(1),
        do_sync,
        sleep_ms
    );

    // Use the tokio runtime context so the data path matches real QMDB usage
    // (MMR + journals + snapshot rebuild on init). Storage is file-backed under storage_dir.
    let mut rt_cfg = tokio::Config::default()
        .with_worker_threads(worker_threads)
        .with_maximum_buffer_size(2 * 1024 * 1024);
    if let Some(dir) = storage_dir {
        rt_cfg = rt_cfg.with_storage_directory(dir);
    }
    eprintln!("storage_dir={}", rt_cfg.storage_directory().display());

    tokio::Runner::new(rt_cfg).start(|context| async move {
        type K = FixedBytes<32>;
        type V = FixedBytes<32>;

        // Generate key deterministically from index (no storage needed).
        fn make_key(i: usize) -> K {
            let mut k = [0u8; 32];
            k[..8].copy_from_slice(&(i as u64).to_be_bytes());
            k[8..16].copy_from_slice(&(i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
            K::new(k)
        }

        let cfg = FixedConfig {
            mmr_journal_partition: format!("{run_id}_mmr_journal"),
            mmr_metadata_partition: format!("{run_id}_mmr_metadata"),
            mmr_items_per_blob: NZU64!(mmr_items_per_blob),
            mmr_write_buffer: NZUsize!(mmr_write_buffer),
            log_journal_partition: format!("{run_id}_log_journal"),
            log_items_per_blob: NZU64!(log_items_per_blob),
            log_write_buffer: NZUsize!(log_write_buffer),
            translator: EightCap,
            thread_pool: None,
            buffer_pool: commonware_runtime::buffer::PoolRef::new(
                NZUsize!(page_size),
                NZUsize!(page_cache_pages),
            ),
        };

        let mut rng = XorShift64::new(0xC0FFEE);

        if ordered_mode {
            type Db = ordered::fixed::Db<tokio::Context, K, V, Sha256, EightCap>;

            eprintln!("initializing qmdb (ordered) ...");
            let db = Db::init(context.with_label("qmdb"), cfg).await.expect("db init");
            let mut db = db.into_dirty();

            eprintln!("populating {} keys (create)...", n);
            let pop_start = std::time::Instant::now();
            for i in 0..n {
                let k = make_key(i);
                let v = V::new(fill_32(&mut rng));
                let created = db.create(k, v).await.expect("create");
                debug_assert!(created, "expected new key");

                if (i + 1) % 100_000 == 0 || i + 1 == n {
                    eprintln!(
                        "  progress: {}/{} keys ({:.1}%)",
                        i + 1,
                        n,
                        (i + 1) as f64 / n as f64 * 100.0
                    );
                }

                // Commit every 1,000,000 keys to release memory
                if (i + 1) % 1_000_000 == 0 && (i + 1) < n {
                    let mut committed = db.merkleize();
                    committed.commit(None).await.expect("commit");
                    if do_sync {
                        committed.sync().await.expect("sync");
                    }
                    db = committed.into_dirty();
                }
            }
            eprintln!("population took: {:?}", pop_start.elapsed());

            // Merkleize + commit once so we start from a clean, committed state.
            let mut db = db.merkleize();
            db.commit(None).await.expect("commit");
            if do_sync {
                db.sync().await.expect("sync");
            }

            eprintln!("built qmdb; entering steady-state updates (key count constant).");
            let mut iter: u64 = 0;
            let mut total_updates: u64 = 0;
            let start_time = std::time::Instant::now();
            loop {
                let iter_start = std::time::Instant::now();
                let mut dirty = db.into_dirty();
                for _ in 0..updates_per_iter {
                    let ki = (rng.next_u64() as usize) % n;
                    let k = make_key(ki);
                    let v = V::new(fill_32(&mut rng));
                    dirty.update(k, v).await.expect("update");
                }

                db = dirty.merkleize();
                if commit_every.max(1) == 1 || (iter as usize + 1) % commit_every.max(1) == 0 {
                    db.commit(None).await.expect("commit");
                    if do_sync {
                        db.sync().await.expect("sync");
                    }
                }

                iter += 1;
                total_updates += updates_per_iter as u64;
                let iter_elapsed = iter_start.elapsed();
                let total_elapsed = start_time.elapsed();
                let tps = updates_per_iter as f64 / iter_elapsed.as_secs_f64();
                let avg_tps = total_updates as f64 / total_elapsed.as_secs_f64();

                eprintln!(
                    "iter={} | total_upd={} | tps={:.2} | avg_tps={:.2} | elapsed={:?}",
                    iter, total_updates, tps, avg_tps, total_elapsed
                );

                if total_updates_limit > 0 && total_updates >= total_updates_limit {
                    eprintln!("reached total updates limit: {}", total_updates_limit);
                    break;
                }

                context.sleep(Duration::from_millis(sleep_ms)).await;
            }
        } else {
            type Db = unordered::fixed::Db<tokio::Context, K, V, Sha256, EightCap>;

            eprintln!("initializing qmdb (unordered) ...");
            let db = Db::init(context.with_label("qmdb"), cfg).await.expect("db init");
            let mut db = db.into_dirty();

            eprintln!("populating {} keys (create)...", n);
            let pop_start = std::time::Instant::now();
            for i in 0..n {
                let k = make_key(i);
                let v = V::new(fill_32(&mut rng));
                let created = db.create(k, v).await.expect("create");
                debug_assert!(created, "expected new key");

                if (i + 1) % 100_000 == 0 || i + 1 == n {
                    eprintln!(
                        "  progress: {}/{} keys ({:.1}%)",
                        i + 1,
                        n,
                        (i + 1) as f64 / n as f64 * 100.0
                    );
                }

                // Commit every 1,000,000 keys to release memory
                if (i + 1) % 1_000_000 == 0 && (i + 1) < n {
                    let mut committed = db.merkleize();
                    committed.commit(None).await.expect("commit");
                    if do_sync {
                        committed.sync().await.expect("sync");
                    }
                    db = committed.into_dirty();
                }
            }
            eprintln!("population took: {:?}", pop_start.elapsed());

            // Merkleize + commit once so we start from a clean, committed state.
            let mut db = db.merkleize();
            db.commit(None).await.expect("commit");
            if do_sync {
                db.sync().await.expect("sync");
            }

            eprintln!("built qmdb; entering steady-state updates (key count constant).");
            let mut iter: u64 = 0;
            let mut total_updates: u64 = 0;
            let start_time = std::time::Instant::now();
            loop {
                let iter_start = std::time::Instant::now();
                let mut dirty = db.into_dirty();
                for _ in 0..updates_per_iter {
                    let ki = (rng.next_u64() as usize) % n;
                    let k = make_key(ki);
                    let v = V::new(fill_32(&mut rng));
                    dirty.update(k, v).await.expect("update");
                }

                db = dirty.merkleize();
                if commit_every.max(1) == 1 || (iter as usize + 1) % commit_every.max(1) == 0 {
                    db.commit(None).await.expect("commit");
                    if do_sync {
                        db.sync().await.expect("sync");
                    }
                }

                iter += 1;
                total_updates += updates_per_iter as u64;
                let iter_elapsed = iter_start.elapsed();
                let total_elapsed = start_time.elapsed();
                let tps = updates_per_iter as f64 / iter_elapsed.as_secs_f64();
                let avg_tps = total_updates as f64 / total_elapsed.as_secs_f64();

                eprintln!(
                    "iter={} | total_upd={} | tps={:.2} | avg_tps={:.2} | elapsed={:?}",
                    iter, total_updates, tps, avg_tps, total_elapsed
                );

                if total_updates_limit > 0 && total_updates >= total_updates_limit {
                    eprintln!("reached total updates limit: {}", total_updates_limit);
                    break;
                }

                context.sleep(Duration::from_millis(sleep_ms)).await;
            }
        }
    });
}
