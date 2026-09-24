use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use neo4j::address::Address;
use neo4j::driver::auth::AuthToken;
use neo4j::driver::{ConnectionConfig, Driver, DriverConfig, RoutingControl};
use neo4j::value_map;

// State must never be 0 — xorshift loops at 0 forever.
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[derive(Debug, Clone)]
enum Mode {
    Point,
    Heavy,
    Mix { write_pct: u8, write_batch: u32 },
    Split { write_batch: u32 },
}

#[derive(Debug, Clone)]
enum Target {
    Direct,
    Routing,
}

struct ThreadResult {
    reads: u64,
    writes: u64,
    served: HashMap<String, u64>,
}

fn worker(driver: Arc<Driver>, duration: Duration, mode: Mode, thread_idx: u64, is_reader: bool) -> ThreadResult {
    let db = Arc::new(String::from("memgraph"));
    let deadline = Instant::now() + duration;
    let mut reads: u64 = 0;
    let mut writes: u64 = 0;
    let mut served: HashMap<String, u64> = HashMap::new();

    // Knuth multiplicative hash constant keeps seeds distinct per thread.
    // `.max(1)` guarantees the xorshift state is never 0.
    let now_ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let mut rng = now_ns
        .wrapping_add(thread_idx.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407))
        .max(1);

    while Instant::now() < deadline {
        let (result, is_write) = match &mode {
            Mode::Point => {
                let id = (xorshift64(&mut rng) % 500000) as i64;
                let r = driver
                    .execute_query("MATCH (n:Bench {id: $id}) RETURN n")
                    .with_database(Arc::clone(&db))
                    .with_routing_control(RoutingControl::Read)
                    .with_parameters(value_map!({"id": id}))
                    .run();
                (r, false)
            }
            Mode::Heavy => {
                let lo = (xorshift64(&mut rng) % 492001) as i64;
                let hi = lo + 8000;
                let r = driver
                    .execute_query(
                        "MATCH (n:Bench) WHERE n.id >= $lo AND n.id < $hi RETURN sum(n.x)",
                    )
                    .with_database(Arc::clone(&db))
                    .with_routing_control(RoutingControl::Read)
                    .with_parameters(value_map!({"lo": lo, "hi": hi}))
                    .run();
                (r, false)
            }
            Mode::Mix { write_pct, write_batch } => {
                if (xorshift64(&mut rng) % 100) < u64::from(*write_pct) {
                    let r = if *write_batch <= 1 {
                        let id = (xorshift64(&mut rng) % 1_000_000_000) as i64;
                        driver
                            .execute_query("CREATE (:WNode {id: $id})")
                            .with_database(Arc::clone(&db))
                            .with_routing_control(RoutingControl::Write)
                            .with_parameters(value_map!({"id": id}))
                            .run()
                    } else {
                        let k = *write_batch as i64;
                        driver
                            .execute_query("UNWIND range(1, $k) AS i CREATE (:WNode {id: i})")
                            .with_database(Arc::clone(&db))
                            .with_routing_control(RoutingControl::Write)
                            .with_parameters(value_map!({"k": k}))
                            .run()
                    };
                    (r, true)
                } else {
                    let lo = (xorshift64(&mut rng) % 492001) as i64;
                    let hi = lo + 8000;
                    let r = driver
                        .execute_query(
                            "MATCH (n:Bench) WHERE n.id >= $lo AND n.id < $hi RETURN sum(n.x)",
                        )
                        .with_database(Arc::clone(&db))
                        .with_routing_control(RoutingControl::Read)
                        .with_parameters(value_map!({"lo": lo, "hi": hi}))
                        .run();
                    (r, false)
                }
            }
            Mode::Split { write_batch } => {
                if is_reader {
                    let lo = (xorshift64(&mut rng) % 492001) as i64;
                    let hi = lo + 8000;
                    let r = driver
                        .execute_query(
                            "MATCH (n:Bench) WHERE n.id >= $lo AND n.id < $hi RETURN sum(n.x)",
                        )
                        .with_database(Arc::clone(&db))
                        .with_routing_control(RoutingControl::Read)
                        .with_parameters(value_map!({"lo": lo, "hi": hi}))
                        .run();
                    (r, false)
                } else {
                    let r = if *write_batch <= 1 {
                        let id = (xorshift64(&mut rng) % 1_000_000_000) as i64;
                        driver
                            .execute_query("CREATE (:WNode {id: $id})")
                            .with_database(Arc::clone(&db))
                            .with_routing_control(RoutingControl::Write)
                            .with_parameters(value_map!({"id": id}))
                            .run()
                    } else {
                        let k = *write_batch as i64;
                        driver
                            .execute_query("UNWIND range(1, $k) AS i CREATE (:WNode {id: i})")
                            .with_database(Arc::clone(&db))
                            .with_routing_control(RoutingControl::Write)
                            .with_parameters(value_map!({"k": k}))
                            .run()
                    };
                    (r, true)
                }
            }
        };

        match result {
            Ok(eager) => {
                let addr = eager.summary.server_info.address.to_string();
                *served.entry(addr).or_insert(0) += 1;
                if is_write {
                    writes += 1;
                } else {
                    reads += 1;
                }
            }
            Err(e) => {
                eprintln!("query error: {e}");
            }
        }
    }

    ThreadResult { reads, writes, served }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!(
            "usage: {} <nthreads> <duration_secs> <mode:point|heavy|mix> <target:direct|routing> [write_pct] [write_batch]",
            args[0]
        );
        std::process::exit(1);
    }

    let nthreads: usize = args[1].parse().expect("nthreads must be a positive integer");
    let duration_secs: u64 = args[2].parse().expect("duration_secs must be a positive integer");
    let mode_str = args[3].as_str();
    let target_str = args[4].as_str();
    let write_pct: u8 = args
        .get(5)
        .map(|s| s.parse::<u8>().expect("write_pct must be 0-100"))
        .unwrap_or(20);
    let write_batch: u32 = args
        .get(6)
        .map(|s| s.parse::<u32>().expect("write_batch must be a positive integer"))
        .unwrap_or(1)
        .max(1);

    // For split mode arg[5] is reinterpreted as n_writers (not write_pct).
    // Default to 2 when absent; clamp so there is always >= 1 writer and >= 1 reader.
    let n_writers_clamped: usize = {
        let raw = if args.get(5).is_some() { write_pct as usize } else { 2 };
        raw.clamp(1, nthreads.saturating_sub(1).max(1))
    };

    let mode = match mode_str {
        "point" => Mode::Point,
        "heavy" => Mode::Heavy,
        "mix" => Mode::Mix { write_pct, write_batch },
        "split" => Mode::Split { write_batch },
        other => {
            eprintln!("unknown mode '{}'; expected point, heavy, mix, or split", other);
            std::process::exit(1);
        }
    };

    let target = match target_str {
        "direct" => Target::Direct,
        "routing" => Target::Routing,
        other => {
            eprintln!("unknown target '{}'; expected direct or routing", other);
            std::process::exit(1);
        }
    };

    let conn_config = match target {
        Target::Routing => ConnectionConfig::new(Address::from(("localhost", 7690_u16))),
        Target::Direct => {
            ConnectionConfig::new(Address::from(("localhost", 7687_u16))).with_routing(false)
        }
    };

    let driver = Arc::new(Driver::new(
        conn_config,
        DriverConfig::new().with_auth(Arc::new(AuthToken::new_none_auth())),
    ));

    let duration = Duration::from_secs(duration_secs);
    let start = Instant::now();

    // Split: the LAST n_writers_clamped threads are writers; all earlier threads are readers.
    // Other modes: is_reader is ignored by the worker arm; nthreads/2 is a neutral placeholder.
    let n_readers = match &mode {
        Mode::Split { .. } => nthreads - n_writers_clamped,
        _ => nthreads / 2,
    };

    let handles: Vec<_> = (0..nthreads)
        .map(|i| {
            let driver = Arc::clone(&driver);
            let mode = mode.clone();
            let is_reader = i < n_readers;
            std::thread::spawn(move || worker(driver, duration, mode, i as u64, is_reader))
        })
        .collect();

    let mut total_reads: u64 = 0;
    let mut total_writes: u64 = 0;
    let mut total_served: HashMap<String, u64> = HashMap::new();

    for handle in handles {
        let r = handle.join().expect("worker thread panicked");
        total_reads += r.reads;
        total_writes += r.writes;
        for (addr, count) in r.served {
            *total_served.entry(addr).or_insert(0) += count;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_queries = total_reads + total_writes;
    let qps = total_queries as f64 / elapsed;

    // Build served map string sorted by address for deterministic output.
    let mut pairs: Vec<_> = total_served.iter().collect();
    pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
    let served_str = format!(
        "{{{}}}",
        pairs
            .iter()
            .map(|(k, v)| format!("{k:?}: {v}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    match &mode {
        Mode::Mix { write_pct: wpct, write_batch: wbatch } => println!(
            "nthreads={nthreads} qps={qps:.0} mode=Mix target={target_str} wpct={wpct} wbatch={wbatch} reads={total_reads} writes={total_writes} served={served_str}"
        ),
        Mode::Split { write_batch: wbatch } => println!(
            "nthreads={nthreads} qps={qps:.0} mode=Split target={target_str} wbatch={wbatch} reads={total_reads} writes={total_writes} served={served_str}"
        ),
        Mode::Point => println!(
            "nthreads={nthreads} qps={qps:.0} mode=Point target={target_str} served={served_str}"
        ),
        Mode::Heavy => println!(
            "nthreads={nthreads} qps={qps:.0} mode=Heavy target={target_str} served={served_str}"
        ),
    }
}
