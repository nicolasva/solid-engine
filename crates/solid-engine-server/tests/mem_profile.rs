//! camp33-MEM (diagnostic, `--ignored`) : décompose la mémoire du serveur sous charge puis au repos.
//! Série 1 Hz (`MEMS`) : RSS/VmHWM/RssAnon/RssFile (/proc), jobs Done, tables, rétention, allocateur glibc, buffers de snapshot.
//! Variables : MEM_LABEL, MEM_SECS, MEM_REST, MEM_RATE, MEM_PAYLOAD, MEM_RET_MS, MEM_EVERY, MEM_SF, MEM_DUR, MEM_SHARDS,
//! MEM_ENV (`K=V,K=V` passées au serveur), MEM_CONSUME (0 = aucun consommateur).
//! Lancer : cargo test --release -p solid-engine-server --test mem_profile -- --ignored --nocapture
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BATCH: u32 = 250;

fn num(line: &str, key: &str) -> Option<u64> {
    let pat = format!("{key}=");
    let pat2 = format!("{key}: ");
    let i = line.find(&pat).map(|i| i + pat.len()).or_else(|| line.find(&pat2).map(|i| i + pat2.len()))?;
    line[i..].split(|c: char| !c.is_ascii_digit()).next()?.parse().ok()
}

fn status(pid: u32) -> HashMap<String, u64> {
    let mut m = HashMap::new();
    if let Ok(s) = std::fs::read_to_string(format!("/proc/{pid}/status")) {
        for l in s.lines() {
            if let Some((k, v)) = l.split_once(':') {
                if let Some(n) = v.split_whitespace().next().and_then(|x| x.parse::<u64>().ok()) {
                    m.insert(k.to_string(), n);
                }
            }
        }
    }
    m
}

#[derive(Default)]
struct Diag {
    // dernier rapport par shard/thread
    reports: HashMap<String, HashMap<&'static str, u64>>,
    proc: HashMap<&'static str, u64>,
    last_proc: String,
}

const REPORT_KEYS: [&str; 10] = [
    "active_jobs",
    "active_capacity",
    "done_entries",
    "done_capacity",
    "done_slot_size",
    "done_table_bytes",
    "retention_entries",
    "retention_capacity",
    "retention_bytes",
    "done_rehashes",
];
const PROC_KEYS: [&str; 12] = [
    "malloc_in_use",
    "malloc_free_held",
    "malloc_arena",
    "malloc_mmapped",
    "trims",
    "snap_n",
    "snap_len_max",
    "snap_cap_max",
    "snap_live_cap",
    "snap_live_cap_max",
    "snap_copy_live",
    "snap_total_live_max",
];

#[test]
#[ignore]
fn mem_profile() {
    let envs = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let envn = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    let label = envs("MEM_LABEL", "run");
    let (secs, rest, rate) = (envn("MEM_SECS", 20), envn("MEM_REST", 0), envn("MEM_RATE", 500_000));
    let (payload, ret_ms, every) = (envn("MEM_PAYLOAD", 16) as usize, envn("MEM_RET_MS", 3_600_000), envn("MEM_EVERY", 100_000));
    let (sf, dur, shards, consume) = (envs("MEM_SF", "true"), envs("MEM_DUR", "balanced"), envn("MEM_SHARDS", 4), envn("MEM_CONSUME", 1));
    let dir = std::env::temp_dir().join(format!("se-mem-{}-{}", std::process::id(), now_ms()));
    std::fs::create_dir_all(&dir).unwrap();
    let sock = dir.join("e.sock");
    let cfg = dir.join("engine.toml");
    std::fs::write(
        &cfg,
        format!(
            "[engine]\nshards = {shards}\n\n[jobs]\ncompleted_retention_ms = {ret_ms}\n\n[limits]\nmax_jobs_per_shard = 20000000\n\n\
             [server]\ntcp_addr = \"127.0.0.1:0\"\nunix_socket = \"{}\"\n\n\
             [storage]\ndurability = \"{dur}\"\ndata_dir = \"{}\"\nsnapshot_every_records = {every}\nsnapshot_single_flight = {sf}\n",
            sock.display(),
            dir.join("data").display()
        ),
    )
    .unwrap();
    // MEM_WRAP : préfixe de commande (ex. `heaptrack -o /f/ht`), pour profiler les allocations.
    let wrap = envs("MEM_WRAP", "");
    let mut words = wrap.split_whitespace();
    let mut cmd = match words.next() {
        Some(w) => {
            let mut c = Command::new(w);
            c.args(words).arg(env!("CARGO_BIN_EXE_solid-engine-server"));
            c
        }
        None => Command::new(env!("CARGO_BIN_EXE_solid-engine-server")),
    };
    cmd.arg("--config").arg(&cfg).env("SOLID_ENGINE_MEM_STATS", "1").env("SOLID_ENGINE_MEM_STATS_MS", "1000").stderr(Stdio::piped());
    for kv in envs("MEM_ENV", "").split(',').filter(|s| !s.is_empty()) {
        let (k, v) = kv.split_once('=').unwrap();
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().unwrap();
    let diag = Arc::new(Mutex::new(Diag::default()));
    let (d2, err) = (diag.clone(), child.stderr.take().unwrap());
    std::thread::spawn(move || {
        for line in BufReader::new(err).lines().map_while(|l| l.ok()) {
            if line.starts_with("mem_report") {
                let key = line.split_whitespace().nth(1).unwrap_or("?").to_string();
                let mut r = HashMap::new();
                for k in REPORT_KEYS {
                    if let Some(v) = num(&line, k) {
                        r.insert(k, v);
                    }
                }
                d2.lock().unwrap().reports.insert(key, r);
            } else if line.starts_with("mem_process") {
                let mut g = d2.lock().unwrap();
                g.last_proc = line.clone();
                for k in PROC_KEYS {
                    if let Some(v) = num(&line, k) {
                        g.proc.insert(k, v);
                    }
                }
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(300);
    while Client::connect_unix(&sock).is_err() {
        assert!(Instant::now() < deadline, "server did not start");
        std::thread::sleep(Duration::from_millis(50));
    }
    let pid = child.id();
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicU64::new(0));
    let confirmed = Arc::new(AtomicU64::new(0));
    let lat: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let period = Duration::from_secs_f64(BATCH as f64 / (rate as f64 / 4.0));
    let producers: Vec<_> = (0..4u128)
        .map(|t| {
            let mut c = Client::connect_unix(&sock).unwrap();
            let (stop, acked, lat) = (stop.clone(), acked.clone(), lat.clone());
            std::thread::spawn(move || {
                let mut local = Vec::new();
                let mut next_id = 1 + (t << 100);
                let mut next = Instant::now();
                while !stop.load(Ordering::Relaxed) {
                    let reqs: Vec<_> = (0..BATCH)
                        .map(|i| PushRequest {
                            job_id: next_id + i as u128,
                            channel_id: 1,
                            priority: 0,
                            max_attempts: 0,
                            delay_ms: 0,
                            partition_key: vec![],
                            payload: vec![7u8; payload],
                        })
                        .collect();
                    next_id += BATCH as u128;
                    let t_call = Instant::now();
                    match c.call(&Request::PushBatch(reqs)) {
                        Ok(Response::PushBatch(rs)) => {
                            local.push(t_call.elapsed().as_micros() as u64);
                            acked.fetch_add(rs.iter().filter(|r| r.is_ok()).count() as u64, Ordering::Relaxed);
                        }
                        _ => break,
                    }
                    next += period;
                    if let Some(w) = next.checked_duration_since(Instant::now()) {
                        std::thread::sleep(w);
                    }
                }
                lat.lock().unwrap().extend(local);
            })
        })
        .collect();
    let consumer = (consume > 0).then(|| {
        let mut c = Client::connect_unix(&sock).unwrap();
        let (stop, confirmed) = (stop.clone(), confirmed.clone());
        std::thread::spawn(move || {
            let mut idle = 0;
            loop {
                match c.call(&Request::ClaimBatch { channel_id: 1, lease_ms: 600_000, max: BATCH, wait_ms: 0 }) {
                    Ok(Response::ClaimBatch(js)) if !js.is_empty() => {
                        idle = 0;
                        let rs: Vec<_> = js
                            .iter()
                            .map(|j| Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token })
                            .collect();
                        if let Ok(Response::AckBatch(res)) = c.call(&Request::AckBatch(rs)) {
                            confirmed.fetch_add(res.iter().filter(|r| r.is_none()).count() as u64, Ordering::Relaxed);
                        } else {
                            break;
                        }
                    }
                    Ok(_) => {
                        idle += 1;
                        if stop.load(Ordering::Relaxed) && idle > 200 {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(_) => break,
                }
            }
        })
    });
    println!("MEMCONF label={label} secs={secs} rest={rest} rate={rate} payload={payload} ret_ms={ret_ms} every={every} sf={sf} dur={dur} shards={shards} consume={consume} env={}", envs("MEM_ENV", ""));
    let mb = |b: u64| b as f64 / 1048576.0;
    let t0 = Instant::now();
    let mut hwm_max = 0;
    let total = secs + rest;
    for t in 1..=total {
        std::thread::sleep(Duration::from_secs(1).saturating_sub(t0.elapsed().saturating_sub(Duration::from_secs(t - 1))));
        if t == secs {
            stop.store(true, Ordering::Relaxed);
        }
        let st = status(pid);
        let kb = |k: &str| st.get(k).copied().unwrap_or(0) * 1024;
        hwm_max = hwm_max.max(kb("VmHWM"));
        let g = diag.lock().unwrap();
        let sum = |k: &str| g.reports.values().map(|r| r.get(k).copied().unwrap_or(0)).sum::<u64>();
        let p = |k: &str| g.proc.get(k).copied().unwrap_or(0);
        println!(
            "MEMS {label} t={t} phase={} acked={} confirmed={} rss={:.0} hwm={:.0} anon={:.0} file={:.0} | active={} done={} done_cap={} done_tbl={:.0} ret_n={} ret_cap={} ret_mb={:.0} rehash={} | in_use={:.0} free_held={:.0} arena={:.0} mmapped={:.0} trims={} | snap_n={} snap_len_max={:.0} snap_cap_max={:.0} snap_live_cap={:.0} snap_live_cap_max={:.0} snap_copy_live={:.0} snap_total_live_max={:.0}",
            if t <= secs { "load" } else { "rest" },
            acked.load(Ordering::Relaxed),
            confirmed.load(Ordering::Relaxed),
            mb(kb("VmRSS")),
            mb(kb("VmHWM")),
            mb(kb("RssAnon")),
            mb(kb("RssFile")),
            sum("active_jobs"),
            sum("done_entries"),
            sum("done_capacity"),
            mb(sum("done_table_bytes")),
            sum("retention_entries"),
            sum("retention_capacity"),
            mb(sum("retention_bytes")),
            sum("done_rehashes"),
            mb(p("malloc_in_use")),
            mb(p("malloc_free_held")),
            mb(p("malloc_arena")),
            mb(p("malloc_mmapped")),
            p("trims"),
            p("snap_n"),
            mb(p("snap_len_max")),
            mb(p("snap_cap_max")),
            mb(p("snap_live_cap")),
            mb(p("snap_live_cap_max")),
            mb(p("snap_copy_live")),
            mb(p("snap_total_live_max")),
        );
    }
    println!("MEMGATE {label} {}", diag.lock().unwrap().last_proc.split_whitespace().filter(|w| w.starts_with("snap_")).collect::<Vec<_>>().join(" "));
    println!("MEMFINAL {label} vmhwm_mb={:.0} acked={} confirmed={}", mb(hwm_max), acked.load(Ordering::Relaxed), confirmed.load(Ordering::Relaxed));
    stop.store(true, Ordering::Relaxed);
    for p in producers {
        let _ = p.join();
    }
    {
        let mut v = lat.lock().unwrap().clone();
        v.sort_unstable();
        let q = |f: f64| v.get(((v.len() as f64 * f) as usize).min(v.len().saturating_sub(1))).copied().unwrap_or(0) as f64 / 1000.0;
        println!(
            "MEMLAT {label} n={} p50_ms={:.2} p95_ms={:.2} p99_ms={:.2} p999_ms={:.2} max_ms={:.2}",
            v.len(), q(0.5), q(0.95), q(0.99), q(0.999), v.last().copied().unwrap_or(0) as f64 / 1000.0
        );
    }
    child.kill().unwrap();
    child.wait().unwrap();
    if envn("MEM_RECOVER", 0) == 1 {
        recover_report(&label, &dir, &cfg, &sock);
    }
    if let Some(c) = consumer {
        let _ = c.join();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn dur_ms(s: &str) -> f64 {
    if let Some(v) = s.strip_suffix("ms") {
        v.parse().unwrap_or(0.0)
    } else if let Some(v) = s.strip_suffix("µs") {
        v.parse::<f64>().unwrap_or(0.0) / 1000.0
    } else if let Some(v) = s.strip_suffix('s') {
        v.parse::<f64>().unwrap_or(0.0) * 1000.0
    } else {
        0.0
    }
}

/// Après SIGKILL : volume WAL/snapshots conservé, records à rejouer, temps de récupération et de reprise du service.
fn recover_report(label: &str, dir: &std::path::Path, cfg: &std::path::Path, sock: &std::path::Path) {
    fn walk(d: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.push(p);
            }
        }
    }
    let mut files = vec![];
    walk(&dir.join("data"), &mut files);
    let (mut wal, mut snap) = (0u64, 0u64);
    let mut snap_seq: HashMap<String, u64> = HashMap::new();
    for p in &files {
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let len = std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        let shard = p.parent().unwrap().file_name().unwrap().to_string_lossy().trim_start_matches("shard-").to_string();
        if name.starts_with("wal-") {
            wal += len;
        } else if name.starts_with("snap-") && name.ends_with(".snap") {
            snap += len;
            let seq: u64 = name["snap-".len()..name.len() - ".snap".len()].parse().unwrap_or(0);
            let e = snap_seq.entry(shard).or_insert(0);
            *e = (*e).max(seq);
        }
    }
    let _ = std::fs::remove_file(sock);
    let t0 = Instant::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_solid-engine-server"))
        .arg("--config")
        .arg(cfg)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let err = child.stderr.take().unwrap();
    let info: Arc<Mutex<Vec<(String, f64, u64)>>> = Arc::new(Mutex::new(vec![]));
    let i2 = info.clone();
    std::thread::spawn(move || {
        for line in BufReader::new(err).lines().map_while(|l| l.ok()) {
            if line.contains("recovered in") {
                let w: Vec<&str> = line.split_whitespace().collect();
                let shard = w[1].to_string();
                let ms = dur_ms(w[4]);
                let seq = w.last().unwrap().trim_end_matches(')').parse().unwrap_or(0);
                i2.lock().unwrap().push((shard, ms, seq));
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(600);
    while Client::connect_unix(sock).is_err() {
        assert!(Instant::now() < deadline, "le serveur ne redémarre pas");
        std::thread::sleep(Duration::from_millis(20));
    }
    let ready_ms = t0.elapsed().as_secs_f64() * 1000.0;
    std::thread::sleep(Duration::from_millis(200));
    let v = info.lock().unwrap().clone();
    let rec_max = v.iter().map(|x| x.1).fold(0.0, f64::max);
    let replayed: u64 = v.iter().map(|(s, _, last)| last.saturating_sub(*snap_seq.get(s).unwrap_or(&0))).sum();
    println!(
        "MEMREC {label} wal_mb={:.0} snap_mb={:.0} shards_recovered={} replayed_records={} recover_max_ms={:.0} ready_ms={:.0}",
        wal as f64 / 1048576.0,
        snap as f64 / 1048576.0,
        v.len(),
        replayed,
        rec_max,
        ready_ms
    );
    child.kill().unwrap();
    child.wait().unwrap();
}
