//! camp35 (banc de mesure, `--ignored`) : charge balanced continue, serveur tué par SIGKILL puis récupéré en processus,
//! avec les compteurs du writer (SOLID_ENGINE_WAL_LAG) conservés bruts dans C35_OUT.
//! Comparer les modes avec SOLID_ENGINE_SNAPSHOT_WORKER=0|1.
//! Variables : C35_SNAP (snapshot_every_records, 0 = aucun), C35_SINGLE (0|1), C35_SECS, C35_CLIENTS, C35_LABEL.
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn envn(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// Temps CPU de `ps -o time=` : `M:SS.cc` (macOS), `HH:MM:SS` ou `J-HH:MM:SS` (Linux).
fn cpu_secs(t: &str) -> f64 {
    let t = t.trim().replace(',', ".");
    let (days, rest) = t.split_once('-').map_or((0.0, t.as_str()), |(d, r)| (d.parse().unwrap_or(0.0), r));
    days * 86400.0 + rest.split(':').fold(0.0, |acc, p| acc * 60.0 + p.parse().unwrap_or(0.0))
}

fn push(id: u128) -> Request {
    Request::PushJob(PushRequest {
        job_id: id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: vec![],
        payload: id.to_le_bytes().to_vec(),
    })
}

#[test]
#[ignore]
fn writer_diag() {
    let (snap, single) = (envn("C35_SNAP", 0), envn("C35_SINGLE", 0));
    let (secs, clients) = (envn("C35_SECS", 6), envn("C35_CLIENTS", 4));
    let label = std::env::var("C35_LABEL").unwrap_or_else(|_| "x".into());
    let dir = std::env::temp_dir().join(format!("se-c35-{}-{}", std::process::id(), now_ms()));
    std::fs::create_dir_all(&dir).unwrap();
    let sock = dir.join("e.sock");
    let cfg = dir.join("engine.toml");
    std::fs::write(
        &cfg,
        format!(
            "[engine]\nshards = 2\n\n[server]\ntcp_addr = \"127.0.0.1:0\"\nunix_socket = \"{}\"\n\n[storage]\ndurability = \"balanced\"\ndata_dir = \"{}\"\nsnapshot_every_records = {snap}\nsnapshot_single_flight = {}\n",
            sock.display(),
            dir.join("data").display(),
            single == 1
        ),
    )
    .unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_solid-engine-server"));
    cmd.arg("--config").arg(&cfg).stderr(Stdio::piped());
    // C35_NODIAG=1 : instrumentation désactivée, pour mesurer son coût sur le débit.
    if std::env::var("C35_NODIAG").is_err() {
        cmd.env("SOLID_ENGINE_WAL_LAG", "1");
    }
    let mut child = cmd.spawn().unwrap();
    let err = child.stderr.take().unwrap();
    let out_path = std::env::var("C35_OUT").unwrap_or_else(|_| dir.join("raw.log").display().to_string());
    let reader = std::thread::spawn(move || {
        let mut f = std::fs::File::create(&out_path).unwrap();
        let (mut max_pend, mut max_pb) = (0u64, 0u64);
        let mut snapc = String::new();
        for line in BufReader::new(err).lines().map_while(|l| l.ok()) {
            if let Some(rest) = line.strip_prefix("wal_lag ") {
                let mut pend = 0;
                for tok in rest.split(' ').skip(1) {
                    if let Some((_, v)) = tok.split_once(':') {
                        let v: Vec<u64> = v.split(',').filter_map(|x| x.parse().ok()).collect();
                        if v.len() == 4 {
                            pend += v[0].saturating_sub(v[1]);
                        }
                    }
                }
                max_pend = max_pend.max(pend);
                if let Some(t) = rest.split(' ').find_map(|t| t.strip_prefix("pbmax=")) {
                    max_pb = max_pb.max(t.parse().unwrap_or(0));
                }
            }
            if let Some(t) = line.split(' ').find_map(|t| t.strip_prefix("snapc=")) {
                snapc = t.to_string();
            }
            let _ = writeln!(f, "{line}");
        }
        (max_pend, max_pb, snapc)
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    while Client::connect_unix(&sock).is_err() {
        assert!(Instant::now() < deadline, "server did not start");
        std::thread::sleep(Duration::from_millis(20));
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let t0 = Instant::now();
    let hs: Vec<_> = (0..clients as u128)
        .map(|t| {
            let mut c = Client::connect_unix(&sock).unwrap();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let (mut n, mut k) = (0u64, 0u128);
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    if let Ok(Response::Push(_)) = c.call(&push(t * 1_000_000_000 + k)) {
                        n += 1;
                    }
                    k += 1;
                }
                n
            })
        })
        .collect();
    let pid = child.id();
    let rss_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rss_flag = rss_stop.clone();
    let rss_thread = std::thread::spawn(move || {
        let mut max_kb = 0u64;
        while !rss_flag.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok(o) = Command::new("ps").args(["-o", "rss=", "-p", &pid.to_string()]).output() {
                max_kb = max_kb.max(String::from_utf8_lossy(&o.stdout).trim().parse().unwrap_or(0));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        max_kb
    });
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let pushed: u64 = hs.into_iter().map(|h| h.join().unwrap()).sum();
    let el = t0.elapsed().as_secs_f64();
    std::thread::sleep(Duration::from_millis(300));
    let cpu = Command::new("ps").args(["-o", "time=", "-p", &child.id().to_string()]).output().map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).unwrap_or_default();
    let cpu_s = cpu_secs(&cpu);
    child.kill().unwrap();
    child.wait().unwrap();
    rss_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let rss_kb = rss_thread.join().unwrap();
    let (max_pend, max_pb, snapc) = reader.join().unwrap();
    let t_rec = Instant::now();
    let mut rcfg = solid_engine_core::config::Config::default();
    rcfg.engine.shards = 2;
    rcfg.server.tcp_addr = Some("127.0.0.1:0".into());
    rcfg.storage.durability = solid_engine_core::config::Durability::Balanced;
    rcfg.storage.data_dir = Some(dir.join("data").display().to_string());
    rcfg.storage.snapshot_every_records = snap;
    let recovered = solid_engine_server::Server::start(rcfg).unwrap();
    let rec_ms = t_rec.elapsed().as_secs_f64() * 1000.0;
    recovered.shutdown();
    let mut snap_bytes = 0u64;
    for sh in 0..2 {
        if let Ok(rd) = std::fs::read_dir(dir.join("data").join(format!("shard-{sh}"))) {
            for e in rd.flatten() {
                if e.file_name().to_string_lossy().ends_with(".snap") {
                    snap_bytes += e.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
    }
    println!(
        "C35 {label} snap={snap} single={single} clients={clients} pushed={pushed} rate={:.0}/s max_pend_records={max_pend} max_pend_bytes={max_pb} rss_kb={rss_kb} recover_ms={rec_ms:.1} snapc={snapc} cpu_s={cpu_s:.2} snap_bytes={snap_bytes}",
        pushed as f64 / el
    );
    let _ = std::fs::remove_dir_all(&dir);
}
