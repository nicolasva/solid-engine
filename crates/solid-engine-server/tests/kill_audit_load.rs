//! camp28 (investigation, `--ignored`) : audit après SIGKILL à débit imposé, avec analyse structurée des WAL
//! (records valides, séquences) et retard du writer échantillonné par le serveur (SOLID_ENGINE_WAL_LAG).
//! Lancer : AUDIT_RUN_MS=30000 AUDIT_RATE=500000 cargo test --release -p solid-engine-server --test kill_audit_load -- --ignored --nocapture
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BATCH: u32 = 250;
const SPAN: u32 = 10_000_000;
const SHARDS: usize = 4;
const WORDS: usize = 1 << 20;

fn push(id: u32) -> PushRequest {
    PushRequest {
        job_id: id as u128,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: vec![],
        payload: (id as u128).to_le_bytes().to_vec(),
    }
}

#[derive(Default)]
struct Lag {
    last: Vec<[u64; 4]>,
    samples: u64,
    sum: u64,
    max: u64,
    bp_us: u64,
    pbmax: u64,
    bbmax: u64,
    bw: u64,
}

struct Daemon {
    child: Child,
    sock: PathBuf,
    lag: Arc<Mutex<Lag>>,
    reader: Option<std::thread::JoinHandle<()>>,
}

impl Daemon {
    fn start(dir: &Path, durability: &str, every: u64, sf: bool, delay_ms: u64, lag_on: bool) -> Self {
        let sock = dir.join("e.sock");
        let cfg = dir.join("engine.toml");
        std::fs::write(
            &cfg,
            format!(
                "[engine]\nshards = {SHARDS}\n\n[jobs]\ncompleted_retention_ms = 3600000\n\n[limits]\nmax_jobs_per_shard = 20000000\n\n\
                 [server]\ntcp_addr = \"127.0.0.1:0\"\nunix_socket = \"{}\"\n\n\
                 [storage]\ndurability = \"{durability}\"\ndata_dir = \"{}\"\nsnapshot_every_records = {every}\nsnapshot_single_flight = {sf}\n",
                sock.display(),
                dir.join("data").display()
            ),
        )
        .unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_solid-engine-server"));
        cmd.arg("--config").arg(&cfg).env("SOLID_ENGINE_TEST_SNAPSHOT_DELAY_MS", delay_ms.to_string()).stderr(Stdio::piped());
        if lag_on {
            cmd.env("SOLID_ENGINE_WAL_LAG", "1");
        }
        let mut child = cmd.spawn().unwrap();
        let lag = Arc::new(Mutex::new(Lag::default()));
        let (l2, err) = (lag.clone(), child.stderr.take().unwrap());
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(err).lines().map_while(|l| l.ok()) {
                let Some(rest) = line.strip_prefix("wal_lag ") else { continue };
                let bp = rest.split(' ').find_map(|t| t.strip_prefix("bp=")).and_then(|v| v.parse().ok()).unwrap_or(0);
                let tok = |k: &str| rest.split(' ').find_map(|t| t.strip_prefix(k)).and_then(|v| v.parse().ok()).unwrap_or(0u64);
                let (pbmax, bbmax, bw) = (tok("pbmax="), tok("bbmax="), tok("bw="));
                let mut last = Vec::new();
                for tok in rest.split(' ').skip(1) {
                    let Some((_, v)) = tok.split_once(':') else { continue };
                    let v: Vec<u64> = v.split(',').filter_map(|x| x.parse().ok()).collect();
                    if v.len() == 4 {
                        last.push([v[0], v[1], v[2], v[3]]);
                    }
                }
                let mut g = l2.lock().unwrap();
                let total: u64 = last.iter().take(SHARDS).map(|s| s[0].saturating_sub(s[1])).sum();
                g.samples += 1;
                g.sum += total;
                g.max = g.max.max(total);
                g.bp_us = bp;
                (g.pbmax, g.bbmax, g.bw) = (pbmax, bbmax, bw);
                g.last = last;
            }
        });
        let deadline = Instant::now() + Duration::from_secs(300);
        while Client::connect_unix(&sock).is_err() {
            assert!(Instant::now() < deadline, "server did not start");
            std::thread::sleep(Duration::from_millis(50));
        }
        Self { child, sock, lag, reader: Some(reader) }
    }
    fn client(&self) -> Client<UnixStream> {
        Client::connect_unix(&self.sock).unwrap()
    }
    fn sigkill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        if let Some(r) = self.reader.take() {
            let _ = r.join();
        }
    }
}

fn crc32(table: &[u32; 256], data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &x in data {
        c = table[((c ^ x as u32) & 0xff) as usize] ^ (c >> 8);
    }
    !c
}

fn mark(bits: &mut [u64], id: u32) {
    bits[(id >> 6) as usize] |= 1 << (id & 63);
}
fn has(bits: &[u64], id: u32) -> bool {
    bits[(id >> 6) as usize] >> (id & 63) & 1 == 1
}

struct Disk {
    push: Vec<u64>,
    ack: Vec<u64>,
    snap_pattern: Vec<u64>,
    last_seq: [u64; SHARDS],
    snap_seq: [u64; SHARDS],
    torn_bytes: u64,
    bad_records: u64,
    bytes: u64,
}

/// Analyse structurée des segments WAL : records valides jusqu'au premier invalide (CRC, longueur), par shard.
fn read_disk(data: &Path) -> Disk {
    let mut d = Disk {
        push: vec![0; WORDS],
        ack: vec![0; WORDS],
        snap_pattern: vec![0; WORDS],
        last_seq: [0; SHARDS],
        snap_seq: [0; SHARDS],
        torn_bytes: 0,
        bad_records: 0,
        bytes: 0,
    };
    let mut table = [0u32; 256];
    for (i, t) in table.iter_mut().enumerate() {
        let mut c = i as u32;
        for _ in 0..8 {
            c = if c & 1 == 1 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
        }
        *t = c;
    }
    for sh in 0..SHARDS {
        let Ok(rd) = std::fs::read_dir(data.join(format!("shard-{sh}"))) else { continue };
        for e in rd.flatten() {
            let p = e.path();
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            let b = std::fs::read(&p).unwrap();
            d.bytes += b.len() as u64;
            if name.ends_with(".snap") {
                if b.len() >= 18 {
                    d.snap_seq[sh] = d.snap_seq[sh].max(u64::from_le_bytes(b[10..18].try_into().unwrap()));
                }
                for i in 0..b.len().saturating_sub(16) {
                    if b[i + 4..i + 16].iter().all(|&x| x == 0) {
                        let id = u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
                        if (1..(1 << 26)).contains(&id) {
                            mark(&mut d.snap_pattern, id);
                        }
                    }
                }
            } else if name.ends_with(".log") {
                let mut off = 8;
                while off + 36 <= b.len() {
                    let len = u32::from_le_bytes(b[off..off + 4].try_into().unwrap()) as usize;
                    let total = 32 + len + 4;
                    if off + total > b.len() {
                        break;
                    }
                    let crc = u32::from_le_bytes(b[off + total - 4..off + total].try_into().unwrap());
                    if crc32(&table, &b[off..off + total - 4]) != crc {
                        d.bad_records += 1;
                        break;
                    }
                    let rtype = b[off + 5];
                    let seq = u64::from_le_bytes(b[off + 8..off + 16].try_into().unwrap());
                    let id = u128::from_le_bytes(b[off + 16..off + 32].try_into().unwrap());
                    d.last_seq[sh] = d.last_seq[sh].max(seq);
                    if id < (1 << 26) {
                        match rtype {
                            1 => mark(&mut d.push, id as u32),
                            4 => mark(&mut d.ack, id as u32),
                            _ => {}
                        }
                    }
                    off += total;
                }
                d.torn_bytes += (b.len() - off.min(b.len())) as u64;
            }
        }
    }
    d
}

/// GetJob pipeliné ; renvoie l'état de chaque id (None = absent).
fn fetch(c: &mut Client<UnixStream>, ids: &[u32]) -> Vec<Option<JobState>> {
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(1000) {
        for id in chunk {
            c.send(&Request::GetJob { job_id: *id as u128 }).unwrap();
        }
        for _ in chunk {
            out.push(match c.recv().unwrap().1 {
                Response::Job(v) => Some(v.state),
                _ => None,
            });
        }
    }
    out
}

fn run(label: &str, durability: &str, every: u64, sf: bool, delay_ms: u64, run_ms: u64, rate: u64) {
    let dir = std::env::temp_dir().join(format!("se-load-{}-{}", std::process::id(), now_ms()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut daemon = Daemon::start(&dir, durability, every, sf, delay_ms, true);
    let t0 = Instant::now();
    let period = Duration::from_secs_f64(BATCH as f64 / (rate as f64 / 4.0));
    let producers: Vec<_> = (0..4u32)
        .map(|t| {
            let mut c = daemon.client();
            std::thread::spawn(move || {
                let base = 1 + t * SPAN;
                let mut acked: Vec<u32> = Vec::new();
                let mut rejected = 0usize;
                let mut lat: Vec<u32> = Vec::new();
                let mut k = 0;
                let mut next = Instant::now();
                while k < SPAN {
                    let n = BATCH.min(SPAN - k);
                    let reqs: Vec<_> = (0..n).map(|i| push(base + k + i)).collect();
                    let tc = Instant::now();
                    let resp = c.call(&Request::PushBatch(reqs));
                    lat.push(tc.elapsed().as_micros() as u32);
                    match resp {
                        Ok(Response::PushBatch(rs)) => {
                            for (i, r) in rs.iter().enumerate() {
                                if r.is_ok() {
                                    acked.push(base + k + i as u32);
                                } else {
                                    rejected += 1;
                                }
                            }
                        }
                        _ => break,
                    }
                    k += n;
                    next += period;
                    if let Some(w) = next.checked_duration_since(Instant::now()) {
                        std::thread::sleep(w);
                    }
                }
                (acked, rejected, lat)
            })
        })
        .collect();
    let consumer = {
        let mut c = daemon.client();
        std::thread::spawn(move || {
            let mut done: Vec<u32> = Vec::new();
            loop {
                match c.call(&Request::ClaimBatch { channel_id: 1, lease_ms: 600_000, max: BATCH, wait_ms: 0 }) {
                    Ok(Response::ClaimBatch(js)) if !js.is_empty() => {
                        let rs: Vec<_> = js
                            .iter()
                            .map(|j| Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token })
                            .collect();
                        match c.call(&Request::AckBatch(rs)) {
                            Ok(Response::AckBatch(res)) => {
                                for (j, r) in js.iter().zip(res) {
                                    if r.is_none() {
                                        done.push(j.job_id as u32);
                                    }
                                }
                            }
                            _ => break,
                        }
                    }
                    Ok(_) => std::thread::sleep(Duration::from_millis(1)),
                    Err(_) => break,
                }
            }
            done
        })
    };
    std::thread::sleep(Duration::from_millis(run_ms));
    let hwm = std::fs::read_to_string(format!("/proc/{}/status", daemon.child.id()))
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()))
        .unwrap_or(0);
    daemon.sigkill();
    let elapsed = t0.elapsed().as_secs_f64();
    let mut acked = Vec::new();
    let mut rejected = 0;
    let mut lats: Vec<u32> = Vec::new();
    for h in producers {
        let (a, r, l) = h.join().unwrap();
        acked.push(a);
        rejected += r;
        lats.extend(l);
    }
    lats.sort_unstable();
    let q = |p: f64| lats.get(((lats.len() as f64 * p) as usize).min(lats.len().saturating_sub(1))).copied().unwrap_or(0) as f64 / 1000.0;
    let acked_ack = consumer.join().unwrap();
    let total: usize = acked.iter().map(Vec::len).sum();
    let lag = daemon.lag.lock().unwrap();
    let last = lag.last.clone();
    let (lag_mean, lag_max, bp_ms) = (lag.sum / lag.samples.max(1), lag.max, lag.bp_us / 1000);
    let (pbmax, bbmax, bw) = (lag.pbmax, lag.bbmax, lag.bw);
    drop(lag);

    let disk = read_disk(&dir.join("data"));

    let t1 = Instant::now();
    let mut daemon2 = Daemon::start(&dir, durability, every, false, 0, false);
    let ready_s = t1.elapsed().as_secs_f64();
    let mut c = daemon2.client();
    let first_op_s = {
        let Response::Stats(_) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
        t1.elapsed().as_secs_f64()
    };
    // lost = push acquitté absent après récupération, classé selon ce que le disque contenait.
    let (mut lost, mut lost_valid_rec, mut lost_snap_pat, mut max_tail) = (0usize, 0usize, 0usize, 0usize);
    for ids in &acked {
        let st = fetch(&mut c, ids);
        let miss: Vec<usize> = st.iter().enumerate().filter(|(_, s)| s.is_none()).map(|(i, _)| i).collect();
        lost += miss.len();
        for &i in &miss {
            if has(&disk.push, ids[i]) {
                lost_valid_rec += 1;
            } else if has(&disk.snap_pattern, ids[i]) {
                lost_snap_pat += 1;
            }
        }
        if let Some(&first) = miss.first() {
            max_tail = max_tail.max(ids.len() - first);
        }
    }
    let st = fetch(&mut c, &acked_ack);
    let ack_absent = st.iter().filter(|s| s.is_none()).count();
    let ack_other = st.iter().filter(|s| matches!(s, Some(x) if *x != JobState::Completed)).count();
    let ack_valid_rec_not_done = acked_ack
        .iter()
        .zip(&st)
        .filter(|(id, s)| !matches!(s, Some(JobState::Completed)) && has(&disk.ack, **id))
        .count();
    let Response::Stats(s) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
    let recovered = (s.ready + s.claimed + s.completed) as usize;
    daemon2.sigkill();
    let _ = std::fs::remove_dir_all(&dir);

    let mut frontier = String::new();
    let mut unwritten = 0u64;
    for (i, l) in last.iter().take(SHARDS).enumerate() {
        let disk_seq = disk.last_seq[i].max(disk.snap_seq[i]);
        unwritten += l[0].saturating_sub(disk_seq);
        frontier += &format!(" s{i}[sent={} written={} synced={} snap={} disk={}]", l[0], l[1], l[2], l[3], disk_seq);
    }
    let rate_real = total as f64 / elapsed;
    println!(
        "AUDIT28 {label}: run={elapsed:.1}s offert={rate} acquitté={rate_real:.0}/s push_acquittés={total} rejetés={rejected} acks_client={} | \
         PUSH_PERDUS={lost} [record_valide_sur_disque={lost_valid_rec} motif_snapshot={lost_snap_pat}] queue_max={max_tail} ({:.0} ms) | \
         ACKS absents={ack_absent} autre_état={ack_other} [record_ack_valide_sur_disque={ack_valid_rec_not_done}] | \
         récupérés={recovered} inattendus={} | retard_writer mean={lag_mean} max={lag_max} records | records_envoyés_non_écrits_au_kill≈{unwritten} |{frontier} | \
         wal_torn_bytes={} crc_invalides={} disque={:.0}Mo prêt={ready_s:.1}s première_op={first_op_s:.1}s | lat_batch_ms p50={:.1} p99={:.1} p99.9={:.1} | VmHWM={}Mo attente_backpressure={bp_ms}ms | octets_pend_max={} lot_max={} octets_écrits={} octets_par_record={:.1}",
        acked_ack.len(),
        max_tail as f64 / rate_real * 1000.0,
        recovered as i64 - (total - lost) as i64,
        disk.torn_bytes,
        disk.bad_records,
        disk.bytes as f64 / 1e6,
        q(0.5),
        q(0.99),
        q(0.999),
        hwm / 1024,
        pbmax,
        bbmax,
        bw,
        bw as f64 / last.iter().take(SHARDS).map(|s| s[1]).sum::<u64>().max(1) as f64,
    );
}

#[test]
#[ignore]
fn kill_audit_load() {
    let env = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    let (run_ms, rate, reps) = (env("AUDIT_RUN_MS", 30_000), env("AUDIT_RATE", 500_000), env("AUDIT_REPS", 3));
    let delay = env("AUDIT_DELAY_MS", 0);
    let only = std::env::var("AUDIT_ONLY").unwrap_or_default();
    for rep in 1..=reps {
        for (label, dur, sf) in [("balanced sf=on ", "balanced", true), ("balanced sf=off", "balanced", false), ("strict   sf=on ", "strict", true)] {
            if !only.is_empty() && !label.contains(&only) {
                continue;
            }
            if dur == "strict" && rep > 2 {
                continue;
            }
            run(&format!("{label} r{rep}"), dur, 100_000, sf, delay, run_ms, rate);
        }
    }
}
