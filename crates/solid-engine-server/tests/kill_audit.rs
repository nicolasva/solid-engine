//! Audit de perte après SIGKILL (investigation, `--ignored`). Compare des ensembles d'identifiants, pas des compteurs :
//! pour chaque job dont le push a été acquitté, est-il récupéré ? S'il manque, était-il écrit sur disque avant la coupure ?
//! Lancer : cargo test --release -p solid-engine-server --test kill_audit -- --ignored --nocapture
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn temp_dir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("se-audit-{tag}-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
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

struct Daemon {
    child: Child,
    sock: PathBuf,
}

impl Daemon {
    fn start(dir: &Path, durability: &str, every: u64, single_flight: bool) -> Self {
        let sock = dir.join("e.sock");
        let cfg_path = dir.join("engine.toml");
        let toml = format!(
            "[engine]\nshards = 2\n\n[server]\ntcp_addr = \"127.0.0.1:0\"\nunix_socket = \"{}\"\n\n\
             [storage]\ndurability = \"{durability}\"\ndata_dir = \"{}\"\nsnapshot_every_records = {every}\nsnapshot_single_flight = {single_flight}\n",
            sock.display(),
            dir.join("data").display(),
        );
        std::fs::write(&cfg_path, toml).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_solid-engine-server"))
            .arg("--config")
            .arg(&cfg_path)
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while Client::connect_unix(&sock).is_err() {
            assert!(Instant::now() < deadline, "server did not start");
            std::thread::sleep(Duration::from_millis(20));
        }
        Self { child, sock }
    }

    fn client(&self) -> Client<std::os::unix::net::UnixStream> {
        Client::connect_unix(&self.sock).unwrap()
    }

    fn sigkill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

/// Identifiants (u32 bas) retrouvés dans les fichiers WAL et snapshot : le payload d'un job est son id sur 16 octets
/// petit-boutiste, soit 4 octets utiles suivis de 12 zéros.
fn ids_on_disk(data: &Path) -> HashSet<u32> {
    let mut out = HashSet::new();
    fn walk(p: &Path, out: &mut HashSet<u32>) {
        let Ok(rd) = std::fs::read_dir(p) else { return };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "log" || x == "snap") {
                let b = std::fs::read(&p).unwrap();
                for i in 0..b.len().saturating_sub(16) {
                    if b[i + 4..i + 16].iter().all(|&x| x == 0) {
                        out.insert(u32::from_le_bytes(b[i..i + 4].try_into().unwrap()));
                    }
                }
            }
        }
    }
    walk(data, &mut out);
    out
}

struct Report {
    acked_push: usize,
    lost_push: usize,
    lost_on_disk: usize,
    tail_span: usize,
    acked_ack: usize,
    lost_ack: usize,
    unacked_present: usize,
}

fn audit(durability: &str, every: u64, single_flight: bool, run_ms: u64) -> Report {
    let dir = temp_dir(durability);
    let daemon = Daemon::start(&dir, durability, every, single_flight);
    let producers: Vec<_> = (0..4u128)
        .map(|t| {
            let mut c = daemon.client();
            let base = 1 + t * 10_000_000;
            std::thread::spawn(move || {
                let mut acked = Vec::new();
                for k in 0..10_000_000u128 {
                    match c.call(&push(base + k)) {
                        Ok(Response::Push(_)) => acked.push(base + k),
                        _ => break,
                    }
                }
                acked
            })
        })
        .collect();
    let consumer = {
        let mut c = daemon.client();
        std::thread::spawn(move || {
            let mut done = Vec::new();
            loop {
                match c.call(&Request::ClaimNextJob { channel_id: 1, lease_ms: 600_000, wait_ms: 0 }) {
                    Ok(Response::Claim(Some(j))) => {
                        let r = Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token };
                        match c.call(&Request::AckJob(r)) {
                            Ok(Response::Ack) => done.push(j.job_id),
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
    // La coupure précède la jointure : l'ensemble des acquittements reçus est exact, sans frontière temporelle floue.
    daemon.sigkill();
    let acked: Vec<Vec<u128>> = producers.into_iter().map(|h| h.join().unwrap()).collect();
    let acked_ack = consumer.join().unwrap();

    // Ce qui était sur disque au moment de la coupure, avant que la récupération n'y touche.
    let on_disk = ids_on_disk(&dir.join("data"));

    let daemon = Daemon::start(&dir, durability, every, false);
    let mut c = daemon.client();
    let (mut lost_push, mut lost_on_disk, mut tail_span, mut acked_push) = (0, 0, 0, 0);
    for ids in &acked {
        acked_push += ids.len();
        let mut first_lost: Option<usize> = None;
        for (pos, id) in ids.iter().enumerate() {
            if !matches!(c.call(&Request::GetJob { job_id: *id }).unwrap(), Response::Job(_)) {
                lost_push += 1;
                lost_on_disk += usize::from(on_disk.contains(&(*id as u32)));
                first_lost.get_or_insert(pos);
            }
        }
        // Distance entre le premier job perdu et le dernier acquitté de ce thread.
        if let Some(p) = first_lost {
            tail_span = tail_span.max(ids.len() - p);
        }
    }
    let mut lost_ack = 0;
    for id in &acked_ack {
        let done = matches!(c.call(&Request::GetJob { job_id: *id }).unwrap(), Response::Job(v) if v.state == JobState::Completed);
        lost_ack += usize::from(!done);
    }
    let Response::Stats(s) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
    let recovered = (s.ready + s.claimed + s.completed) as usize;
    daemon.sigkill();
    let _ = std::fs::remove_dir_all(&dir);
    Report {
        acked_push,
        lost_push,
        lost_on_disk,
        tail_span,
        acked_ack: acked_ack.len(),
        lost_ack,
        // Au plus un push envoyé sans réponse par producteur peut exister sans avoir été acquitté.
        unacked_present: recovered.saturating_sub(acked_push - lost_push),
    }
}

#[test]
#[ignore]
fn kill_audit() {
    for (label, durability, every, sf) in [
        ("strict       every=20000", "strict", 20_000, false),
        ("balanced     every=0    ", "balanced", 0, false),
        ("balanced     every=20000", "balanced", 20_000, false),
        ("balanced+sf  every=20000", "balanced", 20_000, true),
    ] {
        for rep in 1..=3 {
            let r = audit(durability, every, sf, 1500);
            println!(
                "AUDIT {label} r{rep}: push_acquittés={} perdus={} (sur disque avant récupération: {}) étendue_queue={} | acks={} acks_perdus={} | présents_non_acquittés={}",
                r.acked_push, r.lost_push, r.lost_on_disk, r.tail_span, r.acked_ack, r.lost_ack, r.unacked_present
            );
        }
    }
}
