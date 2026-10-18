use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn temp_dir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("solid-engine-{tag}-{}-{}", std::process::id(), now_ms()));
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

fn reservation(j: &ClaimedJob) -> Reservation {
    Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token }
}

fn config(dir: &PathBuf, durability: Durability) -> Config {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = durability;
    cfg.storage.data_dir = Some(dir.join("data").to_string_lossy().into());
    cfg.storage.snapshot_every_records = 50;
    cfg
}

#[test]
fn restart_preserves_confirmed_jobs_across_snapshots() {
    let dir = temp_dir("restart");
    {
        let server = Server::start(config(&dir, Durability::Strict)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        // Plus de 50 enregistrements par shard : au moins un snapshot est pris en cours de route.
        for id in 1..=300u128 {
            assert!(matches!(c.call(&push(id)).unwrap(), Response::Push(_)));
        }
        server.shutdown();
    }
    for _ in 0..2 {
        let server = Server::start(config(&dir, Durability::Strict)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        let Response::Stats(s) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
        assert_eq!(s.ready, 300);
        server.shutdown();
    }
}

#[test]
fn data_dir_cannot_be_shared_or_resharded() {
    let dir = temp_dir("locked");
    let server = Server::start(config(&dir, Durability::Strict)).unwrap();
    assert!(Server::start(config(&dir, Durability::Strict)).is_err());
    server.shutdown();
    let mut cfg = config(&dir, Durability::Strict);
    cfg.engine.shards = 2;
    assert!(Server::start(cfg).is_err());
}

struct Daemon {
    child: Child,
    sock: PathBuf,
}

impl Daemon {
    fn start(dir: &PathBuf) -> Self {
        let sock = dir.join("engine.sock");
        let cfg_path = dir.join("engine.toml");
        let toml = format!(
            "[engine]\nshards = 2\n\n[server]\ntcp_addr = \"127.0.0.1:0\"\nunix_socket = \"{}\"\n\n\
             [storage]\ndurability = \"strict\"\ndata_dir = \"{}\"\n",
            sock.display(),
            dir.join("data").display()
        );
        std::fs::write(&cfg_path, toml).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_solid-engine-server"))
            .arg("--config")
            .arg(&cfg_path)
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        // Le socket n'est lié qu'une fois la récupération terminée.
        let deadline = Instant::now() + Duration::from_secs(10);
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

#[test]
fn sigkill_keeps_confirmed_pushes_and_fences_the_old_worker() {
    let dir = temp_dir("sigkill");
    let daemon = Daemon::start(&dir);
    let mut c = daemon.client();
    for id in 1..=20u128 {
        assert!(matches!(c.call(&push(id)).unwrap(), Response::Push(_)));
    }
    let Response::Claim(Some(old)) =
        c.call(&Request::ClaimNextJob { channel_id: 1, lease_ms: 600_000, wait_ms: 0 }).unwrap()
    else {
        panic!()
    };
    let Response::Claim(Some(acked)) =
        c.call(&Request::ClaimNextJob { channel_id: 1, lease_ms: 600_000, wait_ms: 0 }).unwrap()
    else {
        panic!()
    };
    assert_eq!(c.call(&Request::AckJob(reservation(&acked))).unwrap(), Response::Ack);
    daemon.sigkill();

    // Redémarrages répétés après un SIGKILL.
    for round in 0..2 {
        let daemon = Daemon::start(&dir);
        let mut c = daemon.client();
        let Response::Stats(s) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
        assert_eq!((s.ready, s.claimed, s.completed), (19, 0, 1), "round {round}");
        if round == 0 {
            // Le job réservé avant le crash est de nouveau disponible ; l'ancien worker est rejeté.
            let Response::Error(e) = c.call(&Request::AckJob(reservation(&old))).unwrap() else { panic!() };
            assert_eq!(e.code, ErrorCode::StaleReservation);
            let Response::Job(v) = c.call(&Request::GetJob { job_id: old.job_id }).unwrap() else { panic!() };
            assert_eq!((v.state, v.attempts, v.fencing_token), (JobState::Ready, 1, old.fencing_token));
        }
        daemon.sigkill();
    }
}
