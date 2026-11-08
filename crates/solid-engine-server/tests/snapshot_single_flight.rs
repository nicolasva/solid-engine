//! Un seul snapshot en vol par shard. Fichier dédié : les interrupteurs d'injection sont globaux au processus,
//! les tests sont donc sérialisés.
use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::engine::{SNAPSHOTS_IN_FLIGHT, SNAPSHOTS_IN_FLIGHT_MAX};
use solid_engine_server::Server;
use solid_engine_storage::{INJECT_IO_ERROR, SNAPSHOT_DELAY_MS};
use std::path::PathBuf;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Mutex;
use std::time::{Duration, Instant};

static SERIAL: Mutex<()> = Mutex::new(());

fn temp_dir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("solid-engine-sf-{tag}-{}-{}", std::process::id(), now_ms()));
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

fn config(dir: &std::path::Path, shards: usize, single_flight: bool) -> Config {
    let mut cfg = Config::default();
    cfg.engine.shards = shards;
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = Durability::Balanced;
    cfg.storage.sync_interval_ms = 5;
    cfg.storage.data_dir = Some(dir.join("data").to_string_lossy().into());
    cfg.storage.snapshot_every_records = 20;
    cfg.storage.snapshot_single_flight = single_flight;
    cfg
}

fn reset_hooks() {
    INJECT_IO_ERROR.store(false, SeqCst);
    SNAPSHOT_DELAY_MS.store(0, SeqCst);
    let deadline = Instant::now() + Duration::from_secs(10);
    while SNAPSHOTS_IN_FLIGHT.load(SeqCst) != 0 {
        assert!(Instant::now() < deadline, "snapshots d'un test précédent toujours en vol");
        std::thread::sleep(Duration::from_millis(5));
    }
    SNAPSHOTS_IN_FLIGHT_MAX.store(0, SeqCst);
}

fn job_state(c: &mut Client<std::net::TcpStream>, id: u128) -> Option<JobState> {
    match c.call(&Request::GetJob { job_id: id }).unwrap() {
        Response::Job(v) => Some(v.state),
        _ => None,
    }
}

/// Writer lent, producteur rapide : sans single-flight les snapshots s'empilent, avec il n'y en a jamais plus d'un par shard.
#[test]
fn slow_writer_keeps_at_most_one_snapshot_in_flight_per_shard() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for (single_flight, tag) in [(false, "pile"), (true, "single")] {
        reset_hooks();
        SNAPSHOT_DELAY_MS.store(100, SeqCst);
        let dir = temp_dir(tag);
        let server = Server::start(config(&dir, 1, single_flight)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        for id in 1..=400u128 {
            assert!(matches!(c.call(&push(id)).unwrap(), Response::Push(_)));
        }
        let max = SNAPSHOTS_IN_FLIGHT_MAX.load(SeqCst);
        SNAPSHOT_DELAY_MS.store(0, SeqCst);
        server.shutdown();
        if single_flight {
            assert_eq!(max, 1, "un seul snapshot en vol attendu");
        } else {
            assert!(max > 1, "le témoin sans single-flight doit empiler des snapshots (max={max})");
        }
    }
    reset_hooks();
}

/// Des mutations (pushes, annulations) arrivent pendant que le writer écrit lentement : le prochain snapshot et le WAL
/// restant doivent reconstituer exactement l'état, ce qui n'est vrai que si la séquence de chaque snapshot
/// est celle de l'état encodé.
#[test]
fn mutations_during_slow_snapshots_survive_restart() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    reset_hooks();
    SNAPSHOT_DELAY_MS.store(60, SeqCst);
    let dir = temp_dir("mutations");
    {
        let server = Server::start(config(&dir, 2, true)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        for id in 1..=300u128 {
            assert!(matches!(c.call(&push(id)).unwrap(), Response::Push(_)));
            if id > 60 && id % 3 == 0 {
                assert_eq!(c.call(&Request::CancelJob { job_id: id - 60 }).unwrap(), Response::Cancel);
            }
        }
        SNAPSHOT_DELAY_MS.store(0, SeqCst);
        server.shutdown();
    }
    assert!(SNAPSHOTS_IN_FLIGHT_MAX.load(SeqCst) <= 2, "au plus un snapshot en vol par shard (2 shards)");
    for _ in 0..2 {
        let server = Server::start(config(&dir, 2, true)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        for id in 1..=300u128 {
            let cancelled = id <= 240 && (id + 60) % 3 == 0;
            let expected = if cancelled { JobState::Cancelled } else { JobState::Ready };
            assert_eq!(job_state(&mut c, id), Some(expected), "job {id}");
        }
        server.shutdown();
    }
}

/// Échec d'écriture d'un snapshot : le shard passe en erreur, le drapeau « en vol » est libéré,
/// et les jobs déjà synchronisés restent récupérables (aucune troncature du WAL au-delà de l'état persisté).
#[test]
fn failed_snapshot_fails_the_shard_and_keeps_synced_jobs() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    reset_hooks();
    let dir = temp_dir("failure");
    {
        let server = Server::start(config(&dir, 1, true)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        for id in 1..=20u128 {
            assert!(matches!(c.call(&push(id)).unwrap(), Response::Push(_)));
        }
        // Le fsync périodique rend les 20 premiers jobs durables avant la panne.
        std::thread::sleep(Duration::from_millis(200));
        SNAPSHOT_DELAY_MS.store(300, SeqCst);
        for id in 21..=45u128 {
            assert!(matches!(c.call(&push(id)).unwrap(), Response::Push(_)));
        }
        // Un snapshot est en vol, bloqué dans le délai : la panne le fait échouer.
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(SNAPSHOTS_IN_FLIGHT.load(SeqCst), 1);
        INJECT_IO_ERROR.store(true, SeqCst);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut id = 46;
        loop {
            let failed = matches!(c.call(&push(id)).unwrap(), Response::Error(e) if e.code == ErrorCode::Internal);
            if failed {
                break;
            }
            assert!(Instant::now() < deadline, "l'échec du snapshot n'a pas fait passer le shard en erreur");
            id += 1;
            std::thread::sleep(Duration::from_millis(10));
        }
        INJECT_IO_ERROR.store(false, SeqCst);
        SNAPSHOT_DELAY_MS.store(0, SeqCst);
        let deadline = Instant::now() + Duration::from_secs(5);
        while SNAPSHOTS_IN_FLIGHT.load(SeqCst) != 0 {
            assert!(Instant::now() < deadline, "drapeau en vol non libéré après échec");
            std::thread::sleep(Duration::from_millis(5));
        }
        // L'état d'échec est définitif, même une fois le disque revenu.
        assert!(matches!(c.call(&push(id + 1)).unwrap(), Response::Error(e) if e.code == ErrorCode::Internal));
        server.shutdown();
    }
    let server = Server::start(config(&dir, 1, true)).unwrap();
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    for id in 1..=20u128 {
        assert_eq!(job_state(&mut c, id), Some(JobState::Ready), "job synchronisé {id} perdu");
    }
    server.shutdown();
    reset_hooks();
}
