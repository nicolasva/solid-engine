//! camp34 phase 3 : la limitation globale de snapshots simultanés est respectée, équitable, sans blocage,
//! et la récupération après les snapshots reportés est complète. Fichier dédié : la limite est lue une fois par processus.
use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::engine::{SNAPSHOTS_IN_FLIGHT_MAX, SNAP_DONE_BY_SHARD, SNAP_ENCODING_MAX, SNAP_GATE_ACTIVE, SNAP_GATE_DEFERRED};
use solid_engine_storage::{INJECT_IO_ERROR, SNAPSHOT_DELAY_MS};
use solid_engine_server::Server;
use std::sync::atomic::Ordering::SeqCst;

fn config(dir: &std::path::Path) -> Config {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.engine.shards = 4;
    cfg.storage.durability = Durability::Balanced;
    cfg.storage.sync_interval_ms = 5;
    cfg.storage.data_dir = Some(dir.join("data").to_string_lossy().into());
    cfg.storage.snapshot_every_records = 50;
    cfg
}

#[test]
fn global_snapshot_limit_is_respected_fair_and_recoverable() {
    std::env::set_var("SOLID_ENGINE_SNAPSHOT_MAX_CONCURRENT", "1");
    let dir = std::env::temp_dir().join(format!("solid-engine-gate-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let n = 8000u128;
    {
        let server = Server::start(config(&dir)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        for id in 1..=n {
            let p = PushRequest { job_id: id, channel_id: (id % 7) as u16, priority: 0, max_attempts: 0, delay_ms: 0, partition_key: vec![], payload: vec![3; 64] };
            assert!(matches!(c.call(&Request::PushJob(p)).unwrap(), Response::Push(_)), "push {id}");
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
        server.shutdown();
    }
    assert!(SNAPSHOTS_IN_FLIGHT_MAX.load(SeqCst) <= 1, "plus d'un snapshot simultané : {}", SNAPSHOTS_IN_FLIGHT_MAX.load(SeqCst));
    assert!(SNAP_ENCODING_MAX.load(SeqCst) <= 1);
    let done: Vec<usize> = (0..4).map(|i| SNAP_DONE_BY_SHARD[i].load(SeqCst)).collect();
    assert!(done.iter().all(|&d| d >= 1), "un shard n'a jamais obtenu de snapshot (famine) : {done:?} ; reportés {}", SNAP_GATE_DEFERRED.load(SeqCst));

    let server = Server::start(config(&dir)).unwrap();
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    for id in 1..=n {
        assert!(matches!(c.call(&Request::GetJob { job_id: id }).unwrap(), Response::Job(_)), "job {id} perdu après redémarrage");
    }
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);

    // Erreur d'écriture pendant des snapshots lents : toutes les autorisations doivent être rendues, sans blocage.
    let dir = std::env::temp_dir().join(format!("solid-engine-gate-err-{}-{}", std::process::id(), now_ms()));
    std::fs::create_dir_all(&dir).unwrap();
    let server = Server::start(config(&dir)).unwrap();
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    SNAPSHOT_DELAY_MS.store(20, SeqCst);
    let mut errored = false;
    for id in 1..=3000u128 {
        if id == 500 {
            INJECT_IO_ERROR.store(true, SeqCst);
        }
        let p = PushRequest { job_id: id, channel_id: 1, priority: 0, max_attempts: 0, delay_ms: 0, partition_key: vec![], payload: vec![3; 64] };
        if !matches!(c.call(&Request::PushJob(p)), Ok(Response::Push(_))) {
            errored = true;
            break;
        }
    }
    INJECT_IO_ERROR.store(false, SeqCst);
    SNAPSHOT_DELAY_MS.store(0, SeqCst);
    assert!(errored, "l'erreur injectée n'a pas été propagée");
    let t = std::time::Instant::now();
    while SNAP_GATE_ACTIVE.load(SeqCst) != 0 {
        assert!(t.elapsed() < std::time::Duration::from_secs(10), "autorisation de snapshot jamais rendue : {}", SNAP_GATE_ACTIVE.load(SeqCst));
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
