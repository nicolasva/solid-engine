//! Injection d'erreurs d'I/O WAL. Fichier dédié : le drapeau d'injection est global au processus.
use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use solid_engine_storage::INJECT_IO_ERROR;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

fn push(id: u128) -> Request {
    Request::PushJob(PushRequest {
        job_id: id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: vec![],
        payload: vec![1],
    })
}

fn start(durability: Durability, tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("solid-engine-fault-{tag}-{}-{}", std::process::id(), now_ms()));
    let mut cfg = Config::default();
    cfg.engine.shards = 1;
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = durability;
    cfg.storage.sync_interval_ms = 5;
    cfg.storage.data_dir = Some(dir.to_string_lossy().into());
    Server::start(cfg).unwrap()
}

fn is_internal(r: &Response) -> bool {
    matches!(r, Response::Error(e) if e.code == ErrorCode::Internal)
}

#[test]
fn wal_io_errors_fail_the_shard_for_good() {
    // strict : l'erreur est signalée sur la requête elle-même, jamais de faux succès.
    let server = start(Durability::Strict, "strict");
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    assert!(matches!(c.call(&push(1)).unwrap(), Response::Push(_)));
    INJECT_IO_ERROR.store(true, Ordering::SeqCst);
    assert!(is_internal(&c.call(&push(2)).unwrap()));
    // L'état d'échec est définitif, même si le disque « revient ».
    INJECT_IO_ERROR.store(false, Ordering::SeqCst);
    assert!(is_internal(&c.call(&push(3)).unwrap()));
    assert!(is_internal(&c.call(&Request::GetQueueStats { channel_id: None }).unwrap()));
    server.shutdown();

    // balanced : l'erreur est détectée de façon asynchrone ; fenêtre bornée, puis échec définitif.
    let server = start(Durability::Balanced, "balanced");
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    assert!(matches!(c.call(&push(1)).unwrap(), Response::Push(_)));
    INJECT_IO_ERROR.store(true, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut id = 2;
    loop {
        if is_internal(&c.call(&push(id)).unwrap()) {
            break;
        }
        assert!(Instant::now() < deadline, "balanced shard never reported the WAL failure");
        id += 1;
        std::thread::sleep(Duration::from_millis(2));
    }
    INJECT_IO_ERROR.store(false, Ordering::SeqCst);
    assert!(is_internal(&c.call(&push(1_000)).unwrap()));
    server.shutdown();
}
