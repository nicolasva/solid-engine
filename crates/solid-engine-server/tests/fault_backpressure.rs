//! camp31 : le budget d'octets WAL ne doit ni bloquer le shard ni masquer une erreur du writer.
//! Fichier dédié : le drapeau d'injection et la variable de budget sont globaux au processus.
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
        payload: vec![1; 64],
    })
}

fn start() -> Server {
    // Budget d'un octet : chaque fournée attend que la précédente soit écrite (cas le plus contraint).
    std::env::set_var("SOLID_ENGINE_WAL_MAX_PENDING_BYTES", "1");
    let dir = std::env::temp_dir().join(format!("solid-engine-bytes-{}-{}", std::process::id(), now_ms()));
    let mut cfg = Config::default();
    cfg.engine.shards = 2;
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = Durability::Balanced;
    cfg.storage.sync_interval_ms = 5;
    cfg.storage.data_dir = Some(dir.to_string_lossy().into());
    Server::start(cfg).unwrap()
}

#[test]
fn byte_budget_makes_progress_then_reports_writer_failure() {
    let server = start();
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    let t0 = Instant::now();
    for id in 1..=2_000u128 {
        assert!(matches!(c.call(&push(id)).unwrap(), Response::Push(_)), "push {id}");
    }
    assert!(t0.elapsed() < Duration::from_secs(30), "le budget bloque la progression");

    INJECT_IO_ERROR.store(true, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut id = 10_000u128;
    loop {
        match c.call(&push(id)).unwrap() {
            Response::Error(e) if e.code == ErrorCode::Internal => break,
            _ => {}
        }
        assert!(Instant::now() < deadline, "l'attente du budget masque l'erreur du writer");
        id += 1;
    }
    INJECT_IO_ERROR.store(false, Ordering::SeqCst);
    server.shutdown();
}
