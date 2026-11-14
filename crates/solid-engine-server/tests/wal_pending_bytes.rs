//! Budget d'octets WAL en attente (SOLID_MAX_PENDING_WAL_BYTES). Fichier dédié : le plafond est lu une fois
//! par processus et les compteurs sont globaux.
use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::engine::{pending_wal_bytes, BP_WAIT_US, BYTES_BATCH_MAX, BYTES_PEND_MAX};
use solid_engine_server::Server;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

const CAP: u64 = 65_536;

fn push(id: u128, len: usize) -> Request {
    Request::PushJob(PushRequest {
        job_id: id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: vec![],
        payload: vec![7; len],
    })
}

fn start() -> Server {
    std::env::set_var("SOLID_MAX_PENDING_WAL_BYTES", CAP.to_string());
    let dir = std::env::temp_dir().join(format!(
        "solid-engine-pwb-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let mut cfg = Config::default();
    cfg.engine.shards = 2;
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = Durability::Balanced;
    cfg.storage.sync_interval_ms = 5;
    cfg.storage.data_dir = Some(dir.to_string_lossy().into());
    Server::start(cfg).unwrap()
}

fn settled() -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if (0..2).all(|s| pending_wal_bytes(s) == 0) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

#[test]
fn pending_bytes_return_to_zero_and_budget_throttles_large_payloads() {
    let server = start();
    let addr = server.tcp_addr().unwrap();
    // Peu de records, gros payloads (64 o à 128 Ko), plusieurs clients : le plafond de 64 Ko est franchi
    // bien avant tout plafond en nombre de records.
    let sizes = [64usize, 1_000, 16_384, 65_536, 131_072];
    let threads: Vec<_> = (0..4u128)
        .map(|t| {
            std::thread::spawn(move || {
                let mut c = Client::connect_tcp(addr).unwrap();
                for i in 0..40u128 {
                    let id = (t + 1) * 1_000 + i;
                    let r = c
                        .call(&push(id, sizes[(i as usize + t as usize) % sizes.len()]))
                        .unwrap();
                    assert!(matches!(r, Response::Push(_)), "push {id}");
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert!(
        settled(),
        "pending_wal_bytes ne revient pas à 0 : {:?}",
        (pending_wal_bytes(0), pending_wal_bytes(1))
    );
    assert!(
        BP_WAIT_US.load(Relaxed) > 0,
        "le budget n'a jamais suspendu l'envoi"
    );
    // Plafond souple : au plus une fournée au-delà du budget.
    let (peak, batch) = (BYTES_PEND_MAX.load(Relaxed), BYTES_BATCH_MAX.load(Relaxed));
    assert!(
        peak <= CAP + batch,
        "retard {peak} > budget {CAP} + fournée {batch}"
    );
    server.shutdown();
}
