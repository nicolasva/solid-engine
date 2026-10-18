//! Arrêt du serveur pendant une réduction de la table `done` en attente (et un trim mémoire demandé).
use solid_engine_core::config::Config;
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use std::time::{Duration, Instant};

const JOBS: u128 = 30_000;

fn run_one(stop_after: Duration) -> Duration {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.engine.shards = 1;
    cfg.jobs.completed_retention_ms = 300;
    let server = Server::start(cfg).unwrap();
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    for id in 1..=JOBS {
        let req = Request::PushJob(PushRequest {
            job_id: id,
            channel_id: 1,
            priority: 0,
            max_attempts: 0,
            delay_ms: 0,
            partition_key: vec![],
            payload: vec![1],
        });
        assert!(matches!(c.call(&req).unwrap(), Response::Push(_)));
        let Response::Claim(Some(j)) = c.call(&Request::ClaimNextJob { channel_id: 1, lease_ms: 0, wait_ms: 0 }).unwrap() else { panic!() };
        let r = Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token };
        assert_eq!(c.call(&Request::AckJob(r)).unwrap(), Response::Ack);
    }
    drop(c);
    // La rétention est de 300 ms : on arrête pendant que les expirations et la réduction se déroulent.
    std::thread::sleep(Duration::from_millis(300) + stop_after);
    solid_engine_server::stats::trim_memory();
    let t = Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        server.shutdown();
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_secs(5)).expect("l'arrêt ne doit pas bloquer pendant une réduction");
    t.elapsed()
}

#[test]
fn shutdown_during_pending_shrink_is_bounded() {
    for ms in [0, 2, 10, 50, 200] {
        let took = run_one(Duration::from_millis(ms));
        println!("arrêt {ms} ms après la fin de rétention : {took:?}");
        assert!(took < Duration::from_secs(2));
    }
}
