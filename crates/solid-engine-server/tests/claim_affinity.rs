//! Affinité de claim (SOLID_ENGINE_CLAIM_AFFINITY) : aucun job ne doit être perdu, dupliqué ni affamé.
use solid_engine_core::config::Config;
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use std::collections::HashSet;

fn push(id: u128, partition: &[u8]) -> Request {
    Request::PushJob(PushRequest {
        job_id: id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: partition.to_vec(),
        payload: vec![1],
    })
}

fn claim(c: &mut Client<std::net::TcpStream>) -> Option<ClaimedJob> {
    match c.call(&Request::ClaimNextJob { channel_id: 1, lease_ms: 30_000, wait_ms: 0 }).unwrap() {
        Response::Claim(j) => j,
        other => panic!("claim: {other:?}"),
    }
}

fn ack(c: &mut Client<std::net::TcpStream>, j: &ClaimedJob) {
    let r = Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token };
    assert!(matches!(c.call(&Request::AckJob(r)).unwrap(), Response::Ack));
}

#[test]
fn consumers_reach_jobs_pushed_elsewhere_and_a_busy_connection_does_not_starve_them() {
    std::env::set_var("SOLID_ENGINE_CLAIM_AFFINITY", "1");
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    let server = Server::start(cfg).unwrap();
    let addr = server.tcp_addr().unwrap().to_string();

    // A produit 64 jobs, uniquement sur des shards autres que celui de C, puis se tait.
    let shards = Config::default().engine.shards;
    let c_shard = shard_for(1, b"c", shards);
    let foreign: Vec<u8> = (0..=255u8).filter(|k| shard_for(1, &[*k], shards) != c_shard).take(8).collect();
    let mut a = Client::connect_tcp(&addr).unwrap();
    for id in 1..=64u128 {
        let key = [foreign[(id % 8) as usize]];
        assert!(matches!(a.call(&push(id, &key)).unwrap(), Response::Push(_)));
    }

    // C pousse et consomme en boucle dans sa propre partition : sans balayage tournant,
    // il ne verrait jamais les jobs de A.
    let mut c = Client::connect_tcp(&addr).unwrap();
    let mut seen = HashSet::new();
    let mut foreign_by_c = 0;
    for i in 0..200u128 {
        let own = 1000 + i;
        assert!(matches!(c.call(&push(own, b"c")).unwrap(), Response::Push(_)));
        let j = loop {
            if let Some(j) = claim(&mut c) {
                break j;
            }
        };
        assert!(seen.insert(j.job_id), "job {} livré deux fois", j.job_id);
        if j.job_id <= 64 {
            foreign_by_c += 1;
        }
        ack(&mut c, &j);
    }
    assert!(foreign_by_c > 0, "C n'a jamais atteint les jobs des autres partitions");

    // B ne pousse jamais : il doit trouver tout le reste, quel que soit le shard.
    let mut b = Client::connect_tcp(&addr).unwrap();
    while let Some(j) = claim(&mut b) {
        assert!(seen.insert(j.job_id), "job {} livré deux fois", j.job_id);
        ack(&mut b, &j);
    }
    let a_jobs: HashSet<u128> = seen.iter().copied().filter(|id| *id <= 64).collect();
    assert_eq!(a_jobs.len(), 64, "jobs de A perdus : {}", 64 - a_jobs.len());
    server.shutdown();
}
