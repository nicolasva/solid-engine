//! camp32 : cycle de vie complet à travers le serveur (TCP, shards, writer WAL partagé), avec redémarrages.
//! Retry, Nack, Extend, expiration, ACK tardif et réservations d'une ancienne incarnation.

use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

const N: u128 = 40;

fn temp_dir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("solid-engine-{tag}-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn config(dir: &PathBuf, durability: Durability, shards: usize) -> Config {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.engine.shards = shards;
    cfg.storage.durability = durability;
    cfg.storage.data_dir = Some(dir.join("data").to_string_lossy().into());
    cfg.storage.snapshot_every_records = 25;
    cfg
}

fn res(j: &ClaimedJob) -> Reservation {
    Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token }
}

fn states(c: &mut Client<std::net::TcpStream>) -> BTreeMap<u128, (JobState, u32)> {
    (1..=N)
        .map(|id| {
            let Response::Job(v) = c.call(&Request::GetJob { job_id: id }).unwrap() else { panic!("job {id} introuvable") };
            (id, (v.state, v.attempts))
        })
        .collect()
}

fn claim_all(c: &mut Client<std::net::TcpStream>, lease_ms: u32) -> Vec<ClaimedJob> {
    let mut v = vec![];
    while let Response::Claim(Some(j)) = c.call(&Request::ClaimNextJob { channel_id: 1, lease_ms, wait_ms: 0 }).unwrap() {
        v.push(j);
    }
    v
}

fn is_ready_or_scheduled(s: JobState) -> bool {
    matches!(s, JobState::Ready | JobState::Scheduled)
}

#[test]
fn lifecycle_survives_restarts_multishard() {
    for (tag, mode, shards) in
        [("lc-strict-2", Durability::Strict, 2), ("lc-balanced-2", Durability::Balanced, 2), ("lc-strict-4", Durability::Strict, 4)]
    {
        run(tag, mode, shards);
    }
}

fn run(tag: &str, mode: Durability, shards: usize) {
    let dir = temp_dir(tag);
    let (late, stale_ids);
    {
        let server = Server::start(config(&dir, mode, shards)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        for id in 1..=N {
            let p = PushRequest { job_id: id, channel_id: 1, priority: 0, max_attempts: 2, delay_ms: 0, partition_key: vec![], payload: vec![1; 8] };
            assert!(matches!(c.call(&Request::PushJob(p)).unwrap(), Response::Push(_)), "{tag}: push {id}");
        }
        let claimed = claim_all(&mut c, 400);
        assert_eq!(claimed.len() as u128, N, "{tag}: tous les jobs sont réclamables");
        let mut expiring = vec![];
        let mut kept = vec![];
        for (i, j) in claimed.iter().enumerate() {
            let r = res(j);
            match i % 5 {
                0 => assert_eq!(c.call(&Request::AckJob(r)).unwrap(), Response::Ack),
                1 => assert_eq!(c.call(&Request::NackJob { reservation: r, kind: NackKind::Fail }).unwrap(), Response::Nack),
                2 => assert_eq!(c.call(&Request::NackJob { reservation: r, kind: NackKind::Retry { delay_ms: Some(0) } }).unwrap(), Response::Nack),
                3 => {
                    assert!(matches!(c.call(&Request::ExtendLease { reservation: r, lease_ms: 600_000 }).unwrap(), Response::ExtendLease { .. }));
                    kept.push(j.job_id);
                }
                _ => expiring.push(j.clone()),
            }
        }
        std::thread::sleep(Duration::from_millis(700));
        // ACK tardif : le bail a expiré, la réservation est périmée, même sans redémarrage.
        for j in &expiring {
            assert!(matches!(c.call(&Request::AckJob(res(j))).unwrap(), Response::Error(_)), "{tag}: ack tardif de {} accepté", j.job_id);
        }
        let s = states(&mut c);
        for (i, j) in claimed.iter().enumerate() {
            let (st, att) = s[&j.job_id];
            match i % 5 {
                0 => assert_eq!((st, att), (JobState::Completed, 1)),
                1 => assert_eq!((st, att), (JobState::Dead, 1)),
                2 => assert!(is_ready_or_scheduled(st) && att == 1, "{tag}: nack retry {} → {st:?}", j.job_id),
                3 => assert_eq!((st, att), (JobState::Claimed, 1), "{tag}: bail prolongé perdu"),
                _ => assert!(is_ready_or_scheduled(st) && att == 1, "{tag}: bail expiré {} → {st:?}", j.job_id),
            }
        }
        late = expiring;
        stale_ids = kept;
        server.shutdown();
    }
    // Redémarrage : les réservations de l'ancienne incarnation sont invalidées (jobs prolongés inclus).
    let after_first = {
        let server = Server::start(config(&dir, mode, shards)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        let s = states(&mut c);
        for id in &stale_ids {
            assert!(is_ready_or_scheduled(s[id].0) && s[id].1 == 1, "{tag}: réservation prolongée non invalidée pour {id}");
        }
        for j in &late {
            assert!(matches!(c.call(&Request::AckJob(res(j))).unwrap(), Response::Error(_)), "{tag}: ack périmé accepté après redémarrage");
        }
        // Deuxième tentative : tous les jobs relançables sont réclamés puis rejetés → Dead (max_attempts = 2).
        let again = claim_all(&mut c, 60_000);
        assert!(!again.is_empty());
        for j in &again {
            assert_eq!(j.attempts, 2, "{tag}: tentative {}", j.job_id);
            assert_eq!(c.call(&Request::NackJob { reservation: res(j), kind: NackKind::Retry { delay_ms: Some(0) } }).unwrap(), Response::Nack);
        }
        let s = states(&mut c);
        server.shutdown();
        s
    };
    for id in 1..=N {
        let (st, att) = after_first[&id];
        if att == 2 {
            assert_eq!(st, JobState::Dead, "{tag}: job {id} épuisé doit être Dead");
        }
    }
    // Deux redémarrages supplémentaires : l'état est stable et identique.
    for _ in 0..2 {
        let server = Server::start(config(&dir, mode, shards)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        assert_eq!(states(&mut c), after_first, "{tag}: état instable après redémarrage");
        server.shutdown();
    }
}
