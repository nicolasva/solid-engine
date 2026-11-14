//! Le nombre de shards ne doit pas être limité par les compteurs WAL statiques : 16, 17 et 32 shards dans les
//! trois modes (démarrage, push, réservation, ACK, lecture, arrêt propre, redémarrage avec récupération).
//! Avec `--features wal-diagnostics` (et éventuellement `SOLID_ENGINE_WAL_LAG=1`), les mêmes tests couvrent les
//! compteurs de diagnostic.
use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use std::collections::BTreeSet;

fn config(mode: Durability, shards: usize, dir: &std::path::Path) -> Config {
    let mut cfg = Config::default();
    cfg.engine.shards = shards;
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = mode;
    cfg.storage.sync_interval_ms = 5;
    if mode != Durability::Memory {
        cfg.storage.data_dir = Some(dir.to_string_lossy().into());
    }
    cfg
}

/// `count` identifiants (à partir de `*next`) que le routage envoie au shard `shard`.
fn ids_for(shard: usize, shards: usize, count: usize, next: &mut u128) -> Vec<u128> {
    let mut v = Vec::new();
    while v.len() < count {
        if shard_for(1, &next.to_le_bytes(), shards) == shard {
            v.push(*next);
        }
        *next += 1;
    }
    v
}

fn push(c: &mut Client<std::net::TcpStream>, id: u128) {
    let r = c
        .call(&Request::PushJob(PushRequest {
            job_id: id,
            channel_id: 1,
            priority: 0,
            max_attempts: 0,
            delay_ms: 0,
            partition_key: vec![],
            payload: vec![3; 100],
        }))
        .unwrap();
    assert!(matches!(r, Response::Push(_)), "push {id}: {r:?}");
}

fn state(c: &mut Client<std::net::TcpStream>, id: u128) -> JobState {
    match c.call(&Request::GetJob { job_id: id }).unwrap() {
        Response::Job(v) => v.state,
        r => panic!("get {id}: {r:?}"),
    }
}

fn run(mode: Durability, shards: usize) {
    let dir = std::env::temp_dir().join(format!("solid-engine-sc-{}-{:?}-{shards}", std::process::id(), mode));
    let _ = std::fs::remove_dir_all(&dir);
    // Premier, dernier et, quand ils existent, shards 16 et 31.
    let targets: BTreeSet<usize> = [0, 16, 31, shards - 1].into_iter().filter(|s| *s < shards).collect();
    let mut next = 1u128;
    let mut done = Vec::new();
    let mut ready = Vec::new();

    let server = Server::start(config(mode, shards, &dir)).expect("démarrage");
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    for &s in &targets {
        for id in ids_for(s, shards, 3, &mut next) {
            push(&mut c, id);
            done.push(id);
        }
    }
    let mut got = BTreeSet::new();
    let mut empty = 0;
    while got.len() < done.len() && empty < 20 {
        match c
            .call(&Request::ClaimBatch { channel_id: 1, lease_ms: 30_000, max: 64, wait_ms: 100 })
            .unwrap()
        {
            Response::ClaimBatch(js) if !js.is_empty() => {
                let res: Vec<Reservation> = js
                    .iter()
                    .map(|j| Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token })
                    .collect();
                got.extend(js.iter().map(|j| j.job_id));
                match c.call(&Request::AckBatch(res)).unwrap() {
                    Response::AckBatch(v) => assert!(v.iter().all(Option::is_none), "{v:?}"),
                    r => panic!("{r:?}"),
                }
            }
            _ => empty += 1,
        }
    }
    assert_eq!(got, done.iter().copied().collect::<BTreeSet<_>>());
    for &id in &done {
        assert_eq!(state(&mut c, id), JobState::Completed, "job {id}");
    }
    // Jobs laissés disponibles : ils doivent survivre à l'arrêt et au redémarrage.
    for &s in &targets {
        for id in ids_for(s, shards, 2, &mut next) {
            push(&mut c, id);
            ready.push(id);
        }
    }
    drop(c);
    server.shutdown();
    if mode == Durability::Memory {
        return;
    }

    for &s in &targets {
        let seg: u64 = std::fs::read_dir(dir.join(format!("shard-{s}")))
            .unwrap_or_else(|e| panic!("répertoire WAL du shard {s} : {e}"))
            .flatten()
            .map(|e| e.metadata().unwrap().len())
            .sum();
        assert!(seg > 0, "WAL du shard {s} vide");
    }
    let server = Server::start(config(mode, shards, &dir)).expect("redémarrage");
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    for &id in &done {
        assert_eq!(state(&mut c, id), JobState::Completed, "après redémarrage, job {id}");
    }
    for &id in &ready {
        assert_eq!(state(&mut c, id), JobState::Ready, "après redémarrage, job {id}");
    }
    drop(c);
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

macro_rules! cases {
    ($($name:ident: $mode:expr, $n:expr;)*) => {
        $(#[test] fn $name() { run($mode, $n); })*
    };
}

cases! {
    memory_16: Durability::Memory, 16;
    memory_17: Durability::Memory, 17;
    memory_32: Durability::Memory, 32;
    balanced_16: Durability::Balanced, 16;
    balanced_17: Durability::Balanced, 17;
    balanced_32: Durability::Balanced, 32;
    strict_16: Durability::Strict, 16;
    strict_17: Durability::Strict, 17;
    strict_32: Durability::Strict, 32;
}
