//! Les compteurs WAL par shard sont associés au bon shard au-delà de 16 shards (aucune fusion des shards 15+).
//! Fichier dédié : les compteurs sont globaux au processus.
use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::engine::{pending_wal_bytes, BYTES_SENT, BYTES_WRITTEN, LAG_SENT, LAG_WRITTEN};
use solid_engine_server::Server;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

const SHARDS: usize = 32;

#[test]
fn counters_follow_their_shard_beyond_16() {
    let dir = std::env::temp_dir().join(format!("solid-engine-ctr-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut cfg = Config::default();
    cfg.engine.shards = SHARDS;
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = Durability::Balanced;
    cfg.storage.sync_interval_ms = 5;
    cfg.storage.data_dir = Some(dir.to_string_lossy().into());
    let server = Server::start(cfg).unwrap();
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();

    let snapshot = || -> Vec<(u64, u64)> { (0..SHARDS).map(|s| (BYTES_SENT[s].load(Relaxed), LAG_SENT[s].load(Relaxed))).collect() };
    let before = snapshot();
    // 10 jobs sur le shard 16, 20 sur le shard 31, 5 sur le shard 15 ; aucun ailleurs.
    let plan = [(15usize, 5usize), (16, 10), (31, 20)];
    let mut next = 1u128;
    for (shard, count) in plan {
        let mut sent = 0;
        while sent < count {
            let id = next;
            next += 1;
            if shard_for(1, &id.to_le_bytes(), SHARDS) != shard {
                continue;
            }
            let r = c
                .call(&Request::PushJob(PushRequest {
                    job_id: id,
                    channel_id: 1,
                    priority: 0,
                    max_attempts: 0,
                    delay_ms: 0,
                    partition_key: vec![],
                    payload: vec![1; 200],
                }))
                .unwrap();
            assert!(matches!(r, Response::Push(_)));
            sent += 1;
        }
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while (0..SHARDS).any(|s| pending_wal_bytes(s) != 0) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let after = snapshot();
    let delta = |s: usize| (after[s].0 - before[s].0, after[s].1 - before[s].1);
    for s in 0..SHARDS {
        match s {
            15 | 16 | 31 => {}
            _ => assert_eq!(delta(s), (0, 0), "le shard {s} n'a rien reçu"),
        }
    }
    // Un enregistrement par job, tous de même taille : octets proportionnels au nombre de jobs de chaque shard.
    assert_eq!(delta(15).1, 5);
    assert_eq!(delta(16).1, 10);
    assert_eq!(delta(31).1, 20);
    let unit = delta(15).0 / 5;
    assert!(unit >= 200, "taille d'une trame : {unit}");
    assert_eq!(delta(16).0, unit * 10);
    assert_eq!(delta(31).0, unit * 20);
    for s in [15, 16, 31] {
        assert_eq!(pending_wal_bytes(s), 0, "shard {s}");
        assert_eq!(BYTES_WRITTEN[s].load(Relaxed), BYTES_SENT[s].load(Relaxed), "shard {s}");
        assert_eq!(LAG_WRITTEN[s].load(Relaxed), LAG_SENT[s].load(Relaxed), "shard {s}");
    }
    drop(c);
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
