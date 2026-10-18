//! Plancher de coût d'un shard : push + claim + ack appelés directement (un thread, sans WAL, canal ni réseau).
//! Usage : cargo run --release -p solid-engine-core --example shard_floor [jobs] [fenêtre]
use solid_engine_core::config::Config;
use solid_engine_core::shard::Shard;
use solid_engine_core::{PushRequest, Reservation};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let jobs: u64 = std::env::args().nth(1).and_then(|v| v.parse().ok()).unwrap_or(2_000_000);
    let window: usize = std::env::args().nth(2).and_then(|v| v.parse().ok()).unwrap_or(8 * 8);
    let mut cfg = Config::default();
    cfg.limits.max_jobs_per_shard = 10_000_000;
    let mut shard = Shard::new(0, Arc::new(cfg));
    let payload = vec![7u8; 256];
    let mut held: Vec<Reservation> = Vec::with_capacity(window);
    let (mut next, mut done) = (1u128, 0u64);
    let start = Instant::now();
    while done < jobs {
        let now = 1_000 + start.elapsed().as_millis() as u64;
        for _ in 0..window {
            let req = PushRequest {
                job_id: next,
                channel_id: 1,
                priority: 0,
                max_attempts: 0,
                delay_ms: 0,
                partition_key: Vec::new(),
                payload: payload.clone(),
            };
            next += 1;
            shard.push(req, now).unwrap();
        }
        for _ in 0..window {
            let c = shard.claim(1, 30_000, now).unwrap().expect("job disponible");
            held.push(Reservation { job_id: c.job_id, reservation_id: c.reservation_id, fencing_token: c.fencing_token });
        }
        for r in held.drain(..) {
            shard.ack(r, now).unwrap();
            done += 1;
        }
    }
    let el = start.elapsed();
    println!("{} jobs, fenêtre {}, {:.3} µs/job (push+claim+ack, un thread)", done, window, el.as_secs_f64() * 1e6 / done as f64);
}
