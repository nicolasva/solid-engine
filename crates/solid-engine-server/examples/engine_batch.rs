//! Coût du moteur sans réseau, en lots : push_many -> try_claim_many / claim_dispatch -> ack_many (mode memory).
//! Usage : engine_batch [shards] [lot] [jobs]
use solid_engine_core::config::Config;
use solid_engine_core::*;
use solid_engine_server::Engine;
use std::time::{Duration, Instant};

fn main() {
    let mut a = std::env::args().skip(1).map(|x| x.parse::<usize>().unwrap());
    let (shards, batch, jobs) = (a.next().unwrap_or(1), a.next().unwrap_or(100), a.next().unwrap_or(2_000_000));
    let mut cfg = Config::default();
    cfg.engine.shards = shards;
    cfg.limits.max_jobs_per_shard = 10_000_000;
    let engine = Engine::start(cfg).unwrap();
    let (mut push_t, mut claim_t, mut ack_t) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let mut next = 1u128;
    let mut done = 0usize;
    let start = Instant::now();
    while done < jobs {
        let t = Instant::now();
        let reqs: Vec<_> = (0..batch)
            .map(|_| {
                next += 1;
                PushRequest {
                    job_id: next,
                    channel_id: 1,
                    priority: 0,
                    max_attempts: 0,
                    delay_ms: 0,
                    partition_key: vec![],
                    payload: vec![b'x'; 256],
                }
            })
            .collect();
        for r in engine.push_many(reqs) {
            r.unwrap();
        }
        push_t += t.elapsed();

        let t = Instant::now();
        let claimed = engine.try_claim_many(1, 30_000, batch).unwrap();
        claim_t += t.elapsed();
        assert_eq!(claimed.len(), batch);

        let t = Instant::now();
        let rs = claimed
            .iter()
            .map(|c| Reservation { job_id: c.job_id, reservation_id: c.reservation_id, fencing_token: c.fencing_token })
            .collect();
        for r in engine.ack_many(rs) {
            assert!(r.is_none());
        }
        ack_t += t.elapsed();
        done += batch;
    }
    let el = start.elapsed();
    let per = |d: Duration| d.as_secs_f64() * 1e6 / done as f64;
    println!(
        "{shards} shard(s), lot {batch}: {:.0} k jobs/s | µs/job total {:.2} = push {:.2} + claim {:.2} + ack {:.2}",
        done as f64 / el.as_secs_f64() / 1000.0,
        el.as_secs_f64() * 1e6 / done as f64,
        per(push_t),
        per(claim_t),
        per(ack_t)
    );
}
