use solid_engine_core::config::Config;
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn start(tweak: impl FnOnce(&mut Config)) -> (Server, String) {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    tweak(&mut cfg);
    let server = Server::start(cfg).unwrap();
    let addr = server.tcp_addr().unwrap().to_string();
    (server, addr)
}

fn push(job_id: u128, delay_ms: u64) -> Request {
    Request::PushJob(PushRequest {
        job_id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms,
        partition_key: vec![],
        payload: b"hello".to_vec(),
    })
}

fn claim(c: &mut Client<impl Read + Write>, wait_ms: u32) -> Option<ClaimedJob> {
    match c.call(&Request::ClaimNextJob { channel_id: 1, lease_ms: 0, wait_ms }).unwrap() {
        Response::Claim(j) => j,
        other => panic!("unexpected {other:?}"),
    }
}

fn reservation(j: &ClaimedJob) -> Reservation {
    Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token }
}

#[test]
fn push_claim_ack_over_tcp_and_unix() {
    let sock = std::env::temp_dir().join(format!("solid-engine-test-{}.sock", std::process::id()));
    let (server, addr) = start(|c| c.server.unix_socket = Some(sock.to_string_lossy().into()));
    let mut tcp = Client::connect_tcp(&addr).unwrap();
    let mut unix = Client::connect_unix(&sock).unwrap();

    assert_eq!(tcp.call(&Request::Ping).unwrap(), Response::Pong);
    let Response::Push(out) = tcp.call(&push(0, 0)).unwrap() else { panic!() };
    assert_ne!(out.job_id, 0);

    let job = claim(&mut unix, 0).expect("job available through the other transport");
    assert_eq!(job.payload, b"hello");
    assert_eq!(job.fencing_token, 1);
    assert_eq!(unix.call(&Request::AckJob(reservation(&job))).unwrap(), Response::Ack);

    // Un ACK rejoué est rejeté.
    let Response::Error(e) = unix.call(&Request::AckJob(reservation(&job))).unwrap() else { panic!() };
    assert_eq!(e.code, ErrorCode::StaleReservation);

    let Response::Job(view) = tcp.call(&Request::GetJob { job_id: out.job_id }).unwrap() else { panic!() };
    assert_eq!(view.state, JobState::Completed);
    server.shutdown();
}

#[test]
fn idempotent_push_and_cancel() {
    let (server, addr) = start(|_| {});
    let mut c = Client::connect_tcp(&addr).unwrap();
    let Response::Push(a) = c.call(&push(99, 0)).unwrap() else { panic!() };
    let Response::Push(b) = c.call(&push(99, 0)).unwrap() else { panic!() };
    assert!(!a.duplicate && b.duplicate);
    assert_eq!(c.call(&Request::CancelJob { job_id: 99 }).unwrap(), Response::Cancel);
    assert!(claim(&mut c, 0).is_none());
    let Response::Error(e) = c.call(&Request::GetJob { job_id: 12345 }).unwrap() else { panic!() };
    assert_eq!(e.code, ErrorCode::NotFound);
    server.shutdown();
}

#[test]
fn waiting_claim_is_woken_by_push_without_polling() {
    let (server, addr) = start(|_| {});
    let addr2 = addr.clone();
    let waiter = std::thread::spawn(move || {
        let mut c = Client::connect_tcp(&addr2).unwrap();
        let start = Instant::now();
        (claim(&mut c, 5_000), start.elapsed())
    });
    std::thread::sleep(Duration::from_millis(200));
    let mut p = Client::connect_tcp(&addr).unwrap();
    p.call(&push(5, 0)).unwrap();
    let (job, waited) = waiter.join().unwrap();
    assert_eq!(job.unwrap().job_id, 5);
    assert!(waited < Duration::from_secs(2), "woke after {waited:?}");
    server.shutdown();
}

#[test]
fn waiting_claim_times_out_without_blocking_others() {
    let (server, addr) = start(|_| {});
    let addr2 = addr.clone();
    let waiter = std::thread::spawn(move || {
        let mut c = Client::connect_tcp(&addr2).unwrap();
        let start = Instant::now();
        (claim(&mut c, 400), start.elapsed())
    });
    std::thread::sleep(Duration::from_millis(100));
    let mut p = Client::connect_tcp(&addr).unwrap();
    let t = Instant::now();
    assert_eq!(p.call(&Request::Ping).unwrap(), Response::Pong);
    assert!(t.elapsed() < Duration::from_millis(100));
    let (job, waited) = waiter.join().unwrap();
    assert!(job.is_none());
    assert!(waited >= Duration::from_millis(380) && waited < Duration::from_secs(2), "{waited:?}");
    server.shutdown();
}

#[test]
fn job_taken_by_a_disconnected_waiter_is_redelivered() {
    let (server, addr) = start(|_| {});
    let mut gone = std::net::TcpStream::connect(&addr).unwrap();
    let req = Request::ClaimNextJob { channel_id: 1, lease_ms: 300, wait_ms: 5_000 };
    solid_engine_protocol::write_frame(&mut gone, &req.to_frame(1)).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    drop(gone);
    std::thread::sleep(Duration::from_millis(100));
    let mut c = Client::connect_tcp(&addr).unwrap();
    c.call(&push(7, 0)).unwrap();
    let job = claim(&mut c, 3_000).expect("job redelivered once the dead waiter's lease expires");
    assert_eq!(job.job_id, 7);
    server.shutdown();
}

#[test]
fn two_waiters_share_two_jobs_without_duplicates() {
    let (server, addr) = start(|_| {});
    let waiters: Vec<_> = (0..2)
        .map(|_| {
            let a = addr.clone();
            std::thread::spawn(move || claim(&mut Client::connect_tcp(&a).unwrap(), 3_000).map(|j| j.job_id))
        })
        .collect();
    std::thread::sleep(Duration::from_millis(150));
    let mut p = Client::connect_tcp(&addr).unwrap();
    p.call(&push(1, 0)).unwrap();
    p.call(&push(2, 0)).unwrap();
    let mut got: Vec<_> = waiters.into_iter().map(|w| w.join().unwrap().unwrap()).collect();
    got.sort();
    assert_eq!(got, vec![1, 2]);
    server.shutdown();
}

#[test]
fn lease_expiry_redelivers_and_fences_old_worker() {
    let (server, addr) = start(|c| c.jobs.retry.strategy = solid_engine_core::config::BackoffStrategy::Immediate);
    let mut w1 = Client::connect_tcp(&addr).unwrap();
    let mut w2 = Client::connect_tcp(&addr).unwrap();
    w1.call(&push(1, 0)).unwrap();
    let Response::Claim(Some(old)) =
        w1.call(&Request::ClaimNextJob { channel_id: 1, lease_ms: 100, wait_ms: 0 }).unwrap()
    else {
        panic!()
    };
    // Le worker 2 attend : l'expiration du bail réveille l'attente sans polling.
    let new = claim(&mut w2, 3_000).expect("redelivered after lease expiry");
    assert!(new.fencing_token > old.fencing_token);
    assert_eq!(new.attempts, 2);
    let Response::Error(e) = w1.call(&Request::AckJob(reservation(&old))).unwrap() else { panic!() };
    assert_eq!(e.code, ErrorCode::StaleReservation);
    assert_eq!(w2.call(&Request::AckJob(reservation(&new))).unwrap(), Response::Ack);
    server.shutdown();
}

#[test]
fn delayed_job_and_stats() {
    let (server, addr) = start(|_| {});
    let mut c = Client::connect_tcp(&addr).unwrap();
    c.call(&push(1, 300)).unwrap();
    assert!(claim(&mut c, 0).is_none());
    let Response::Stats(s) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
    assert_eq!((s.scheduled, s.ready), (1, 0));
    assert_eq!(c.call(&Request::WaitForJob { channel_id: 1, timeout_ms: 3_000 }).unwrap(), Response::Wait { available: true });
    assert!(claim(&mut c, 0).is_some());
    server.shutdown();
}

#[test]
fn many_workers_never_share_a_job() {
    let (server, addr) = start(|_| {});
    let mut p = Client::connect_tcp(&addr).unwrap();
    const N: u128 = 400;
    for id in 1..=N {
        p.call(&push(id, 0)).unwrap();
    }
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (addr, seen) = (addr.clone(), seen.clone());
            std::thread::spawn(move || {
                let mut c = Client::connect_tcp(&addr).unwrap();
                while let Some(j) = claim(&mut c, 0) {
                    assert_eq!(c.call(&Request::AckJob(reservation(&j))).unwrap(), Response::Ack);
                    seen.lock().unwrap().push(j.job_id);
                }
            })
        })
        .collect();
    handles.into_iter().for_each(|h| h.join().unwrap());
    let mut ids = seen.lock().unwrap().clone();
    ids.sort();
    assert_eq!(ids, (1..=N).collect::<Vec<_>>());
    server.shutdown();
}

#[test]
fn invalid_frames_are_rejected() {
    let (server, addr) = start(|c| c.limits.max_payload_bytes = 16);
    let mut raw = std::net::TcpStream::connect(&addr).unwrap();
    raw.write_all(&[0xAB; 32]).unwrap();
    let mut buf = Vec::new();
    raw.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let _ = raw.read_to_end(&mut buf); // le serveur répond une erreur puis ferme

    let mut c = Client::connect_tcp(&addr).unwrap();
    let Response::Error(e) = c
        .call(&Request::PushJob(PushRequest {
            job_id: 1,
            channel_id: 1,
            priority: 0,
            max_attempts: 0,
            delay_ms: 0,
            partition_key: vec![],
            payload: vec![0; 17],
        }))
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(e.code, ErrorCode::PayloadTooLarge);
    server.shutdown();
}

#[test]
fn pipelined_requests_on_one_connection_are_all_answered_by_request_id() {
    use solid_engine_protocol::{read_frame, write_frame};
    let (server, addr) = start(|_| {});
    let mut s = std::net::TcpStream::connect(&addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    const N: u64 = 500;
    for i in 1..=N {
        let req = push(i as u128, 0);
        write_frame(&mut s, &req.to_frame(i)).unwrap();
    }
    let mut seen = std::collections::HashSet::new();
    for _ in 0..N {
        let f = read_frame(&mut s, 1 << 20).unwrap();
        let Response::Push(_) = Response::decode(f.msg_type & 0x7F, &f.body).unwrap() else { panic!() };
        assert!(seen.insert(f.request_id));
    }
    assert_eq!(seen.len() as u64, N);
    server.shutdown();
}
