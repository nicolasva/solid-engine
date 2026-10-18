use solid_engine_core::config::Config;
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{ProtocolError, Request, Response};
use solid_engine_server::Server;

fn start(tweak: impl FnOnce(&mut Config)) -> (Server, String) {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    tweak(&mut cfg);
    let server = Server::start(cfg).unwrap();
    let addr = server.tcp_addr().unwrap().to_string();
    (server, addr)
}

fn push(job_id: u128) -> Request {
    Request::PushJob(PushRequest {
        job_id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: vec![],
        payload: b"x".to_vec(),
    })
}

fn unauthorized(r: std::result::Result<Response, ProtocolError>) -> bool {
    matches!(r, Ok(Response::Error(e)) if e.code == ErrorCode::Unauthorized)
}

#[test]
fn tcp_requires_the_token_before_any_command() {
    let (server, addr) = start(|c| c.server.auth_token = Some("s3cret".into()));

    // Commande sans AUTH : refusée, connexion fermée.
    let mut c = Client::connect_tcp(&addr).unwrap();
    assert!(unauthorized(c.call(&push(1))));
    assert!(c.call(&Request::Ping).is_err());

    // Mauvais jeton : refusé, connexion fermée, et rien n'a été exécuté.
    let mut c = Client::connect_tcp(&addr).unwrap();
    assert!(unauthorized(c.authenticate(b"wrong")));
    assert!(c.call(&Request::Ping).is_err());

    // Bon jeton : les commandes passent.
    let mut c = Client::connect_tcp(&addr).unwrap();
    assert_eq!(c.authenticate(b"s3cret").unwrap(), Response::Authed);
    assert!(matches!(c.call(&push(2)).unwrap(), Response::Push(_)));
    assert!(matches!(c.call(&Request::GetJob { job_id: 1 }).unwrap(), Response::Error(e) if e.code == ErrorCode::NotFound));
    server.shutdown();
}

#[test]
fn token_can_come_from_a_file() {
    let path = std::env::temp_dir().join(format!("solid-engine-token-{}", std::process::id()));
    std::fs::write(&path, "from-file\nignored\n").unwrap();
    let (server, addr) = start(|c| {
        c.server.auth_token = Some("ignored-config-token".into());
        c.server.auth_token_file = Some(path.display().to_string());
    });
    let mut c = Client::connect_tcp(&addr).unwrap();
    assert!(unauthorized(c.authenticate(b"ignored-config-token")));
    let mut c = Client::connect_tcp(&addr).unwrap();
    assert_eq!(c.authenticate(b"from-file").unwrap(), Response::Authed);
    server.shutdown();
    let _ = std::fs::remove_file(path);
}

#[test]
fn unix_socket_is_not_subject_to_the_tcp_token_but_to_its_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let sock = std::env::temp_dir().join(format!("solid-engine-auth-{}.sock", std::process::id()));
    let (server, addr) = start(|c| {
        c.server.auth_token = Some("s3cret".into());
        c.server.unix_socket = Some(sock.display().to_string());
        c.server.unix_socket_mode = Some(0o600);
    });
    assert_eq!(std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777, 0o600);
    let mut u = Client::connect_unix(&sock).unwrap();
    assert_eq!(u.call(&Request::Ping).unwrap(), Response::Pong);
    let mut t = Client::connect_tcp(&addr).unwrap();
    assert!(unauthorized(t.call(&Request::Ping)));
    server.shutdown();
}

#[test]
fn without_a_token_auth_is_not_required() {
    let (server, addr) = start(|_| {});
    let mut c = Client::connect_tcp(&addr).unwrap();
    assert_eq!(c.call(&Request::Ping).unwrap(), Response::Pong);
    server.shutdown();
}

#[test]
fn commands_pipelined_in_the_same_packet_never_run_before_a_valid_auth() {
    use solid_engine_protocol::{read_frame, write_frame};
    use std::io::Write;
    let (server, addr) = start(|c| c.server.auth_token = Some("s3cret".into()));
    let probe = |first: Request| {
        let mut s = std::net::TcpStream::connect(&addr).unwrap();
        let mut buf = Vec::new();
        write_frame(&mut buf, &first.to_frame(1)).unwrap();
        write_frame(&mut buf, &push(77).to_frame(2)).unwrap();
        s.write_all(&buf).unwrap();
        s.set_read_timeout(Some(std::time::Duration::from_millis(500))).unwrap();
        let mut codes = vec![];
        while let Ok(f) = read_frame(&mut s, 1 << 20) {
            codes.push(Response::decode(f.msg_type, &f.body).unwrap());
        }
        codes
    };
    // PUSH puis AUTH valide dans le même paquet, et AUTH invalide puis PUSH : le PUSH n'est jamais exécuté.
    assert!(matches!(probe(push(76))[..], [Response::Error(_)]));
    assert!(matches!(probe(Request::Auth(b"bad".to_vec()))[..], [Response::Error(_)]));
    let mut c = Client::connect_tcp(&addr).unwrap();
    c.authenticate(b"s3cret").unwrap();
    for id in [76u128, 77] {
        assert!(matches!(c.call(&Request::GetJob { job_id: id }).unwrap(), Response::Error(e) if e.code == ErrorCode::NotFound));
    }
    // AUTH valide suivi de PUSH dans le même paquet : les deux sont traités dans l'ordre.
    let ok = probe(Request::Auth(b"s3cret".to_vec()));
    assert!(matches!(ok[..], [Response::Authed, Response::Push(_)]), "{ok:?}");
    server.shutdown();
}
