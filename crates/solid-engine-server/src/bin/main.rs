use solid_engine_core::config::Config;
use solid_engine_server::Server;

fn main() {
    let mut args = std::env::args().skip(1);
    let mut cfg = Config::default();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" => {
                let path = args.next().unwrap_or_else(|| die("--config requires a path"));
                let text = std::fs::read_to_string(&path).unwrap_or_else(|e| die(&format!("{path}: {e}")));
                cfg = Config::from_toml(&text).unwrap_or_else(|e| die(&format!("invalid config: {e}")));
            }
            "--help" | "-h" => {
                println!("usage: solid-engine-server [--config solid-engine.toml]");
                return;
            }
            other => die(&format!("unknown argument: {other}")),
        }
    }
    let server = Server::start(cfg).unwrap_or_else(|e| die(&format!("failed to start: {e}")));
    if let Some(addr) = server.tcp_addr() {
        eprintln!("solid-engine listening on tcp://{addr}");
    }
    if std::env::var("SOLID_ENGINE_SPIN_STATS").is_ok() {
        std::thread::spawn(|| loop {
            std::thread::sleep(std::time::Duration::from_millis(500));
            let (a, h, s) = solid_engine_server::engine::spin_stats();
            eprintln!("spin_stats attempts={a} hits={h} skipped={s}");
        });
    }
    if let Some(path) = std::env::var_os("SOLID_ENGINE_COUNTERS_FILE") {
        std::thread::spawn(move || {
            let tmp = std::path::PathBuf::from(&path).with_extension("tmp");
            loop {
                std::thread::sleep(std::time::Duration::from_millis(5));
                let body: String = solid_engine_server::stats::snapshot().iter().map(|(n, v)| format!("{n}={v}\n")).collect();
                if std::fs::write(&tmp, body).is_ok() {
                    let _ = std::fs::rename(&tmp, &path);
                }
            }
        });
    }
    // Pas de gestion de signaux en v1 (mémoire seule) : le processus tourne jusqu'à SIGTERM.
    loop {
        std::thread::park();
    }
}

fn die(msg: &str) -> ! {
    eprintln!("{msg}");
    std::process::exit(2);
}
