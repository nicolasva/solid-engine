use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub engine: EngineConfig,
    pub jobs: JobsConfig,
    pub limits: LimitsConfig,
    pub server: ServerConfig,
    pub storage: StorageConfig,
}

/// Les trois modes ne sont jamais équivalents :
/// - `strict` : fsync avant chaque confirmation (group commit par shard) ;
/// - `balanced` : écriture immédiate, fsync toutes les `sync_interval_ms` — les écritures des
///   dernières `sync_interval_ms` peuvent être perdues lors d'un crash de la machine ;
/// - `memory` : aucune persistance.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Durability {
    Strict,
    Balanced,
    Memory,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub durability: Durability,
    pub data_dir: Option<String>,
    pub sync_interval_ms: u64,
    /// Nombre d'enregistrements WAL d'un shard déclenchant un snapshot (0 = jamais).
    pub snapshot_every_records: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self { durability: Durability::Memory, data_dir: None, sync_interval_ms: 10, snapshot_every_records: 100_000 }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    pub shards: usize,
    pub mailbox_capacity: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackoffStrategy {
    Immediate,
    Fixed,
    Exponential,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct RetryConfig {
    pub strategy: BackoffStrategy,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    /// Fraction (0.0..=1.0) du délai retranchée aléatoirement.
    pub jitter: f64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct JobsConfig {
    pub default_lease_ms: u32,
    pub max_lease_ms: u32,
    pub default_max_attempts: u32,
    pub completed_retention_ms: u64,
    pub dead_retention_ms: u64,
    pub retry: RetryConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct LimitsConfig {
    pub max_payload_bytes: usize,
    pub max_jobs_per_shard: usize,
    pub max_connections: usize,
    pub idle_timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub tcp_addr: Option<String>,
    pub unix_socket: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            engine: EngineConfig::default(),
            jobs: JobsConfig::default(),
            limits: LimitsConfig::default(),
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self { shards: 4, mailbox_capacity: 4096 }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            strategy: BackoffStrategy::Exponential,
            base_delay_ms: 1_000,
            max_delay_ms: 3_600_000,
            jitter: 0.1,
        }
    }
}

impl Default for JobsConfig {
    fn default() -> Self {
        Self {
            default_lease_ms: 30_000,
            max_lease_ms: 3_600_000,
            default_max_attempts: 5,
            completed_retention_ms: 60_000,
            dead_retention_ms: 7 * 24 * 3_600_000,
            retry: RetryConfig::default(),
        }
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: 1 << 20,
            max_jobs_per_shard: 1_000_000,
            max_connections: 1024,
            idle_timeout_ms: 300_000,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { tcp_addr: Some("127.0.0.1:7777".into()), unix_socket: None }
    }
}

impl Config {
    pub fn from_toml(s: &str) -> Result<Self, String> {
        let cfg: Config = toml::from_str(s).map_err(|e| e.to_string())?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.engine.shards == 0 || self.engine.shards > u16::MAX as usize {
            return Err("engine.shards must be in 1..=65535".into());
        }
        if self.engine.mailbox_capacity == 0 {
            return Err("engine.mailbox_capacity must be > 0".into());
        }
        if self.jobs.default_lease_ms == 0 || self.jobs.default_lease_ms > self.jobs.max_lease_ms {
            return Err("jobs.default_lease_ms must be in 1..=max_lease_ms".into());
        }
        if !(0.0..=1.0).contains(&self.jobs.retry.jitter) {
            return Err("jobs.retry.jitter must be in 0.0..=1.0".into());
        }
        if self.storage.durability != Durability::Memory && self.storage.data_dir.is_none() {
            return Err("storage.data_dir is required unless durability = \"memory\"".into());
        }
        if self.storage.sync_interval_ms == 0 {
            return Err("storage.sync_interval_ms must be > 0".into());
        }
        Ok(())
    }
}
