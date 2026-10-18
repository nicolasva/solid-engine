//! Types partagés et logique d'un shard de SolidEngine (sans I/O, sans threads).

pub mod config;
pub mod record;
pub mod shard;

use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum JobState {
    Ready = 0,
    Claimed = 1,
    Completed = 2,
    Scheduled = 3,
    Dead = 4,
    Cancelled = 5,
}

impl JobState {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Ready,
            1 => Self::Claimed,
            2 => Self::Completed,
            3 => Self::Scheduled,
            4 => Self::Dead,
            5 => Self::Cancelled,
            _ => return None,
        })
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Dead | Self::Cancelled)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    BadRequest = 1,
    NotFound = 2,
    StaleReservation = 3,
    InvalidState = 4,
    Overloaded = 5,
    PayloadTooLarge = 6,
    Internal = 7,
    ShuttingDown = 8,
}

impl ErrorCode {
    pub fn from_u16(v: u16) -> Self {
        match v {
            1 => Self::BadRequest,
            2 => Self::NotFound,
            3 => Self::StaleReservation,
            4 => Self::InvalidState,
            5 => Self::Overloaded,
            6 => Self::PayloadTooLarge,
            8 => Self::ShuttingDown,
            _ => Self::Internal,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineError {
    pub code: ErrorCode,
    pub message: String,
}

impl EngineError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for EngineError {}

pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushRequest {
    /// 0 = identifiant à générer par le moteur.
    pub job_id: u128,
    pub channel_id: u16,
    pub priority: u8,
    /// 0 = valeur par défaut de la configuration.
    pub max_attempts: u32,
    pub delay_ms: u64,
    /// Vide = routage par `job_id`.
    pub partition_key: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushOutcome {
    pub job_id: u128,
    /// Le job existait déjà (push idempotent rejoué).
    pub duplicate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedJob {
    pub job_id: u128,
    pub reservation_id: u64,
    pub fencing_token: u64,
    pub lease_deadline: u64,
    pub attempts: u32,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reservation {
    pub job_id: u128,
    pub reservation_id: u64,
    pub fencing_token: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NackKind {
    /// Nouvelle tentative ; `None` applique la politique de backoff.
    Retry { delay_ms: Option<u64> },
    /// Échec définitif : le job passe en `Dead`.
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JobView {
    pub job_id: u128,
    pub channel_id: u16,
    pub state: JobState,
    pub priority: u8,
    pub attempts: u32,
    pub max_attempts: u32,
    pub fencing_token: u64,
    pub lease_deadline: u64,
    pub scheduled_at: u64,
    pub created_at: u64,
    pub payload_length: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub ready: u64,
    pub claimed: u64,
    pub scheduled: u64,
    pub completed: u64,
    pub dead: u64,
    pub cancelled: u64,
    pub pushed: u64,
    pub claims: u64,
    pub acks: u64,
    pub nacks: u64,
    pub lease_expirations: u64,
}

impl Stats {
    pub fn add(&mut self, o: &Stats) {
        self.ready += o.ready;
        self.claimed += o.claimed;
        self.scheduled += o.scheduled;
        self.completed += o.completed;
        self.dead += o.dead;
        self.cancelled += o.cancelled;
        self.pushed += o.pushed;
        self.claims += o.claims;
        self.acks += o.acks;
        self.nacks += o.nacks;
        self.lease_expirations += o.lease_expirations;
    }
}

/// Millisecondes depuis l'époque UNIX (UTC).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Hash FNV-1a stable, utilisé pour le routage des shards.
pub fn stable_hash(channel_id: u16, key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in channel_id.to_le_bytes().iter().chain(key) {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// `shard_id = hash(channel_id, partition_key) % shard_count`
pub fn shard_for(channel_id: u16, partition_key: &[u8], shard_count: usize) -> usize {
    (stable_hash(channel_id, partition_key) % shard_count as u64) as usize
}
