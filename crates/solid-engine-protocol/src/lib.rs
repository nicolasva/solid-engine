//! Protocole binaire SolidEngine v1.
//!
//! Enveloppe (little-endian) :
//! `MAGIC u32 | VERSION u8 | TYPE u8 | FLAGS u16 | REQUEST_ID u64 | BODY_LEN u32 | BODY | CRC32 u32`
//! Le CRC32 couvre l'en-tête et le corps. Les réponses reprennent le `REQUEST_ID` de la requête
//! et ont pour type `TYPE_REQUETE | 0x80` (ou `0xFF` pour une erreur).

pub mod client;
mod codec;
mod message;

pub use codec::{read_frame, write_frame, Frame, ProtocolError, HEADER_LEN, MAGIC, VERSION};
pub use message::{Request, Response};

pub mod msg {
    pub const PING: u8 = 1;
    pub const PUSH_JOB: u8 = 2;
    pub const CLAIM_NEXT_JOB: u8 = 3;
    pub const ACK_JOB: u8 = 4;
    pub const NACK_JOB: u8 = 5;
    pub const EXTEND_LEASE: u8 = 6;
    pub const GET_JOB: u8 = 7;
    pub const CANCEL_JOB: u8 = 8;
    pub const WAIT_FOR_JOB: u8 = 9;
    pub const GET_QUEUE_STATS: u8 = 10;
    pub const AUTH: u8 = 11;
    pub const RESPONSE_BIT: u8 = 0x80;
    pub const ERROR: u8 = 0xFF;
}
