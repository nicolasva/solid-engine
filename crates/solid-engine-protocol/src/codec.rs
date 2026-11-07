use std::fmt;
use std::io::{self, Read, Write};

pub const MAGIC: u32 = 0x474E_4553; // "SENG" en little-endian
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 20;
const CRC_LEN: usize = 4;

#[derive(Debug)]
pub enum ProtocolError {
    Io(io::Error),
    /// Fin de flux propre entre deux messages.
    Eof,
    BadMagic,
    UnsupportedVersion(u8),
    TooLarge(u32),
    ChecksumMismatch,
    Malformed(&'static str),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Eof => write!(f, "connection closed"),
            Self::BadMagic => write!(f, "bad magic"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported protocol version {v}"),
            Self::TooLarge(n) => write!(f, "message body too large ({n} bytes)"),
            Self::ChecksumMismatch => write!(f, "checksum mismatch"),
            Self::Malformed(m) => write!(f, "malformed message: {m}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<io::Error> for ProtocolError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub msg_type: u8,
    pub flags: u16,
    pub request_id: u64,
    pub body: Vec<u8>,
}

pub fn write_frame<W: Write>(w: &mut W, f: &Frame) -> Result<(), ProtocolError> {
    let mut buf = Vec::new();
    append_frame(&mut buf, f);
    w.write_all(&buf)?;
    w.flush()?;
    Ok(())
}

fn frame_header(f: &Frame) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    h[4] = VERSION;
    h[5] = f.msg_type;
    h[6..8].copy_from_slice(&f.flags.to_le_bytes());
    h[8..16].copy_from_slice(&f.request_id.to_le_bytes());
    h[16..20].copy_from_slice(&(f.body.len() as u32).to_le_bytes());
    h
}

fn frame_crc(header: &[u8; HEADER_LEN], body: &[u8]) -> [u8; CRC_LEN] {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(header);
    hasher.update(body);
    hasher.finalize().to_le_bytes()
}

/// Ajoute la trame à `buf` en une seule copie du corps (pas de tampon intermédiaire).
pub fn append_frame(buf: &mut Vec<u8>, f: &Frame) {
    let header = frame_header(f);
    buf.reserve(HEADER_LEN + f.body.len() + CRC_LEN);
    buf.extend_from_slice(&header);
    buf.extend_from_slice(&f.body);
    buf.extend_from_slice(&frame_crc(&header, &f.body));
}

/// Écrit `pending` puis la trame sans copier son corps (écriture vectorisée) ; gère les écritures partielles.
pub fn write_pending_and_frame<W: Write>(w: &mut W, pending: &[u8], f: &Frame) -> Result<(), ProtocolError> {
    let header = frame_header(f);
    let crc = frame_crc(&header, &f.body);
    let parts: [&[u8]; 4] = [pending, &header, &f.body, &crc];
    let (mut idx, mut off) = (0usize, 0usize);
    while idx < parts.len() {
        if off >= parts[idx].len() {
            idx += 1;
            off = 0;
            continue;
        }
        let slices: Vec<io::IoSlice> = std::iter::once(io::IoSlice::new(&parts[idx][off..]))
            .chain(parts[idx + 1..].iter().map(|p| io::IoSlice::new(p)))
            .collect();
        let mut n = match w.write_vectored(&slices) {
            Ok(0) => return Err(ProtocolError::Io(io::ErrorKind::WriteZero.into())),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        };
        while n > 0 && idx < parts.len() {
            let left = parts[idx].len() - off;
            if n >= left {
                n -= left;
                idx += 1;
                off = 0;
            } else {
                off += n;
                n = 0;
            }
        }
    }
    w.flush()?;
    Ok(())
}

pub fn read_frame<R: Read>(r: &mut R, max_body: usize) -> Result<Frame, ProtocolError> {
    let mut header = [0u8; HEADER_LEN];
    let mut got = 0;
    while got < HEADER_LEN {
        match r.read(&mut header[got..])? {
            0 if got == 0 => return Err(ProtocolError::Eof),
            0 => return Err(ProtocolError::Io(io::ErrorKind::UnexpectedEof.into())),
            n => got += n,
        }
    }
    if u32::from_le_bytes(header[0..4].try_into().unwrap()) != MAGIC {
        return Err(ProtocolError::BadMagic);
    }
    if header[4] != VERSION {
        return Err(ProtocolError::UnsupportedVersion(header[4]));
    }
    let len = u32::from_le_bytes(header[16..20].try_into().unwrap());
    if len as usize > max_body {
        return Err(ProtocolError::TooLarge(len));
    }
    let mut body = vec![0u8; len as usize];
    r.read_exact(&mut body)?;
    let mut crc = [0u8; CRC_LEN];
    r.read_exact(&mut crc)?;
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header);
    hasher.update(&body);
    if hasher.finalize() != u32::from_le_bytes(crc) {
        return Err(ProtocolError::ChecksumMismatch);
    }
    Ok(Frame {
        msg_type: header[5],
        flags: u16::from_le_bytes(header[6..8].try_into().unwrap()),
        request_id: u64::from_le_bytes(header[8..16].try_into().unwrap()),
        body,
    })
}

pub(crate) struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Self(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        if self.0.len() < n {
            return Err(ProtocolError::Malformed("truncated body"));
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }

    pub fn u8(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, ProtocolError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> Result<u32, ProtocolError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, ProtocolError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u128(&mut self) -> Result<u128, ProtocolError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub fn bytes(&mut self) -> Result<Vec<u8>, ProtocolError> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    pub fn finish(self) -> Result<(), ProtocolError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(ProtocolError::Malformed("trailing bytes"))
        }
    }
}

pub(crate) trait PutExt {
    fn put_u16(&mut self, v: u16);
    fn put_u32(&mut self, v: u32);
    fn put_u64(&mut self, v: u64);
    fn put_u128(&mut self, v: u128);
    fn put_bytes(&mut self, b: &[u8]);
}

impl PutExt for Vec<u8> {
    fn put_u16(&mut self, v: u16) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn put_u32(&mut self, v: u32) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn put_u64(&mut self, v: u64) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn put_u128(&mut self, v: u128) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn put_bytes(&mut self, b: &[u8]) {
        self.put_u32(b.len() as u32);
        self.extend_from_slice(b);
    }
}
