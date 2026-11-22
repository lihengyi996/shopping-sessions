//! User / event / campaign identifiers as a single machine word when they fit.

use std::cmp::Ordering;
use std::sync::{Mutex, OnceLock};

use hashbrown::HashMap;

/// Opaque identifier. Short `[0-9A-Za-z_]` values pack so byte order matches
/// numeric order; everything else is interned behind a process-wide table.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct Id(u64);

const MAX_PACK: usize = 10;
const INTERNED: u64 = 1 << 63;

const CODES: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut next = 1u8;
    let mut i = 0;
    while i < 256 {
        let b = i as u8;
        if matches!(b, b'0'..=b'9' | b'A'..=b'Z' | b'_' | b'a'..=b'z') {
            t[i] = next;
            next += 1;
        }
        i += 1;
    }
    t
};

const CHARS: [u8; 64] = {
    let mut out = [0u8; 64];
    let mut i = 0;
    while i < 256 {
        let c = CODES[i];
        if c != 0 {
            out[c as usize] = i as u8;
        }
        i += 1;
    }
    out
};

struct Pool {
    by_bytes: HashMap<Vec<u8>, u32>,
    bytes: Vec<Vec<u8>>,
}

#[cold]
#[inline(never)]
fn intern(raw: &[u8]) -> Id {
    let mut p = pool().lock().unwrap();
    let n = p.bytes.len() as u32;
    let idx = *p.by_bytes.entry(raw.to_vec()).or_insert(n);
    if idx == n {
        p.bytes.push(raw.to_vec());
    }
    Id(INTERNED | u64::from(idx))
}

fn pool() -> &'static Mutex<Pool> {
    static POOL: OnceLock<Mutex<Pool>> = OnceLock::new();
    POOL.get_or_init(|| {
        Mutex::new(Pool { by_bytes: HashMap::new(), bytes: Vec::new() })
    })
}

impl Id {
    #[inline]
    pub fn of(raw: &[u8]) -> Self {
        if let Some(packed) = pack(raw) {
            Id(packed)
        } else {
            intern(raw)
        }
    }

    pub fn bytes(self) -> Vec<u8> {
        if self.0 & INTERNED == 0 {
            unpack(self.0)
        } else {
            pool().lock().unwrap().bytes[(self.0 & !INTERNED) as usize].clone()
        }
    }

    #[inline]
    pub fn ord(self, other: Self) -> Ordering {
        if (self.0 | other.0) & INTERNED == 0 {
            self.0.cmp(&other.0)
        } else {
            self.bytes().cmp(&other.bytes())
        }
    }

    /// Stable machine key for grouping/sorting when byte order is not required.
    #[inline]
    pub fn key(self) -> u64 {
        self.0
    }

    #[inline]
    pub fn shard(self, bits: u32) -> usize {
        (self.0.wrapping_mul(0x517c_c1b7_2722_0a95) >> (64 - bits)) as usize
    }
}

#[inline]
fn pack(raw: &[u8]) -> Option<u64> {
    if raw.len() > MAX_PACK {
        return None;
    }
    let mut v = 0u64;
    for &b in raw {
        let c = CODES[b as usize];
        if c == 0 {
            return None;
        }
        v = (v << 6) | u64::from(c);
    }
    Some(v << (6 * (MAX_PACK - raw.len())))
}

fn unpack(mut v: u64) -> Vec<u8> {
    let mut tmp = [0u8; MAX_PACK];
    let mut n = 0;
    for i in (0..MAX_PACK).rev() {
        let c = ((v >> (6 * i)) & 63) as usize;
        if c == 0 {
            break;
        }
        tmp[n] = CHARS[c];
        n += 1;
    }
    let _ = &mut v;
    tmp[..n].to_vec()
}
