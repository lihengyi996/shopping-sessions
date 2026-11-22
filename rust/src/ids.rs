//! Opaque identifiers for v5: short IDs pack into a word; 33-char `X`+hex IDs
//! store inline without a lock; everything else uses a sharded intern pool.

use std::cmp::Ordering;
use std::sync::{Mutex, OnceLock};

use hashbrown::HashMap;

/// Opaque identifier. Equality is exact byte identity; `ord` is byte order.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct Id {
    kind: u8,
    prefix: u8,
    lo: u64,
    hi: u64,
}

const KIND_PACK: u8 = 0;
const KIND_HEX: u8 = 1;
const KIND_INTERN: u8 = 2;

const MAX_PACK: usize = 10;
const SHARDS: usize = 256;

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

/// ASCII → nibble; 0xFF = invalid. Speeds the v5 33-char hex hot path.
const HEX_VAL: [u8; 256] = {
    let mut t = [0xFFu8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = i as u8;
        i += 1;
    }
    i = 0;
    while i < 6 {
        t[b'a' as usize + i] = 10 + i as u8;
        i += 1;
    }
    t
};

const HEX_OUT: [u8; 16] = *b"0123456789abcdef";

struct Pool {
    by_bytes: HashMap<Vec<u8>, u32>,
    bytes: Vec<Vec<u8>>,
}

type ShardPool = Mutex<Pool>;

fn pools() -> &'static [ShardPool; SHARDS] {
    static POOLS: OnceLock<[ShardPool; SHARDS]> = OnceLock::new();
    POOLS.get_or_init(|| {
        std::array::from_fn(|_| Mutex::new(Pool { by_bytes: HashMap::new(), bytes: Vec::new() }))
    })
}

#[inline]
fn shard_of(raw: &[u8]) -> usize {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in raw {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h as usize & (SHARDS - 1)
}

#[cold]
#[inline(never)]
fn intern(raw: &[u8]) -> Id {
    let s = shard_of(raw);
    let mut p = pools()[s].lock().unwrap();
    let n = p.bytes.len() as u32;
    let idx = *p.by_bytes.entry(raw.to_vec()).or_insert(n);
    if idx == n {
        p.bytes.push(raw.to_vec());
    }
    Id {
        kind: KIND_INTERN,
        prefix: 0,
        lo: ((s as u64) << 32) | u64::from(idx),
        hi: 0,
    }
}

impl Id {
    #[inline]
    pub fn of(raw: &[u8]) -> Self {
        if let Some(packed) = pack(raw) {
            return Id {
                kind: KIND_PACK,
                prefix: 0,
                lo: packed,
                hi: 0,
            };
        }
        if let Some((prefix, bits)) = pack_hex33(raw) {
            return Id {
                kind: KIND_HEX,
                prefix,
                lo: bits as u64,
                hi: (bits >> 64) as u64,
            };
        }
        intern(raw)
    }

    pub fn bytes(self) -> Vec<u8> {
        match self.kind {
            KIND_PACK => unpack(self.lo),
            KIND_HEX => unpack_hex(self.prefix, self.lo, self.hi),
            _ => {
                let s = (self.lo >> 32) as usize;
                let idx = self.lo as u32 as usize;
                pools()[s].lock().unwrap().bytes[idx].clone()
            }
        }
    }

    #[inline]
    pub fn ord(self, other: Self) -> Ordering {
        if self.kind == other.kind && self.kind != KIND_INTERN {
            match self.kind {
                KIND_PACK => self.lo.cmp(&other.lo),
                KIND_HEX => self
                    .prefix
                    .cmp(&other.prefix)
                    .then_with(|| self.hi.cmp(&other.hi))
                    .then_with(|| self.lo.cmp(&other.lo)),
                _ => self.bytes().cmp(&other.bytes()),
            }
        } else {
            self.bytes().cmp(&other.bytes())
        }
    }

    /// Stable machine key for grouping/sorting when byte order is not required.
    #[inline]
    pub fn key(self) -> u64 {
        self.lo
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(self.hi)
            .wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
            .wrapping_add(u64::from(self.kind) << 56)
            .wrapping_add(u64::from(self.prefix))
    }

    #[inline]
    pub fn shard(self, bits: u32) -> usize {
        (self.key().wrapping_mul(0x517c_c1b7_2722_0a95) >> (64 - bits)) as usize
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

/// v5 long form: one prefix byte + 32 lowercase hex digits (no lock).
#[inline]
fn pack_hex33(raw: &[u8]) -> Option<(u8, u128)> {
    if raw.len() != 33 {
        return None;
    }
    let prefix = raw[0];
    if !prefix.is_ascii_alphanumeric() {
        return None;
    }
    let mut v = 0u128;
    // Unrolled 4-at-a-time with a nibble LUT.
    let hex = &raw[1..];
    let mut i = 0;
    while i < 32 {
        let a = HEX_VAL[hex[i] as usize];
        let b = HEX_VAL[hex[i + 1] as usize];
        let c = HEX_VAL[hex[i + 2] as usize];
        let d = HEX_VAL[hex[i + 3] as usize];
        if a > 15 || b > 15 || c > 15 || d > 15 {
            return None;
        }
        v = (v << 16) | u128::from((u16::from(a) << 12) | (u16::from(b) << 8) | (u16::from(c) << 4) | u16::from(d));
        i += 4;
    }
    Some((prefix, v))
}

fn unpack_hex(prefix: u8, lo: u64, hi: u64) -> Vec<u8> {
    let mut bits = (u128::from(hi) << 64) | u128::from(lo);
    let mut out = vec![0u8; 33];
    out[0] = prefix;
    for i in (0..32).rev() {
        out[1 + i] = HEX_OUT[(bits & 15) as usize];
        bits >>= 4;
    }
    out
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
