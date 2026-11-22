//! Timestamp parsing for F1 / F2 / F3 forms in rules.md.

use std::cell::Cell;

pub const DAY_MS: i64 = 86_400_000;
pub const HOUR_MS: i64 = 3_600_000;
pub const MIN_MS: i64 = 60_000;
pub const GAP_MS: i64 = 1_800_000;
pub const WEEK_MS: i64 = 7 * DAY_MS;

// Nearby rows usually share a calendar day.
thread_local! {
    static DAY_CACHE: Cell<(i32, i32, i32, i64)> = const { Cell::new((0, 0, 0, 0)) };
}

#[inline]
pub fn parse(raw: &[u8]) -> Option<i64> {
    // Generator and fixtures are dominated by F1 (`...Z`). Keep that path tiny.
    if raw.len() == 20 && raw[19] == b'Z' && raw[10] == b'T' {
        return parse_civil(&raw[..19]);
    }
    if raw.len() == 25 && raw[10] == b'T' && (raw[19] == b'+' || raw[19] == b'-') && raw[22] == b':' {
        let local = parse_civil(&raw[..19])?;
        let oh = dig2(raw, 20)?;
        let om = dig2(raw, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = oh as i64 * HOUR_MS + om as i64 * MIN_MS;
        return Some(if raw[19] == b'+' { local - off } else { local + off });
    }
    if !raw.is_empty() && raw.iter().all(u8::is_ascii_digit) {
        return digits(raw);
    }
    None
}

#[inline]
fn parse_civil(raw: &[u8]) -> Option<i64> {
    if raw.len() != 19
        || raw[4] != b'-'
        || raw[7] != b'-'
        || raw[10] != b'T'
        || raw[13] != b':'
        || raw[16] != b':'
    {
        return None;
    }
    let y = dig4(raw, 0)?;
    let m = dig2(raw, 5)?;
    let d = dig2(raw, 8)?;
    let h = dig2(raw, 11)?;
    let mi = dig2(raw, 14)?;
    let s = dig2(raw, 17)?;
    if h > 23 || mi > 59 || s > 59 || !ymd_ok(y, m, d) {
        return None;
    }
    let days = DAY_CACHE.with(|cache| {
        let (cy, cm, cd, cdays) = cache.get();
        if cy == y && cm == m && cd == d {
            cdays
        } else {
            let days = days_from_civil(y, m, d);
            cache.set((y, m, d, days));
            days
        }
    });
    Some(days * DAY_MS + h as i64 * HOUR_MS + mi as i64 * MIN_MS + s as i64 * 1000)
}

fn ymd_ok(y: i32, m: i32, d: i32) -> bool {
    if !(1..=12).contains(&m) || d < 1 {
        return false;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let days = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    d <= days
}

#[inline(always)]
fn dig2(raw: &[u8], i: usize) -> Option<i32> {
    let a = raw[i].wrapping_sub(b'0');
    let b = raw[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        None
    } else {
        Some(a as i32 * 10 + b as i32)
    }
}

#[inline(always)]
fn dig4(raw: &[u8], i: usize) -> Option<i32> {
    let a = raw[i].wrapping_sub(b'0');
    let b = raw[i + 1].wrapping_sub(b'0');
    let c = raw[i + 2].wrapping_sub(b'0');
    let d = raw[i + 3].wrapping_sub(b'0');
    if a > 9 || b > 9 || c > 9 || d > 9 {
        None
    } else {
        Some(a as i32 * 1000 + b as i32 * 100 + c as i32 * 10 + d as i32)
    }
}

fn digits(raw: &[u8]) -> Option<i64> {
    let mut i = 0;
    while i < raw.len() && raw[i] == b'0' {
        i += 1;
    }
    let sig = &raw[i..];
    if sig.is_empty() {
        return Some(0);
    }
    if sig.len() > 19 || (sig.len() == 19 && sig > b"9223372036854775807".as_slice()) {
        return None;
    }
    let mut n = 0i64;
    for &b in sig {
        n = n * 10 + (b - b'0') as i64;
    }
    Some(n)
}

#[inline]
pub fn epoch_ms(y: i32, m: i32, d: i32, h: i32, mi: i32, s: i32) -> i64 {
    days_from_civil(y, m, d) * DAY_MS + h as i64 * HOUR_MS + mi as i64 * MIN_MS + s as i64 * 1000
}

pub fn days_from_civil(y: i32, m: i32, d: i32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u64;
    era as i64 * 146097 + doe as i64 - 719468
}

pub fn day_of(ms: i64) -> i32 {
    (ms.div_euclid(DAY_MS)) as i32
}
