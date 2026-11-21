//! Timestamp parsing for F1 / F2 / F3 forms in rules.md.

pub const DAY_MS: i64 = 86_400_000;
pub const HOUR_MS: i64 = 3_600_000;
pub const MIN_MS: i64 = 60_000;
pub const GAP_MS: i64 = 1_800_000;
pub const WEEK_MS: i64 = 7 * DAY_MS;

#[inline]
pub fn parse(raw: &[u8]) -> Option<i64> {
    if raw.len() == 20 && raw[19] == b'Z' && raw[10] == b'T' {
        let (y, m, d, h, mi, s) = clock(&raw[..19])?;
        return Some(epoch_ms(y, m, d, h, mi, s));
    }
    if raw.len() == 25 && raw[10] == b'T' && (raw[19] == b'+' || raw[19] == b'-') && raw[22] == b':' {
        let (y, m, d, h, mi, s) = clock(&raw[..19])?;
        let oh = two(&raw[20..22])?;
        let om = two(&raw[23..25])?;
        if oh > 23 || om > 59 {
            return None;
        }
        let local = epoch_ms(y, m, d, h, mi, s);
        let off = oh as i64 * HOUR_MS + om as i64 * MIN_MS;
        return Some(if raw[19] == b'+' { local - off } else { local + off });
    }
    if !raw.is_empty() && raw.iter().all(u8::is_ascii_digit) {
        return digits(raw);
    }
    None
}

fn clock(raw: &[u8]) -> Option<(i32, i32, i32, i32, i32, i32)> {
    if raw.len() != 19
        || raw[4] != b'-'
        || raw[7] != b'-'
        || raw[10] != b'T'
        || raw[13] != b':'
        || raw[16] != b':'
    {
        return None;
    }
    let y = four(&raw[0..4])?;
    let m = two(&raw[5..7])?;
    let d = two(&raw[8..10])?;
    let h = two(&raw[11..13])?;
    let mi = two(&raw[14..16])?;
    let s = two(&raw[17..19])?;
    if h > 23 || mi > 59 || s > 59 || !ymd_ok(y, m, d) {
        return None;
    }
    Some((y, m, d, h, mi, s))
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

fn two(raw: &[u8]) -> Option<i32> {
    if raw.len() == 2 && raw[0].is_ascii_digit() && raw[1].is_ascii_digit() {
        Some((raw[0] - b'0') as i32 * 10 + (raw[1] - b'0') as i32)
    } else {
        None
    }
}

fn four(raw: &[u8]) -> Option<i32> {
    if raw.len() == 4 && raw.iter().all(|b| b.is_ascii_digit()) {
        Some(raw.iter().fold(0, |n, b| n * 10 + (b - b'0') as i32))
    } else {
        None
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
