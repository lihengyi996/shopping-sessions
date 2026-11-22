//! CSV dialect from rules.md: `\n` ends a line, quotes only wrap whole fields.

use memchr::{memchr, memchr2};

#[inline]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut i = 0usize;
    for slot in 0..N {
        let Some((field, next)) = field(line, i) else {
            return false;
        };
        out[slot] = field;
        if slot + 1 == N {
            return next == line.len();
        }
        if next >= line.len() || line[next] != b',' {
            return false;
        }
        i = next + 1;
    }
    false
}

#[inline]
fn field<'a>(line: &'a [u8], i: usize) -> Option<(&'a [u8], usize)> {
    if i > line.len() {
        return None;
    }
    if i == line.len() {
        return Some((b"", i));
    }
    if line[i] == b'"' {
        let rel = memchr(b'"', &line[i + 1..])?;
        let j = i + 1 + rel;
        let after = j + 1;
        if after < line.len() && line[after] != b',' {
            return None;
        }
        return Some((&line[i + 1..j], after));
    }
    match memchr2(b',', b'"', &line[i..]) {
        Some(rel) if line[i + rel] == b',' => Some((&line[i..i + rel], i + rel)),
        Some(_) => None,
        None => Some((&line[i..], line.len())),
    }
}

/// Walk body lines of a CSV buffer (skip the header). `end` is exclusive; the
/// chunk may start mid-line, so we skip to the next `\n` first when needed.
pub fn for_body_lines(data: &[u8], start: usize, end: usize, mut each: impl FnMut(&[u8])) {
    let Some(mut at) = first_body(data, start) else {
        return;
    };
    while at < end {
        let stop = match memchr(b'\n', &data[at..]) {
            Some(i) => at + i,
            None => data.len(),
        };
        if stop > at {
            each(&data[at..stop]);
        }
        at = stop + 1;
    }
}

fn first_body(data: &[u8], start: usize) -> Option<usize> {
    if start == 0 {
        memchr(b'\n', data).map(|i| i + 1)
    } else if data.get(start - 1) == Some(&b'\n') {
        Some(start)
    } else {
        memchr(b'\n', &data[start..]).map(|i| start + i + 1)
    }
}
