//! Shopping Sessions pipeline.
//!
//! One process reads the CSVs, applies rules.md, and writes two CSV tables
//! under $SCRATCH_DIR. pipeline.py turns those into the Parquet the scorer reads.
//! Strings stay bytes so comparisons are byte-wise, which is what the rules require.

use rustc_hash::FxHashMap;
use std::env;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const VIEW: u8 = 0;
const CART: u8 = 1;
const PURCHASE: u8 = 2;
const BAD_KIND: u8 = 255;

struct Intern {
    map: FxHashMap<Vec<u8>, u32>,
    values: Vec<Vec<u8>>,
}

impl Intern {
    fn new() -> Self {
        Self { map: FxHashMap::default(), values: Vec::new() }
    }

    fn insert(&mut self, value: &[u8]) -> u32 {
        if let Some(&id) = self.map.get(value) {
            return id;
        }
        let id = self.values.len() as u32;
        self.values.push(value.to_vec());
        self.map.insert(self.values[id as usize].clone(), id);
        id
    }
}

struct Ev {
    ingest: i64,
    user: u32,
    ts: i64,
    product: u32,
    kind: u8,
    price: i64,
}

struct Cl {
    user: u32,
    campaign: u32,
    ts: i64,
    id: Vec<u8>,
}

struct Row {
    person: u32,
    ts: i64,
    kind: u8,
    cat: u32,
    price: i64,
}

struct Click {
    person: u32,
    ts: i64,
    campaign: u32,
    id: Vec<u8>,
}

#[derive(Clone, Copy, Default)]
struct UserAgg {
    sessions: i64,
    events: i64,
    revenue: i64,
}

#[derive(Clone, Copy, Default)]
struct CampAgg {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

fn main() {
    let data = env_path("DATA_DIR", "/data");
    let scratch = env_path("SCRATCH_DIR", "/scratch");
    if let Err(err) = run(&data, &scratch) {
        eprintln!("shopsess: {err}");
        std::process::exit(1);
    }
}

fn env_path(key: &str, default: &str) -> PathBuf {
    env::var_os(key).map(PathBuf::from).unwrap_or_else(|| PathBuf::from(default))
}

fn run(data: &Path, scratch: &Path) -> Result<(), String> {
    let manifest = fs::read(data.join("manifest.json")).map_err(|e| e.to_string())?;
    let (window_start, window_end) = parse_manifest(&manifest)?;

    let mut users = Intern::new();
    let mut products = Intern::new();
    let mut categories = Intern::new();
    let unknown = categories.insert(b"unknown");
    let mut product_cat: FxHashMap<u32, u32> = FxHashMap::default();
    for file in csv_files(&data.join("products")) {
        // products.csv lives beside the part directories, not inside one.
        let _ = file;
    }
    if data.join("products.csv").is_file() {
        for_lines(&fs::read(data.join("products.csv")).map_err(|e| e.to_string())?, |line| {
            let Some(fields) = split_fields(line, 2) else { return };
            let (Some(id), Some(cat)) = (fields[0], fields[1]) else { return };
            let pid = products.insert(id);
            let cid = categories.insert(cat);
            product_cat.insert(pid, cid);
        });
    }

    let mut campaigns = Intern::new();
    let mut campaign_channel: Vec<u32> = Vec::new();
    if data.join("campaigns.csv").is_file() {
        for_lines(&fs::read(data.join("campaigns.csv")).map_err(|e| e.to_string())?, |line| {
            let Some(fields) = split_fields(line, 2) else { return };
            let (Some(id), Some(channel)) = (fields[0], fields[1]) else { return };
            let cid = campaigns.insert(id);
            let ch = categories.insert(channel);
            if campaign_channel.len() <= cid as usize {
                campaign_channel.resize(cid as usize + 1, u32::MAX);
            }
            campaign_channel[cid as usize] = ch;
        });
    }
    let organic = campaigns.insert(b"organic");
    let organic_channel = categories.insert(b"organic");
    if campaign_channel.len() <= organic as usize {
        campaign_channel.resize(organic as usize + 1, u32::MAX);
    }
    campaign_channel[organic as usize] = organic_channel;

    let mut link_pairs: Vec<(u32, u32)> = Vec::new();
    for file in csv_files(&data.join("links")) {
        let bytes = fs::read(&file).map_err(|e| e.to_string())?;
        for_lines(&bytes, |line| {
            let Some(fields) = split_fields(line, 2) else { return };
            let (Some(a), Some(b)) = (fields[0], fields[1]) else { return };
            link_pairs.push((users.insert(a), users.insert(b)));
        });
    }

    let mut events: FxHashMap<Vec<u8>, Ev> = FxHashMap::default();
    for file in csv_files(&data.join("events")) {
        let bytes = fs::read(&file).map_err(|e| e.to_string())?;
        for_lines(&bytes, |line| {
            let Some(fields) = split_fields(line, 7) else { return };
            let Some(event_id) = fields[0] else { return };
            let user = fields[1].map(|v| users.insert(v)).unwrap_or(u32::MAX);
            let ts = fields[2].and_then(parse_ts).unwrap_or(i64::MIN);
            let ingest = fields[3].and_then(parse_ts).unwrap_or(i64::MAX);
            let kind = match fields[4] {
                Some(b"view") => VIEW,
                Some(b"cart") => CART,
                Some(b"purchase") => PURCHASE,
                _ => BAD_KIND,
            };
            let product = fields[5].map(|v| products.insert(v)).unwrap_or(u32::MAX);
            let price = if kind == PURCHASE {
                fields[6].and_then(parse_price).unwrap_or(-1)
            } else {
                0
            };
            let row = Ev { ingest, user, ts, product, kind, price };
            match events.get_mut(event_id) {
                Some(kept) if ingest < kept.ingest => *kept = row,
                Some(_) => {}
                None => {
                    events.insert(event_id.to_vec(), row);
                }
            }
        });
    }

    let mut clicks: FxHashMap<Vec<u8>, Cl> = FxHashMap::default();
    for file in csv_files(&data.join("clicks")) {
        let bytes = fs::read(&file).map_err(|e| e.to_string())?;
        for_lines(&bytes, |line| {
            let Some(fields) = split_fields(line, 4) else { return };
            let Some(click_id) = fields[0] else { return };
            if clicks.contains_key(click_id) {
                return;
            }
            let user = fields[1].map(|v| users.insert(v)).unwrap_or(u32::MAX);
            let campaign = match fields[2] {
                Some(name) => match campaigns.map.get(name) {
                    Some(&id) if id != organic => id,
                    _ => return,
                },
                None => return,
            };
            let Some(ts) = fields[3].and_then(parse_ts) else { return };
            clicks.insert(click_id.to_vec(), Cl { user, campaign, ts, id: click_id.to_vec() });
        });
    }

    let n = users.values.len();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    let mut rank = vec![0u8; n];
    let mut label: Vec<u32> = (0..n as u32).collect();
    for (a, b) in link_pairs {
        union(a, b, &mut parent, &mut rank, &mut label, &users.values);
    }
    let mut person_of = |mut node: u32| -> u32 {
        let mut root = node;
        while parent[root as usize] != root {
            root = parent[root as usize];
        }
        while parent[node as usize] != root {
            let next = parent[node as usize];
            parent[node as usize] = root;
            node = next;
        }
        label[root as usize]
    };

    let click_lo = window_start - WEEK_MS;
    let mut click_rows = Vec::with_capacity(clicks.len());
    for (_, click) in clicks {
        if click.user == u32::MAX || click.ts < click_lo || click.ts >= window_end {
            continue;
        }
        click_rows.push(Click {
            person: person_of(click.user),
            ts: click.ts,
            campaign: click.campaign,
            id: click.id,
        });
    }
    click_rows.sort_unstable_by(|a, b| {
        a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then(a.id.cmp(&b.id))
    });

    let mut rows = Vec::with_capacity(events.len());
    for (_, ev) in events {
        if ev.user == u32::MAX
            || ev.product == u32::MAX
            || ev.ts < window_start
            || ev.ts >= window_end
            || ev.kind > PURCHASE
            || ev.ingest > ev.ts.saturating_add(DAY_MS)
        {
            continue;
        }
        if ev.kind == PURCHASE && !(1..=10_000_000).contains(&ev.price) {
            continue;
        }
        let cat = product_cat.get(&ev.product).copied().unwrap_or(unknown);
        rows.push(Row { person: person_of(ev.user), ts: ev.ts, kind: ev.kind, cat, price: ev.price });
    }
    rows.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));

    let mut users_out: FxHashMap<(u32, i32), UserAgg> = FxHashMap::default();
    let mut camps: FxHashMap<(u32, i32), CampAgg> = FxHashMap::default();
    let mut cat_best: FxHashMap<(u32, i32), (u32, i64)> = FxHashMap::default();

    let mut click_at = 0usize;
    let mut i = 0usize;
    while i < rows.len() {
        let person = rows[i].person;
        let end = rows[i..].iter().position(|r| r.person != person).map(|p| i + p).unwrap_or(rows.len());
        while click_at < click_rows.len() && click_rows[click_at].person < person {
            click_at += 1;
        }
        let click_end = click_rows[click_at..]
            .iter()
            .position(|c| c.person != person)
            .map(|p| click_at + p)
            .unwrap_or(click_rows.len());
        let person_clicks = &click_rows[click_at..click_end];

        let mut s = i;
        while s < end {
            let start = rows[s].ts;
            let day = (start / DAY_MS) as i32;
            let mut last_ts = start;
            let mut k = s + 1;
            while k < end {
                let ts = rows[k].ts;
                if ts != last_ts && ts - last_ts > GAP_MS {
                    break;
                }
                if ts != last_ts {
                    last_ts = ts;
                }
                k += 1;
            }
            let mut events_n = 0i64;
            let mut purchases = 0i64;
            let mut revenue = 0i64;
            for row in &rows[s..k] {
                events_n += 1;
                if row.kind == PURCHASE {
                    purchases += 1;
                    revenue += row.price;
                    let slot = cat_best.entry((person, day)).or_insert((row.cat, 0));
                    // Category totals are finished after the person is done; stash
                    // the running sum in a side map below.
                    let _ = slot;
                }
            }
            let agg = users_out.entry((person, day)).or_default();
            agg.sessions += 1;
            agg.events += events_n;
            agg.revenue += revenue;

            let lo = start - WEEK_MS;
            let from = person_clicks.partition_point(|c| c.ts < lo);
            let to = person_clicks.partition_point(|c| c.ts <= start);
            let window = &person_clicks[from..to];
            let credited = if window.is_empty() {
                organic
            } else {
                let max_ts = window[window.len() - 1].ts;
                let mut idx = window.len() - 1;
                while idx > 0 && window[idx - 1].ts == max_ts {
                    idx -= 1;
                }
                window[idx].campaign
            };
            let camp = camps.entry((credited, day)).or_default();
            camp.sessions += 1;
            if purchases > 0 {
                camp.purchasing += 1;
            }
            camp.revenue += revenue;
            if !window.is_empty() {
                let mut seen: Vec<u32> = window.iter().map(|c| c.campaign).collect();
                seen.sort_unstable();
                seen.dedup();
                for campaign in seen {
                    camps.entry((campaign, day)).or_default().assisted += 1;
                }
            }
            s = k;
        }
        i = end;
        click_at = click_end;
    }

    // Category totals were not accumulated above; do that in one pass now.
    // The placeholder loop kept the session walk readable. Rebuild from rows.
    cat_best.clear();
    let mut cat_rev: FxHashMap<(u32, i32, u32), i64> = FxHashMap::default();
    // Recompute day from the session start, not the event timestamp. Purchases
    // after midnight still belong to the session's start day, so walk sessions again.
    i = 0;
    while i < rows.len() {
        let person = rows[i].person;
        let end = rows[i..].iter().position(|r| r.person != person).map(|p| i + p).unwrap_or(rows.len());
        let mut s = i;
        while s < end {
            let start = rows[s].ts;
            let day = (start / DAY_MS) as i32;
            let mut last_ts = start;
            let mut k = s + 1;
            while k < end {
                let ts = rows[k].ts;
                if ts != last_ts && ts - last_ts > GAP_MS {
                    break;
                }
                if ts != last_ts {
                    last_ts = ts;
                }
                k += 1;
            }
            for row in &rows[s..k] {
                if row.kind == PURCHASE {
                    *cat_rev.entry((person, day, row.cat)).or_insert(0) += row.price;
                }
            }
            s = k;
        }
        i = end;
    }
    for ((person, day, cat), rev) in cat_rev {
        let slot = cat_best.entry((person, day)).or_insert((cat, rev));
        if rev > slot.1 || (rev == slot.1 && categories.values[cat as usize] < categories.values[slot.0 as usize]) {
            *slot = (cat, rev);
        }
    }

    fs::create_dir_all(scratch).map_err(|e| e.to_string())?;
    write_user_csv(&scratch.join("user_daily.csv"), &users_out, &cat_best, &users.values, &categories.values)?;
    write_camp_csv(&scratch.join("campaign_daily.csv"), &camps, &campaigns.values, &campaign_channel, &categories.values)?;
    Ok(())
}

fn write_user_csv(
    path: &Path,
    rows: &FxHashMap<(u32, i32), UserAgg>,
    cats: &FxHashMap<(u32, i32), (u32, i64)>,
    users: &[Vec<u8>],
    categories: &[Vec<u8>],
) -> Result<(), String> {
    let mut file = BufWriter::new(fs::File::create(path).map_err(|e| e.to_string())?);
    file.write_all(b"user_id,day,sessions,events,revenue_cents,top_category\n").map_err(|e| e.to_string())?;
    for (&(person, day), agg) in rows {
        write_quoted(&mut file, &users[person as usize])?;
        let (y, m, d) = civil_from_days(day);
        write!(file, ",{y:04}-{m:02}-{d:02},{},{},{},", agg.sessions, agg.events, agg.revenue).map_err(|e| e.to_string())?;
        if let Some(&(cat, _)) = cats.get(&(person, day)) {
            write_quoted(&mut file, &categories[cat as usize])?;
        }
        file.write_all(b"\n").map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn write_camp_csv(
    path: &Path,
    rows: &FxHashMap<(u32, i32), CampAgg>,
    campaigns: &[Vec<u8>],
    channels: &[u32],
    categories: &[Vec<u8>],
) -> Result<(), String> {
    let mut file = BufWriter::new(fs::File::create(path).map_err(|e| e.to_string())?);
    file.write_all(b"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")
        .map_err(|e| e.to_string())?;
    for (&(campaign, day), agg) in rows {
        if agg.sessions == 0 && agg.assisted == 0 {
            continue;
        }
        write_quoted(&mut file, &campaigns[campaign as usize])?;
        file.write_all(b",").map_err(|e| e.to_string())?;
        let channel = channels.get(campaign as usize).copied().unwrap_or(u32::MAX);
        if channel == u32::MAX {
            write_quoted(&mut file, b"organic")?;
        } else {
            write_quoted(&mut file, &categories[channel as usize])?;
        }
        let (y, m, d) = civil_from_days(day);
        let assisted = if campaigns[campaign as usize] == b"organic" { 0 } else { agg.assisted };
        write!(
            file,
            ",{y:04}-{m:02}-{d:02},{},{},{},{}\n",
            agg.sessions, agg.purchasing, agg.revenue, assisted
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn write_quoted(file: &mut impl Write, value: &[u8]) -> Result<(), String> {
    file.write_all(b"\"").map_err(|e| e.to_string())?;
    for &byte in value {
        if byte == b'"' {
            file.write_all(b"\"\"").map_err(|e| e.to_string())?;
        } else {
            file.write_all(&[byte]).map_err(|e| e.to_string())?;
        }
    }
    file.write_all(b"\"").map_err(|e| e.to_string())?;
    Ok(())
}

fn union(a: u32, b: u32, parent: &mut [u32], rank: &mut [u8], label: &mut [u32], names: &[Vec<u8>]) {
    let mut find = |mut node: u32| -> u32 {
        let mut root = node;
        while parent[root as usize] != root {
            root = parent[root as usize];
        }
        while parent[node as usize] != root {
            let next = parent[node as usize];
            parent[node as usize] = root;
            node = next;
        }
        root
    };
    let ra = find(a);
    let rb = find(b);
    if ra == rb {
        return;
    }
    let (ra, rb) = if rank[ra as usize] < rank[rb as usize] { (rb, ra) } else { (ra, rb) };
    parent[rb as usize] = ra;
    if rank[ra as usize] == rank[rb as usize] {
        rank[ra as usize] = rank[ra as usize].saturating_add(1);
    }
    if names[label[rb as usize] as usize] < names[label[ra as usize] as usize] {
        label[ra as usize] = label[rb as usize];
    }
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else { return files };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else { continue };
        if path.is_file() && name.ends_with(".csv") && !name.starts_with('.') && !name.starts_with('_') {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn for_lines(bytes: &[u8], mut each: impl FnMut(&[u8])) {
    let mut start = match bytes.iter().position(|&b| b == b'\n') {
        Some(i) => i + 1,
        None => return,
    };
    while start < bytes.len() {
        let mut end = start;
        while end < bytes.len() && bytes[end] != b'\n' {
            end += 1;
        }
        if end > start {
            each(&bytes[start..end]);
        }
        start = end + 1;
    }
}

fn split_fields(line: &[u8], n: usize) -> Option<[Option<&[u8]>; 7]> {
    let mut out = [None; 7];
    let mut i = 0usize;
    for slot in 0..n {
        let (value, next) = read_field(line, i)?;
        out[slot] = value;
        if slot + 1 == n {
            if next != line.len() {
                return None;
            }
        } else if next >= line.len() || line[next] != b',' {
            return None;
        } else {
            i = next + 1;
        }
    }
    Some(out)
}

fn read_field(line: &[u8], i: usize) -> Option<(Option<&[u8]>, usize)> {
    if i > line.len() {
        return None;
    }
    if i == line.len() {
        return Some((None, i));
    }
    if line[i] == b'"' {
        let mut j = i + 1;
        while j < line.len() && line[j] != b'"' {
            j += 1;
        }
        if j >= line.len() {
            return None;
        }
        let after = j + 1;
        if after < line.len() && line[after] != b',' {
            return None;
        }
        let inner = &line[i + 1..j];
        return Some((if inner.is_empty() { None } else { Some(inner) }, after));
    }
    let mut j = i;
    while j < line.len() && line[j] != b',' {
        if line[j] == b'"' {
            return None;
        }
        j += 1;
    }
    let inner = &line[i..j];
    Some((if inner.is_empty() { None } else { Some(inner) }, j))
}

fn parse_ts(raw: &[u8]) -> Option<i64> {
    if raw.len() == 20 && raw[19] == b'Z' && raw[10] == b'T' {
        let (y, m, d, h, mi, s) = parse_clock(&raw[..19])?;
        return Some(epoch_ms(y, m, d, h, mi, s));
    }
    if raw.len() == 25 && raw[10] == b'T' && (raw[19] == b'+' || raw[19] == b'-') && raw[22] == b':' {
        let (y, m, d, h, mi, s) = parse_clock(&raw[..19])?;
        let oh = two_digits(&raw[20..22])?;
        let om = two_digits(&raw[23..25])?;
        if oh > 23 || om > 59 {
            return None;
        }
        let local = epoch_ms(y, m, d, h, mi, s);
        let off = oh as i64 * 3_600_000 + om as i64 * 60_000;
        return Some(if raw[19] == b'+' { local - off } else { local + off });
    }
    if raw.iter().all(|b| b.is_ascii_digit()) {
        return parse_i64(raw);
    }
    None
}

fn parse_clock(raw: &[u8]) -> Option<(i32, i32, i32, i32, i32, i32)> {
    if raw.len() != 19 || raw[4] != b'-' || raw[7] != b'-' || raw[10] != b'T' || raw[13] != b':' || raw[16] != b':' {
        return None;
    }
    let y = four_digits(&raw[0..4])?;
    let m = two_digits(&raw[5..7])?;
    let d = two_digits(&raw[8..10])?;
    let h = two_digits(&raw[11..13])?;
    let mi = two_digits(&raw[14..16])?;
    let s = two_digits(&raw[17..19])?;
    if h > 23 || mi > 59 || s > 59 || !ymd_valid(y, m, d) {
        return None;
    }
    Some((y, m, d, h, mi, s))
}

fn ymd_valid(y: i32, m: i32, d: i32) -> bool {
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

fn two_digits(raw: &[u8]) -> Option<i32> {
    if raw.len() == 2 && raw[0].is_ascii_digit() && raw[1].is_ascii_digit() {
        Some((raw[0] - b'0') as i32 * 10 + (raw[1] - b'0') as i32)
    } else {
        None
    }
}

fn four_digits(raw: &[u8]) -> Option<i32> {
    if raw.len() == 4 && raw.iter().all(|b| b.is_ascii_digit()) {
        Some(raw.iter().fold(0, |n, b| n * 10 + (b - b'0') as i32))
    } else {
        None
    }
}

fn parse_i64(raw: &[u8]) -> Option<i64> {
    if raw.is_empty() || raw.iter().any(|b| !b.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < raw.len() && raw[i] == b'0' {
        i += 1;
    }
    let sig = &raw[i..];
    if sig.is_empty() {
        return Some(0);
    }
    if sig.len() > 19 || (sig.len() == 19 && sig > b"9223372036854775807") {
        return None;
    }
    let mut n: i64 = 0;
    for &b in sig {
        n = n * 10 + (b - b'0') as i64;
    }
    Some(n)
}

fn parse_price(raw: &[u8]) -> Option<i64> {
    let n = parse_i64(raw)?;
    if (1..=10_000_000).contains(&n) { Some(n) } else { None }
}

fn epoch_ms(y: i32, m: i32, d: i32, h: i32, mi: i32, s: i32) -> i64 {
    days_from_civil(y, m, d) * DAY_MS + h as i64 * 3_600_000 + mi as i64 * 60_000 + s as i64 * 1000
}

fn days_from_civil(y: i32, m: i32, d: i32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u64;
    era as i64 * 146097 + doe as i64 - 719468
}

fn civil_from_days(day: i32) -> (i32, u32, u32) {
    let z = day as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

fn parse_manifest(bytes: &[u8]) -> Result<(i64, i64), String> {
    let text = std::str::from_utf8(bytes).map_err(|_| "manifest is not utf-8".to_string())?;
    let start = json_string(text, "window_start").ok_or("manifest missing window_start")?;
    let end = json_string(text, "window_end").ok_or("manifest missing window_end")?;
    let start = parse_ts(start.as_bytes()).ok_or("bad window_start")?;
    let end = parse_ts(end.as_bytes()).ok_or("bad window_end")?;
    Ok((start, end))
}

fn json_string<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("\"{key}\"");
    let at = text.find(&needle)? + needle.len();
    let rest = text[at..].trim_start().strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(&rest[..end])
}
