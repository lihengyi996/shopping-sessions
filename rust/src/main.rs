//! Shopping Sessions — native Rust pipeline.
//!
//! Ideas adapted from fast public track entries (parallel mmap scans, hash
//! shards for dedup, packed IDs, dense campaign grids, direct Parquet, a
//! static musl image): implemented here without copying their sources.

mod csv;
mod ids;
mod out;
mod scan;
mod ts;

use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Mutex;

use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use mimalloc::MiMalloc;

use ids::Id;
use out::{CampCell, UserRow};
use ts::{day_of, DAY_MS, GAP_MS, WEEK_MS};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

const EVENT_BITS: u32 = 10;
const EVENT_SHARDS: usize = 1 << EVENT_BITS;
const PERSON_BITS: u32 = 8;
const PERSON_SHARDS: usize = 1 << PERSON_BITS;

const PRICE_BITS: u32 = 24;
const PRICE_MASK: u32 = (1 << PRICE_BITS) - 1;

#[derive(Clone, Copy)]
struct RawEvent {
    event: Id,
    ingest: i64,
    user: Id,
    /// Milliseconds from window_start; only meaningful when `ok`.
    ts: i32,
    /// 0 = not a purchase / invalid purchase marker handled via ok flag.
    sale: u32,
    ok: bool,
}

#[derive(Clone, Copy)]
struct Event {
    person: Id,
    ts: i32,
    sale: u32,
}

#[derive(Clone, Copy)]
struct Click {
    person: Id,
    ts: i32,
    campaign: u16,
    id: Id,
}

struct Window {
    start: i64,
    end: i64,
}

struct Products {
    /// product Id -> category index
    map: HashMap<Id, u16>,
    categories: Vec<Vec<u8>>,
    /// byte-rank of each category for tie breaks
    rank: Vec<u16>,
    unknown: u16,
}

struct Campaigns {
    map: HashMap<Id, u16>,
    ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    organic: u16,
}

struct People {
    /// Linked user Id -> person name Id (only non-identity entries).
    rename: HashMap<Id, Id>,
}

impl People {
    fn build(pairs: Vec<(Id, Id)>) -> Self {
        let mut index: HashMap<Id, u32> = HashMap::with_capacity(pairs.len() * 2);
        let mut keys = Vec::new();
        let mut parent = Vec::new();
        let mut intern = |id: Id, keys: &mut Vec<Id>, parent: &mut Vec<u32>| -> u32 {
            *index.entry(id).or_insert_with(|| {
                let i = keys.len() as u32;
                keys.push(id);
                parent.push(i);
                i
            })
        };
        for (a, b) in pairs {
            let a = intern(a, &mut keys, &mut parent);
            let b = intern(b, &mut keys, &mut parent);
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
        let mut best: Vec<u32> = (0..keys.len() as u32).collect();
        for i in 0..keys.len() as u32 {
            let root = find(&mut parent, i) as usize;
            if keys[i as usize].ord(keys[best[root] as usize]) == Ordering::Less {
                best[root] = i;
            }
        }
        let mut rename = HashMap::new();
        for i in 0..keys.len() as u32 {
            let root = find(&mut parent, i) as usize;
            let name = keys[best[root] as usize];
            if name != keys[i as usize] {
                rename.insert(keys[i as usize], name);
            }
        }
        People { rename }
    }

    #[inline]
    fn name_of(&self, id: Id) -> Id {
        self.rename.get(&id).copied().unwrap_or(id)
    }
}

fn find(parent: &mut [u32], mut i: u32) -> u32 {
    while parent[i as usize] != i {
        let g = parent[parent[i as usize] as usize];
        parent[i as usize] = g;
        i = g;
    }
    i
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => {
            // Skip tearing down multi-GB arenas; output is already on disk.
            std::process::exit(0);
        }
        Err(err) => {
            eprintln!("shopsess: {err}");
            ExitCode::FAILURE
        }
    }
}

fn env_path(name: &str, default: &str) -> PathBuf {
    PathBuf::from(std::env::var_os(name).unwrap_or_else(|| default.into()))
}

fn threads() -> usize {
    std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
}

fn run() -> Result<(), String> {
    let data = env_path("DATA_DIR", "/data");
    let out = env_path("OUTPUT_DIR", "/output");
    let ncpu = threads();

    let window = read_manifest(&data)?;
    let products = read_products(&data)?;
    let campaigns = read_campaigns(&data)?;
    let user_dir = out.join("user_daily");
    let camp_dir = out.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&camp_dir).map_err(|e| e.to_string())?;

    let link_files = scan::csv_files(&data.join("links")).map_err(|e| e.to_string())?;
    let event_files = scan::csv_files(&data.join("events")).map_err(|e| e.to_string())?;
    let click_files = scan::csv_files(&data.join("clicks")).map_err(|e| e.to_string())?;

    let (people, event_shards) = std::thread::scope(|scope| -> Result<(People, Vec<Vec<RawEvent>>), String> {
        let links = scope.spawn(|| collect_links(&link_files, ncpu));
        let events = scope.spawn(|| parse_events(&event_files, ncpu, &window, &products));
        let pairs = links.join().unwrap()?;
        let people = People::build(pairs);
        let shards = events.join().unwrap()?;
        Ok((people, shards))
    })?;

    let event_buckets = dedup_events(event_shards, ncpu, &people);
    let click_buckets = parse_clicks(&click_files, ncpu, &window, &campaigns, &people)?;

    let base_day = day_of(window.start);
    let days = (day_of(window.end - 1) - base_day + 1) as usize;
    let n_campaigns = campaigns.ids.len();

    let mut click_parts = click_buckets.into_iter();
    let work: Vec<Mutex<Option<(Vec<Vec<Event>>, Vec<Vec<Click>>)>>> = event_buckets
        .into_iter()
        .map(|ev| Mutex::new(Some((ev, click_parts.next().unwrap_or_default()))))
        .collect();
    let next = AtomicUsize::new(0);

    let results: Vec<Result<(Vec<CampCell>, Vec<UserRow>), String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..ncpu)
            .map(|tid| {
                let (work, next, days, n_campaigns, products, user_dir) =
                    (&work, &next, days, n_campaigns, &products, &user_dir);
                scope.spawn(move || -> Result<(Vec<CampCell>, Vec<UserRow>), String> {
                    let mut cells = vec![CampCell::default(); n_campaigns * days];
                    let mut rows = Vec::new();
                    loop {
                        let b = next.fetch_add(1, AtomicOrdering::Relaxed);
                        if b >= work.len() {
                            break;
                        }
                        let Some((events, clicks)) = work[b].lock().unwrap().take() else {
                            continue;
                        };
                        run_person_shard(&events, &clicks, days, base_day, window.start, products, &mut cells, &mut rows);
                    }
                    if tid == 0 || !rows.is_empty() {
                        let path = user_dir.join(format!("part-{tid:03}.parquet"));
                        out::write_user_daily(&path, &rows).map_err(|e| e.to_string())?;
                    }
                    Ok((cells, rows))
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut total = vec![CampCell::default(); n_campaigns * days];
    for result in results {
        let (cells, _) = result?;
        for (dst, src) in total.iter_mut().zip(&cells) {
            dst.sessions += src.sessions;
            dst.purchasing += src.purchasing;
            dst.revenue += src.revenue;
            dst.assisted += src.assisted;
        }
    }

    out::write_campaign_daily(
        &camp_dir.join("part-000.parquet"),
        &total,
        days,
        &campaigns.ids,
        &campaigns.channels,
        base_day,
        campaigns.organic as usize,
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn collect_links(files: &[PathBuf], ncpu: usize) -> Result<Vec<(Id, Id)>, String> {
    let states = scan::for_chunks(files, ncpu, Vec::new, |out: &mut Vec<(Id, Id)>, data, start, end| {
        csv::for_body_lines(data, start, end, |line| {
            let mut fields = [b"" as &[u8]; 2];
            if !csv::split(line, &mut fields) {
                return;
            }
            if fields[0].is_empty() || fields[1].is_empty() {
                return;
            }
            out.push((Id::of(fields[0]), Id::of(fields[1])));
        });
    })
    .map_err(|e| e.to_string())?;
    Ok(states.into_iter().flatten().collect())
}

fn parse_events(
    files: &[PathBuf],
    ncpu: usize,
    window: &Window,
    products: &Products,
) -> Result<Vec<Vec<RawEvent>>, String> {
    let per = 64usize;
    let states = scan::for_chunks(
        files,
        ncpu,
        || (0..EVENT_SHARDS).map(|_| Vec::with_capacity(per)).collect::<Vec<_>>(),
        |shards: &mut Vec<Vec<RawEvent>>, data, start, end| {
            csv::for_body_lines(data, start, end, |line| {
                let mut fields = [b"" as &[u8]; 7];
                if !csv::split(line, &mut fields) || fields[0].is_empty() {
                    return;
                }
                let Some(raw) = decode_event(&fields, window, products) else {
                    return;
                };
                let s = raw.event.shard(EVENT_BITS);
                shards[s].push(raw);
            });
        },
    )
    .map_err(|e| e.to_string())?;

    let mut merged: Vec<Vec<RawEvent>> = (0..EVENT_SHARDS).map(|_| Vec::new()).collect();
    for state in states {
        for (s, part) in state.into_iter().enumerate() {
            if !part.is_empty() {
                merged[s].extend(part);
            }
        }
    }
    Ok(merged)
}

fn decode_event(fields: &[&[u8]; 7], window: &Window, products: &Products) -> Option<RawEvent> {
    let event = Id::of(fields[0]);
    let ingest = ts::parse(fields[3]).unwrap_or(i64::MAX);
    let user_raw = fields[1];
    let product_raw = fields[5];
    let (typed, purchase) = match fields[4] {
        b"view" | b"cart" => (true, false),
        b"purchase" => (true, true),
        _ => (false, false),
    };
    let mut ok = !user_raw.is_empty() && !product_raw.is_empty() && typed;
    let ts = match ts::parse(fields[2]) {
        Some(t) if t >= window.start && t < window.end => {
            ok &= ingest.saturating_sub(t) <= DAY_MS;
            (t - window.start) as i32
        }
        _ => {
            ok = false;
            0
        }
    };
    let mut sale = 0u32;
    if purchase && ok {
        match parse_price(fields[6]) {
            Some(price) => {
                let pid = Id::of(product_raw);
                let cat = products.map.get(&pid).copied().unwrap_or(products.unknown);
                sale = price | (u32::from(cat) << PRICE_BITS);
            }
            None => ok = false,
        }
    }
    Some(RawEvent {
        event,
        ingest,
        user: if ok { Id::of(user_raw) } else { Id::of(b"") },
        ts,
        sale,
        ok,
    })
}

fn parse_price(raw: &[u8]) -> Option<u32> {
    if raw.is_empty() {
        return None;
    }
    let mut n: u64 = 0;
    for &b in raw {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n * 10 + u64::from(b - b'0');
        if n > 10_000_000 {
            return None;
        }
    }
    (1..=10_000_000).contains(&n).then_some(n as u32)
}

fn dedup_events(shards: Vec<Vec<RawEvent>>, ncpu: usize, people: &People) -> Vec<Vec<Vec<Event>>> {
    let shard_lock: Vec<Mutex<Vec<RawEvent>>> = shards.into_iter().map(Mutex::new).collect();
    let next = AtomicUsize::new(0);
    let outs: Vec<Vec<Vec<Vec<Event>>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..ncpu.max(1))
            .map(|_| {
                scope.spawn(|| {
                    let mut open: Vec<Vec<Event>> = (0..PERSON_SHARDS).map(|_| Vec::new()).collect();
                    let mut full: Vec<Vec<Vec<Event>>> = (0..PERSON_SHARDS).map(|_| Vec::new()).collect();
                    let mut kept: HashMap<Id, (i64, usize)> = HashMap::new();
                    loop {
                        let s = next.fetch_add(1, AtomicOrdering::Relaxed);
                        if s >= shard_lock.len() {
                            break;
                        }
                        let part = std::mem::take(&mut *shard_lock[s].lock().unwrap());
                        kept.clear();
                        kept.reserve(part.len());
                        for (i, raw) in part.iter().enumerate() {
                            match kept.entry(raw.event) {
                                Entry::Vacant(v) => {
                                    v.insert((raw.ingest, i));
                                }
                                Entry::Occupied(mut o) => {
                                    if raw.ingest < o.get().0 {
                                        o.insert((raw.ingest, i));
                                    }
                                }
                            }
                        }
                        for &(_, i) in kept.values() {
                            let raw = &part[i];
                            if !raw.ok {
                                continue;
                            }
                            let person = people.name_of(raw.user);
                            let b = person.shard(PERSON_BITS);
                            let cur = &mut open[b];
                            if cur.len() == cur.capacity() && !cur.is_empty() {
                                let filled = std::mem::replace(cur, Vec::with_capacity(256));
                                full[b].push(filled);
                            }
                            cur.push(Event { person, ts: raw.ts, sale: raw.sale });
                        }
                    }
                    for (b, cur) in open.into_iter().enumerate() {
                        if !cur.is_empty() {
                            full[b].push(cur);
                        }
                    }
                    full
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut buckets: Vec<Vec<Vec<Event>>> = (0..PERSON_SHARDS).map(|_| Vec::new()).collect();
    for state in outs {
        for (b, parts) in state.into_iter().enumerate() {
            buckets[b].extend(parts);
        }
    }
    buckets
}

fn parse_clicks(
    files: &[PathBuf],
    ncpu: usize,
    window: &Window,
    campaigns: &Campaigns,
    people: &People,
) -> Result<Vec<Vec<Vec<Click>>>, String> {
    let click_lo = window.start - WEEK_MS;
    let states = scan::for_chunks(
        files,
        ncpu,
        || (0..PERSON_SHARDS).map(|_| Vec::new()).collect::<Vec<_>>(),
        |shards: &mut Vec<Vec<Click>>, data, start, end| {
            let mut seen: HashMap<Id, ()> = HashMap::new();
            csv::for_body_lines(data, start, end, |line| {
                let mut fields = [b"" as &[u8]; 4];
                if !csv::split(line, &mut fields) || fields[0].is_empty() {
                    return;
                }
                let cid = Id::of(fields[0]);
                if seen.insert(cid, ()).is_some() {
                    return;
                }
                if fields[1].is_empty() {
                    return;
                }
                let Some(&campaign) = campaigns.map.get(&Id::of(fields[2])) else {
                    return;
                };
                if campaign == campaigns.organic {
                    return;
                }
                let Some(ts_abs) = ts::parse(fields[3]) else {
                    return;
                };
                if ts_abs < click_lo || ts_abs >= window.end {
                    return;
                }
                let person = people.name_of(Id::of(fields[1]));
                let b = person.shard(PERSON_BITS);
                shards[b].push(Click {
                    person,
                    ts: (ts_abs - window.start) as i32,
                    campaign,
                    id: cid,
                });
            });
        },
    )
    .map_err(|e| e.to_string())?;

    // Click dedup is per click_id globally. Thread-local seen only catches
    // duplicates inside one worker's chunks; finish with a global pass per
    // person shard (click_ids rarely collide across people, but must be exact).
    let mut buckets: Vec<Vec<Vec<Click>>> = (0..PERSON_SHARDS).map(|_| Vec::new()).collect();
    let mut global: HashMap<Id, Click> = HashMap::new();
    for state in states {
        for part in state {
            for click in part {
                global.entry(click.id).or_insert(click);
            }
        }
    }
    for click in global.into_values() {
        let b = click.person.shard(PERSON_BITS);
        if buckets[b].is_empty() {
            buckets[b].push(Vec::new());
        }
        buckets[b][0].push(click);
    }
    Ok(buckets)
}

fn run_person_shard(
    event_parts: &[Vec<Event>],
    click_parts: &[Vec<Click>],
    days: usize,
    base_day: i32,
    origin: i64,
    products: &Products,
    cells: &mut [CampCell],
    rows: &mut Vec<UserRow>,
) {
    let mut by_person: HashMap<Id, (Vec<Event>, Vec<Click>)> = HashMap::new();
    for part in event_parts {
        for &e in part {
            by_person.entry(e.person).or_default().0.push(e);
        }
    }
    for part in click_parts {
        for &c in part {
            if let Some(slot) = by_person.get_mut(&c.person) {
                slot.1.push(c);
            }
        }
    }
    for (person, (mut events, mut clicks)) in by_person {
        events.sort_unstable_by_key(|e| e.ts);
        clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.id.ord(b.id)));
        sessionize(person, &events, &clicks, days, base_day, origin, products, cells, rows);
    }
}

fn sessionize(
    person: Id,
    events: &[Event],
    clicks: &[Click],
    days: usize,
    base_day: i32,
    origin: i64,
    products: &Products,
    cells: &mut [CampCell],
    rows: &mut Vec<UserRow>,
) {
    if events.is_empty() {
        return;
    }
    let mut day_sessions = vec![0i64; days];
    let mut day_events = vec![0i64; days];
    let mut day_revenue = vec![0i64; days];
    let mut day_cats: Vec<Vec<(u16, i64)>> = (0..days).map(|_| Vec::new()).collect();
    let mut used = Vec::new();
    let mut sessions: Vec<(i32, usize)> = Vec::new();

    let mut k = 0;
    while k < events.len() {
        let start = events[k].ts;
        let day = (day_of(origin + i64::from(start)) - base_day) as usize;
        if day_sessions[day] == 0 {
            used.push(day);
        }
        let mut prev = start;
        let mut m = k;
        let mut revenue = 0i64;
        while m < events.len() {
            let ts = events[m].ts;
            if i64::from(ts) - i64::from(prev) > GAP_MS {
                break;
            }
            prev = ts;
            let sale = events[m].sale;
            if sale != 0 {
                let price = i64::from(sale & PRICE_MASK);
                let cat = (sale >> PRICE_BITS) as u16;
                revenue += price;
                match day_cats[day].iter_mut().find(|(c, _)| *c == cat) {
                    Some(slot) => slot.1 += price,
                    None => day_cats[day].push((cat, price)),
                }
            }
            m += 1;
        }
        let n = (m - k) as i64;
        day_sessions[day] += 1;
        day_events[day] += n;
        day_revenue[day] += revenue;

        let credited = credit_campaign(clicks, start);
        let cell = &mut cells[credited as usize * days + day];
        cell.sessions += 1;
        if revenue > 0 {
            cell.purchasing += 1;
            cell.revenue += revenue;
        }
        sessions.push((start, day));
        k = m;
    }

    if !clicks.is_empty() {
        assist(clicks, &sessions, days, cells);
    }

    for day in used {
        let mut top = None;
        let mut best = 0i64;
        for &(cat, sum) in &day_cats[day] {
            let better = match top {
                None => true,
                Some(cur) => {
                    sum > best
                        || (sum == best && products.rank[cat as usize] < products.rank[cur as usize])
                }
            };
            if better {
                top = Some(cat);
                best = sum;
            }
        }
        rows.push(UserRow {
            person,
            day: base_day + day as i32,
            sessions: day_sessions[day],
            events: day_events[day],
            revenue: day_revenue[day],
            top: top.map(|c| products.categories[c as usize].clone()),
        });
    }
}

fn credit_campaign(clicks: &[Click], start: i32) -> u16 {
    // Latest click with ts <= start and start - ts <= WEEK; ties -> smallest id
    // (clicks already sorted by ts then id). Index 0 is always `organic`.
    let week = WEEK_MS as i32;
    let i = clicks.partition_point(|c| c.ts <= start);
    if i == 0 {
        return 0;
    }
    let t = clicks[i - 1].ts;
    if start - t > week {
        return 0;
    }
    let j = clicks.partition_point(|c| c.ts < t);
    clicks[j].campaign
}

fn assist(clicks: &[Click], sessions: &[(i32, usize)], days: usize, cells: &mut [CampCell]) {
    let week = WEEK_MS as i32;
    let mut by_camp: Vec<(u16, i32)> = clicks.iter().map(|c| (c.campaign, c.ts)).collect();
    by_camp.sort_unstable();
    let mut day_lo = vec![0usize; days + 1];
    for d in 0..=days {
        day_lo[d] = sessions.partition_point(|s| s.1 < d);
    }
    let mut a = 0;
    while a < by_camp.len() {
        let campaign = by_camp[a].0;
        let mut lo_t = by_camp[a].1;
        let mut hi_t = lo_t + week;
        a += 1;
        loop {
            while a < by_camp.len() && by_camp[a].0 == campaign && by_camp[a].1 <= hi_t {
                hi_t = by_camp[a].1 + week;
                a += 1;
            }
            let lo = sessions.partition_point(|s| s.0 < lo_t);
            let hi = sessions.partition_point(|s| s.0 <= hi_t);
            if lo < hi {
                for d in sessions[lo].1..=sessions[hi - 1].1 {
                    let n = hi.min(day_lo[d + 1]).saturating_sub(lo.max(day_lo[d]));
                    if n > 0 {
                        cells[campaign as usize * days + d].assisted += n as i64;
                    }
                }
            }
            if a < by_camp.len() && by_camp[a].0 == campaign {
                lo_t = by_camp[a].1;
                hi_t = lo_t + week;
                a += 1;
            } else {
                break;
            }
        }
    }
}

fn read_manifest(data: &Path) -> Result<Window, String> {
    let bytes = std::fs::read(data.join("manifest.json")).map_err(|e| e.to_string())?;
    let text = std::str::from_utf8(&bytes).map_err(|_| "manifest is not utf-8".to_string())?;
    let start = json_string(text, "window_start").ok_or("manifest missing window_start")?;
    let end = json_string(text, "window_end").ok_or("manifest missing window_end")?;
    Ok(Window {
        start: ts::parse(start.as_bytes()).ok_or("bad window_start")?,
        end: ts::parse(end.as_bytes()).ok_or("bad window_end")?,
    })
}

fn json_string<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("\"{key}\"");
    let at = text.find(&needle)? + needle.len();
    let rest = text[at..].trim_start().strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(&rest[..end])
}

fn read_products(data: &Path) -> Result<Products, String> {
    let path = data.join("products.csv");
    let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
    let mut map = HashMap::new();
    let mut categories: Vec<Vec<u8>> = Vec::new();
    let mut cat_index: HashMap<Vec<u8>, u16> = HashMap::new();
    let ensure = |name: &[u8], categories: &mut Vec<Vec<u8>>, cat_index: &mut HashMap<Vec<u8>, u16>| -> u16 {
        if let Some(&i) = cat_index.get(name) {
            return i;
        }
        let i = categories.len() as u16;
        categories.push(name.to_vec());
        cat_index.insert(name.to_vec(), i);
        i
    };
    csv::for_body_lines(&bytes, 0, bytes.len(), |line| {
        let mut fields = [b"" as &[u8]; 2];
        if !csv::split(line, &mut fields) || fields[0].is_empty() {
            return;
        }
        let cat = ensure(fields[1], &mut categories, &mut cat_index);
        map.insert(Id::of(fields[0]), cat);
    });
    let unknown = ensure(b"unknown", &mut categories, &mut cat_index);
    let mut order: Vec<u16> = (0..categories.len() as u16).collect();
    order.sort_by(|&a, &b| categories[a as usize].cmp(&categories[b as usize]));
    let mut rank = vec![0u16; categories.len()];
    for (r, &c) in order.iter().enumerate() {
        rank[c as usize] = r as u16;
    }
    Ok(Products { map, categories, rank, unknown })
}

fn read_campaigns(data: &Path) -> Result<Campaigns, String> {
    let path = data.join("campaigns.csv");
    let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
    // organic is always index 0 so credit_campaign can return 0 for organic.
    let mut ids = vec![b"organic".to_vec()];
    let mut channels = vec![b"organic".to_vec()];
    let mut map = HashMap::new();
    map.insert(Id::of(b"organic"), 0);
    csv::for_body_lines(&bytes, 0, bytes.len(), |line| {
        let mut fields = [b"" as &[u8]; 2];
        if !csv::split(line, &mut fields) || fields[0].is_empty() {
            return;
        }
        if fields[0] == b"organic" {
            return;
        }
        let idx = ids.len() as u16;
        map.insert(Id::of(fields[0]), idx);
        ids.push(fields[0].to_vec());
        channels.push(fields[1].to_vec());
    });
    Ok(Campaigns { map, ids, channels, organic: 0 })
}
