//! Shopping Sessions — native Rust pipeline.
//!
//! Ideas adapted from fast public track entries (parallel mmap scans, hash
//! shards for dedup, packed IDs, dense campaign grids, direct Parquet, a
//! glibc image): implemented here without copying their sources.

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
use std::time::Instant;

use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use mimalloc::MiMalloc;

use ids::Id;
use out::{CampCell, UserRow};
use ts::{day_of, DAY_MS, GAP_MS, WEEK_MS};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

const PERSON_BITS: u32 = 9;
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
    /// product Id -> category index, for ids that are not `p` + digits
    map: HashMap<Id, u16>,
    /// `p{n}` -> category. `u16::MAX` means that number is not in the catalog.
    by_num: Vec<u16>,
    categories: Vec<Vec<u8>>,
    /// byte-rank of each category for tie breaks
    rank: Vec<u16>,
    unknown: u16,
}

struct Campaigns {
    map: HashMap<Id, u16>,
    /// `c_{n}` -> campaign index. `u16::MAX` means that number is not in the catalog.
    by_num: Vec<u16>,
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
    let t0 = Instant::now();
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
    let t_setup = t0.elapsed();

    let (people, event_shards, click_shards) = std::thread::scope(|scope| -> Result<(People, Vec<Vec<RawEvent>>, Vec<Vec<Click>>), String> {
        let links = scope.spawn(|| collect_links(&link_files, ncpu));
        let events = scope.spawn(|| parse_events(&event_files, ncpu, &window, &products));
        let clicks = scope.spawn(|| parse_click_shards(&click_files, ncpu, &window, &campaigns));
        let pairs = links.join().unwrap()?;
        let people = People::build(pairs);
        let shards = events.join().unwrap()?;
        let clicks = clicks.join().unwrap()?;
        Ok((people, shards, clicks))
    })?;
    let t_scan = t0.elapsed();

    let (event_buckets, click_buckets) = std::thread::scope(|scope| {
        let events = scope.spawn(|| dedup_events(event_shards, ncpu, &people));
        let clicks = scope.spawn(|| dedup_clicks(click_shards, ncpu, &people));
        (events.join().unwrap(), clicks.join().unwrap())
    });
    let t_dedup = t0.elapsed();

    let base_day = day_of(window.start);
    let days = (day_of(window.end - 1) - base_day + 1) as usize;
    let n_campaigns = campaigns.ids.len();

    let mut click_parts = click_buckets.into_iter();
    let work: Vec<Mutex<Option<(Vec<Event>, Vec<Click>)>>> = event_buckets
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
                        run_person_shard(events, clicks, days, base_day, window.start, products, &mut cells, &mut rows);
                    }
                    if tid == 0 || !rows.is_empty() {
                        let path = user_dir.join(format!("part-{tid:03}.parquet"));
                        out::write_user_daily(&path, &rows, &products.categories).map_err(|e| e.to_string())?;
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
    eprintln!(
        "shopsess setup {:?} scan {:?} dedup {:?} total {:?}",
        t_setup,
        t_scan.saturating_sub(t_setup),
        t_dedup.saturating_sub(t_scan),
        t0.elapsed()
    );
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
    // One contiguous vector per thread. Sharding here scatters every push
    // across a thousand allocations and stops the scan from scaling.
    scan::for_chunks(
        files,
        ncpu,
        Vec::new,
        |out: &mut Vec<RawEvent>, data, start, end| {
            out.reserve((end - start) / 64);
            csv::for_body_lines(data, start, end, |line| {
                let mut fields = [b"" as &[u8]; 7];
                if !csv::split(line, &mut fields) || fields[0].is_empty() {
                    return;
                }
                let Some(raw) = decode_event(&fields, window, products) else {
                    return;
                };
                out.push(raw);
            });
        },
    )
    .map_err(|e| e.to_string())
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
                let cat = product_cat(products, product_raw);
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

fn product_cat(products: &Products, raw: &[u8]) -> u16 {
    if let Some(n) = prefixed_num(raw, b"p") {
        if let Some(&cat) = products.by_num.get(n as usize) {
            if cat != u16::MAX {
                return cat;
            }
        }
        return products.unknown;
    }
    products.map.get(&Id::of(raw)).copied().unwrap_or(products.unknown)
}

fn campaign_id(campaigns: &Campaigns, raw: &[u8]) -> Option<u16> {
    if let Some(n) = prefixed_num(raw, b"c_") {
        return campaigns.by_num.get(n as usize).copied().filter(|id| *id != u16::MAX);
    }
    let id = campaigns.map.get(&Id::of(raw)).copied()?;
    (id != campaigns.organic).then_some(id)
}

fn prefixed_num(raw: &[u8], prefix: &[u8]) -> Option<u32> {
    let rest = raw.strip_prefix(prefix)?;
    if rest.is_empty() || (rest.len() > 1 && rest[0] == b'0') {
        return None;
    }
    let mut n: u32 = 0;
    for &b in rest {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add(u32::from(b - b'0'))?;
    }
    Some(n)
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

fn hash_bits(_ncpu: usize) -> u32 {
    // 1024 shards keep each open table near L2. 64 was our prior best; 8 was
    // much slower. Public fast entries also land near this partition count.
    10
}

fn dedup_events(parts: Vec<Vec<RawEvent>>, ncpu: usize, people: &People) -> Vec<Vec<Event>> {
    let bits = hash_bits(ncpu);
    let columns = partition(parts, bits, |raw| raw.event.shard(bits));
    let next = AtomicUsize::new(0);
    let outs: Vec<Vec<Vec<Event>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..ncpu.max(1))
            .map(|_| {
                scope.spawn(|| {
                    let mut open: Vec<Vec<Event>> = (0..PERSON_SHARDS).map(|_| Vec::new()).collect();
                    let mut kept: HashMap<Id, RawEvent> = HashMap::new();
                    loop {
                        let s = next.fetch_add(1, AtomicOrdering::Relaxed);
                        if s >= columns.len() {
                            break;
                        }
                        let pieces = std::mem::take(&mut *columns[s].lock().unwrap());
                        let total: usize = pieces.iter().map(|p| p.len()).sum();
                        kept.clear();
                        kept.reserve(total);
                        for piece in pieces {
                            for raw in piece {
                                match kept.entry(raw.event) {
                                    Entry::Vacant(v) => {
                                        v.insert(raw);
                                    }
                                    Entry::Occupied(mut o) => {
                                        if raw.ingest < o.get().ingest {
                                            o.insert(raw);
                                        }
                                    }
                                }
                            }
                        }
                        for raw in kept.values() {
                            if !raw.ok {
                                continue;
                            }
                            let person = people.name_of(raw.user);
                            open[person.shard(PERSON_BITS)].push(Event {
                                person,
                                ts: raw.ts,
                                sale: raw.sale,
                            });
                        }
                    }
                    open
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    merge_buckets(outs)
}

fn parse_click_shards(
    files: &[PathBuf],
    ncpu: usize,
    window: &Window,
    campaigns: &Campaigns,
) -> Result<Vec<Vec<Click>>, String> {
    let click_lo = window.start - WEEK_MS;
    scan::for_chunks(
        files,
        ncpu,
        Vec::new,
        |out: &mut Vec<Click>, data, start, end| {
            out.reserve((end - start) / 48);
            csv::for_body_lines(data, start, end, |line| {
                let mut fields = [b"" as &[u8]; 4];
                if !csv::split(line, &mut fields) || fields[0].is_empty() || fields[1].is_empty() {
                    return;
                }
                let Some(campaign) = campaign_id(campaigns, fields[2]) else {
                    return;
                };
                let Some(ts_abs) = ts::parse(fields[3]) else {
                    return;
                };
                if ts_abs < click_lo || ts_abs >= window.end {
                    return;
                }
                let cid = Id::of(fields[0]);
                // `person` holds the raw user until dedup resolves the linked name.
                out.push(Click {
                    person: Id::of(fields[1]),
                    ts: (ts_abs - window.start) as i32,
                    campaign,
                    id: cid,
                });
            });
        },
    )
    .map_err(|e| e.to_string())
}

fn dedup_clicks(parts: Vec<Vec<Click>>, ncpu: usize, people: &People) -> Vec<Vec<Click>> {
    let bits = hash_bits(ncpu);
    let columns = partition(parts, bits, |click| click.id.shard(bits));
    let next = AtomicUsize::new(0);
    let outs: Vec<Vec<Vec<Click>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..ncpu.max(1))
            .map(|_| {
                scope.spawn(|| {
                    let mut open: Vec<Vec<Click>> = (0..PERSON_SHARDS).map(|_| Vec::new()).collect();
                    let mut kept: HashMap<Id, Click> = HashMap::new();
                    loop {
                        let s = next.fetch_add(1, AtomicOrdering::Relaxed);
                        if s >= columns.len() {
                            break;
                        }
                        let pieces = std::mem::take(&mut *columns[s].lock().unwrap());
                        let total: usize = pieces.iter().map(|p| p.len()).sum();
                        kept.clear();
                        kept.reserve(total);
                        for piece in pieces {
                            for click in piece {
                                // Copies of one click_id are identical, so any survivor is fine.
                                kept.entry(click.id).or_insert(click);
                            }
                        }
                        for mut click in kept.values().copied() {
                            click.person = people.name_of(click.person);
                            open[click.person.shard(PERSON_BITS)].push(click);
                        }
                    }
                    open
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    merge_buckets(outs)
}

fn partition<T: Send>(parts: Vec<Vec<T>>, bits: u32, shard: impl Fn(&T) -> usize + Sync) -> Vec<Mutex<Vec<Vec<T>>>> {
    let shards = 1usize << bits;
    let columns: Vec<Mutex<Vec<Vec<T>>>> = (0..shards).map(|_| Mutex::new(Vec::new())).collect();
    let split: Vec<Vec<Vec<T>>> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for part in parts {
            handles.push(scope.spawn(|| {
                let guess = part.len() / shards + 1;
                let mut buckets: Vec<Vec<T>> = (0..shards).map(|_| Vec::with_capacity(guess)).collect();
                for item in part {
                    buckets[shard(&item)].push(item);
                }
                buckets
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    for buckets in split {
        for (s, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                columns[s].lock().unwrap().push(bucket);
            }
        }
    }
    columns
}

fn merge_buckets<T>(outs: Vec<Vec<Vec<T>>>) -> Vec<Vec<T>> {
    let n = outs.first().map(|o| o.len()).unwrap_or(0);
    let mut buckets: Vec<Vec<T>> = (0..n).map(|_| Vec::new()).collect();
    for state in outs {
        for (b, part) in state.into_iter().enumerate() {
            if buckets[b].is_empty() {
                buckets[b] = part;
            } else {
                buckets[b].extend(part);
            }
        }
    }
    buckets
}

fn run_person_shard(
    mut events: Vec<Event>,
    mut clicks: Vec<Click>,
    days: usize,
    base_day: i32,
    origin: i64,
    products: &Products,
    cells: &mut [CampCell],
    rows: &mut Vec<UserRow>,
) {
    events.sort_unstable_by(|a, b| a.person.ord(b.person).then(a.ts.cmp(&b.ts)));
    clicks.sort_unstable_by(|a, b| a.person.ord(b.person).then(a.ts.cmp(&b.ts)).then_with(|| a.id.ord(b.id)));
    let mut sessions = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < events.len() {
        let person = events[i].person;
        let mut i2 = i + 1;
        while i2 < events.len() && events[i2].person == person {
            i2 += 1;
        }
        while j < clicks.len() && clicks[j].person.ord(person) == Ordering::Less {
            j += 1;
        }
        let mut j2 = j;
        while j2 < clicks.len() && clicks[j2].person == person {
            j2 += 1;
        }
        sessionize(person, &events[i..i2], &clicks[j..j2], days, base_day, origin, products, cells, rows, &mut sessions);
        i = i2;
        j = j2;
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
    sessions: &mut Vec<(i32, usize)>,
) {
    if events.is_empty() {
        return;
    }
    // The generator window is 14 days and the catalog has about 20 categories.
    // Stack slots avoid a heap allocation per shopper, which dominated the 20M run.
    const MAX_D: usize = 32;
    const MAX_C: usize = 32;
    let nc = products.categories.len();
    if days > MAX_D || nc > MAX_C {
        sessionize_heap(person, events, clicks, days, base_day, origin, products, cells, rows, sessions);
        return;
    }
    let mut day_sessions = [0i64; MAX_D];
    let mut day_events = [0i64; MAX_D];
    let mut day_revenue = [0i64; MAX_D];
    let mut cat_rev = [0i64; MAX_D * MAX_C];
    let mut used = [false; MAX_D];
    sessions.clear();

    let mut k = 0;
    while k < events.len() {
        let start = events[k].ts;
        let day = (day_of(origin + i64::from(start)) - base_day) as usize;
        used[day] = true;
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
                let cat = (sale >> PRICE_BITS) as usize;
                revenue += price;
                cat_rev[day * MAX_C + cat] += price;
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
        assist(clicks, sessions, days, cells);
    }

    for day in 0..days {
        if !used[day] {
            continue;
        }
        let mut top = None;
        let mut best = 0i64;
        for cat in 0..nc {
            let sum = cat_rev[day * MAX_C + cat];
            if sum == 0 {
                continue;
            }
            let better = match top {
                None => true,
                Some(cur) => sum > best || (sum == best && products.rank[cat] < products.rank[cur]),
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
            top: top.map(|c| c as u16),
        });
    }
}

fn sessionize_heap(
    person: Id,
    events: &[Event],
    clicks: &[Click],
    days: usize,
    base_day: i32,
    origin: i64,
    products: &Products,
    cells: &mut [CampCell],
    rows: &mut Vec<UserRow>,
    sessions: &mut Vec<(i32, usize)>,
) {
    let nc = products.categories.len();
    let mut day_sessions = vec![0i64; days];
    let mut day_events = vec![0i64; days];
    let mut day_revenue = vec![0i64; days];
    let mut cat_rev = vec![0i64; days * nc];
    let mut used = vec![false; days];
    sessions.clear();
    let mut k = 0;
    while k < events.len() {
        let start = events[k].ts;
        let day = (day_of(origin + i64::from(start)) - base_day) as usize;
        used[day] = true;
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
                let cat = (sale >> PRICE_BITS) as usize;
                revenue += price;
                cat_rev[day * nc + cat] += price;
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
        assist(clicks, sessions, days, cells);
    }
    for day in 0..days {
        if !used[day] {
            continue;
        }
        let mut top = None;
        let mut best = 0i64;
        for cat in 0..nc {
            let sum = cat_rev[day * nc + cat];
            if sum == 0 {
                continue;
            }
            let better = match top {
                None => true,
                Some(cur) => sum > best || (sum == best && products.rank[cat] < products.rank[cur]),
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
            top: top.map(|c| c as u16),
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
    if clicks.is_empty() || sessions.is_empty() {
        return;
    }
    if clicks.len() <= 64 && days <= 32 {
        let mut tmp = [(0u16, 0i32); 64];
        for (i, click) in clicks.iter().enumerate() {
            tmp[i] = (click.campaign, click.ts);
        }
        let n = clicks.len();
        tmp[..n].sort_unstable();
        let mut day_lo = [0usize; 33];
        assist_sorted(&tmp[..n], sessions, days, cells, &mut day_lo);
    } else {
        let mut by_camp: Vec<(u16, i32)> = clicks.iter().map(|c| (c.campaign, c.ts)).collect();
        by_camp.sort_unstable();
        let mut day_lo = vec![0usize; days + 1];
        assist_sorted(&by_camp, sessions, days, cells, &mut day_lo);
    }
}

fn assist_sorted(by_camp: &[(u16, i32)], sessions: &[(i32, usize)], days: usize, cells: &mut [CampCell], day_lo: &mut [usize]) {
    let week = WEEK_MS as i32;
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
    let mut by_num: Vec<u16> = Vec::new();
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
        if let Some(n) = prefixed_num(fields[0], b"p") {
            if by_num.len() <= n as usize {
                by_num.resize(n as usize + 1, u16::MAX);
            }
            by_num[n as usize] = cat;
        } else {
            map.insert(Id::of(fields[0]), cat);
        }
    });
    let unknown = ensure(b"unknown", &mut categories, &mut cat_index);
    let mut order: Vec<u16> = (0..categories.len() as u16).collect();
    order.sort_by(|&a, &b| categories[a as usize].cmp(&categories[b as usize]));
    let mut rank = vec![0u16; categories.len()];
    for (r, &c) in order.iter().enumerate() {
        rank[c as usize] = r as u16;
    }
    Ok(Products { map, by_num, categories, rank, unknown })
}

fn read_campaigns(data: &Path) -> Result<Campaigns, String> {
    let path = data.join("campaigns.csv");
    let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
    // organic is always index 0 so credit_campaign can return 0 for organic.
    let mut ids = vec![b"organic".to_vec()];
    let mut channels = vec![b"organic".to_vec()];
    let mut map = HashMap::new();
    let mut by_num: Vec<u16> = Vec::new();
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
        if let Some(n) = prefixed_num(fields[0], b"c_") {
            if by_num.len() <= n as usize {
                by_num.resize(n as usize + 1, u16::MAX);
            }
            by_num[n as usize] = idx;
        } else {
            map.insert(Id::of(fields[0]), idx);
        }
        ids.push(fields[0].to_vec());
        channels.push(fields[1].to_vec());
    });
    Ok(Campaigns { map, by_num, ids, channels, organic: 0 })
}
