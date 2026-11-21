//! Shopping Sessions pipeline.
//!
//! Reads the click-stream CSVs under DATA_DIR, applies every rule in rules.md
//! and writes the two daily Parquet tables under OUTPUT_DIR.
//!
//! Stages:
//!   1. small inputs (manifest, products, campaigns); links parsed in parallel
//!   2. events and clicks parsed in parallel while one thread joins the links
//!      into people; events are shuffled into buckets by event_id
//!   3. per event_id bucket: dedup, then shuffle survivors and clicks by person
//!   4. per person bucket: sessionize, attribute, aggregate, write Parquet

mod csv;
mod input;
mod key;
mod out;
mod people;
mod shuffle;
mod ts;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use hashbrown::HashMap;

use crate::csv::scan;
use crate::input::{load, Input, Piece};
use crate::key::{hash_key, key_cmp, pack, pack_fast, Key, KeyBuild};
use crate::people::People;
use crate::shuffle::Shuffle;
use crate::ts::{parse_price, parse_ts, parse_ts_fast, DAY};

const SESSION_GAP: i64 = 30 * 60 * 1000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;

/// Number of buckets used for both shuffles (by event_id, then by person).
const BUCKETS: usize = 512;
const CHUNK: usize = 2 << 20;

const T_VIEW: u8 = 1;
const T_CART: u8 = 2;
const T_PURCHASE: u8 = 3;
const PRICE_MASK: u32 = (1 << 24) - 1;

/// One parsed event line. `pt` holds the price in its low 24 bits and the
/// type above them; 0 means the copy is invalid. `ingest` is in seconds and
/// `ts` in milliseconds, both relative to `Small::base`.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    eid: Key,
    user: Key,
    ingest: i32,
    ts: u32,
    pt: u32,
    cat: u16,
}

/// A surviving event, keyed by its person.
#[derive(Clone, Copy)]
#[repr(C)]
struct Sv {
    person: Key,
    ts: u32,
    pt: u32,
    cat: u16,
}

/// A valid click, keyed by its person (by its user until links are applied).
#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    person: Key,
    cid: Key,
    ts: u32,
    camp: u32,
}

/// Per-campaign, per-day counters: credited sessions, purchasing sessions,
/// revenue, assisted sessions.
type CampAgg = Vec<[i64; 4]>;

#[inline(always)]
fn bucket_of(k: Key) -> usize {
    (hash_key(k) >> 40) as usize & (BUCKETS - 1)
}

/// The data rows of a file (everything after the header line).
fn body(m: &[u8]) -> &[u8] {
    match memchr::memchr(b'\n', m) {
        Some(i) => &m[i + 1..],
        None => &[],
    }
}

/// CPU time used by the calling thread, in seconds (for TIMING output).
fn thread_cpu() -> f64 {
    let mut t = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut t) };
    t.tv_sec as f64 + t.tv_nsec as f64 * 1e-9
}

/// Per-stage CPU seconds summed over threads, for TIMING output.
static CPU: [std::sync::atomic::AtomicU64; 4] = [const { std::sync::atomic::AtomicU64::new(0) }; 4];

fn add_cpu(stage: usize, secs: f64) {
    CPU[stage].fetch_add((secs * 1e6) as u64, Ordering::Relaxed);
}

fn read_small(p: &Path) -> Vec<u8> {
    std::fs::read(p).unwrap_or_default()
}

/// Reads a two-column clean CSV into (key, value) pairs.
fn read_pairs(p: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = read_small(p);
    let mut v = Vec::new();
    scan::<2, _>(body(&data), |f| {
        if !f[0].is_empty() {
            v.push((f[0].to_vec(), f[1].to_vec()));
        }
    });
    v
}

fn manifest_time(text: &str, name: &str) -> i64 {
    let at = text.find(&format!("\"{name}\"")).expect("manifest field");
    let rest = &text[at + name.len() + 2..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    parse_ts(rest[..q2].as_bytes()).expect("manifest time")
}

fn run_parallel<F: Fn(usize) + Sync>(threads: usize, f: F) {
    std::thread::scope(|s| {
        for t in 1..threads {
            let f = &f;
            s.spawn(move || f(t));
        }
        f(0);
    });
}

/// A union-find over link IDs, giving each linked ID its person's name.
fn build_people(links: Vec<Vec<(Key, Key)>>) -> People {
    let total: usize = links.iter().map(|v| v.len()).sum();
    let mut index: HashMap<Key, u32, KeyBuild> = HashMap::with_capacity_and_hasher(total, KeyBuild);
    let mut ids: Vec<Key> = Vec::with_capacity(total);
    let mut parent: Vec<u32> = Vec::with_capacity(total);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    let mut intern = |k: Key, ids: &mut Vec<Key>, parent: &mut Vec<u32>| -> u32 {
        *index.entry(k).or_insert_with(|| {
            ids.push(k);
            parent.push(parent.len() as u32);
            (ids.len() - 1) as u32
        })
    };
    for part in &links {
        for &(a, b) in part {
            if a == b {
                intern(a, &mut ids, &mut parent);
                continue;
            }
            let ia = intern(a, &mut ids, &mut parent);
            let ib = intern(b, &mut ids, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    let n = ids.len();
    let mut name: Vec<Key> = vec![0; n];
    let mut has: Vec<bool> = vec![false; n];
    let mut root = vec![0u32; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        root[i] = r;
        let r = r as usize;
        if !has[r] || key_cmp(ids[i], name[r]).is_lt() {
            name[r] = ids[i];
            has[r] = true;
        }
    }
    let mut pairs = Vec::with_capacity(n);
    for i in 0..n {
        let nm = name[root[i] as usize];
        if nm != ids[i] {
            pairs.push((ids[i], nm));
        }
    }
    People::new(&pairs)
}

struct Small {
    /// Base of the relative times: window_start - 7 days.
    base: i64,
    ws: i64,
    we: i64,
    product_cat: HashMap<Key, u16, KeyBuild>,
    unknown_cat: u16,
    cat_names: Vec<Vec<u8>>,
    camp_index: HashMap<Key, u32, KeyBuild>,
    camp_ids: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
}

fn load_small(data: &Path) -> Small {
    let manifest = String::from_utf8_lossy(&read_small(&data.join("manifest.json"))).into_owned();
    let ws = manifest_time(&manifest, "window_start");
    let we = manifest_time(&manifest, "window_end");

    let products = read_pairs(&data.join("products.csv"));
    let mut names: Vec<Vec<u8>> = products.iter().map(|(_, c)| c.clone()).collect();
    names.push(b"unknown".to_vec());
    names.sort();
    names.dedup();
    let rank: std::collections::HashMap<Vec<u8>, u16> =
        names.iter().enumerate().map(|(i, n)| (n.clone(), i as u16)).collect();
    let mut product_cat = HashMap::with_capacity_and_hasher(products.len(), KeyBuild);
    for (p, c) in &products {
        product_cat.insert(pack(p), rank[c]);
    }
    let unknown_cat = rank[&b"unknown".to_vec()];

    let campaigns = read_pairs(&data.join("campaigns.csv"));
    let mut camp_index = HashMap::with_capacity_and_hasher(campaigns.len(), KeyBuild);
    let mut camp_ids = Vec::new();
    let mut camp_channels = Vec::new();
    for (c, ch) in campaigns {
        let k = pack(&c);
        if camp_index.contains_key(&k) {
            continue;
        }
        camp_index.insert(k, camp_ids.len() as u32);
        camp_ids.push(c);
        camp_channels.push(ch);
    }
    Small { base: ws - WEEK, ws, we, product_cat, unknown_cat, cat_names: names, camp_index, camp_ids, camp_channels }
}


/// `view`, `cart` or `purchase` exactly, as T_*; 0 for anything else.
#[inline(always)]
fn event_type(t: &[u8]) -> u8 {
    match t.len() {
        4 => match u32::from_le_bytes(t.try_into().unwrap()) {
            x if x == u32::from_le_bytes(*b"view") => T_VIEW,
            x if x == u32::from_le_bytes(*b"cart") => T_CART,
            _ => 0,
        },
        8 if u64::from_le_bytes(t.try_into().unwrap()) == u64::from_le_bytes(*b"purchase") => T_PURCHASE,
        _ => 0,
    }
}

#[inline(always)]
fn parse_event(f: &[&[u8]; 7], sm: &Small, lim: usize) -> Option<Ev> {
    if f[0].is_empty() {
        return None;
    }
    let ok16 = |x: &[u8]| x.as_ptr() as usize + 16 <= lim;
    let eid = pack_fast(f[0], ok16(f[0]));
    let ingest = parse_ts(f[3]).unwrap_or(i64::MAX / 2);
    let ingest_rel = (ingest - sm.base).div_euclid(1000).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
    let mut ev = Ev { eid, user: 0, ingest: ingest_rel, ts: 0, pt: 0, cat: 0 };
    let user = f[1];
    if user.is_empty() || f[5].is_empty() {
        return Some(ev);
    }
    let typ = event_type(f[4]);
    if typ == 0 {
        return Some(ev);
    }
    let ts = match parse_ts_fast(f[2], ok16(f[2])) {
        Some(t) if t >= sm.ws && t < sm.we => t,
        _ => return Some(ev),
    };
    if ingest - ts > LATE {
        return Some(ev);
    }
    let mut price = 0u32;
    if typ == T_PURCHASE {
        match parse_price(f[6]) {
            Some(p) => price = p,
            None => return Some(ev),
        }
        ev.cat = *sm.product_cat.get(&pack(f[5])).unwrap_or(&sm.unknown_cat);
    }
    ev.user = pack_fast(user, ok16(user));
    ev.ts = (ts - sm.base) as u32;
    ev.pt = price | (typ as u32) << 24;
    Some(ev)
}

#[inline(always)]
fn parse_click(f: &[&[u8]; 4], sm: &Small, lim: usize) -> Option<Ck> {
    let ok16 = |x: &[u8]| x.as_ptr() as usize + 16 <= lim;
    if f[0].is_empty() || f[1].is_empty() {
        return None;
    }
    let ts = parse_ts_fast(f[3], ok16(f[3]))?;
    if ts < sm.ws - WEEK || ts >= sm.we {
        return None;
    }
    let camp = *sm.camp_index.get(&pack_fast(f[2], ok16(f[2])))?;
    Some(Ck { person: pack_fast(f[1], ok16(f[1])), cid: pack_fast(f[0], ok16(f[0])), ts: (ts - sm.base) as u32, camp })
}

fn main() {
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let output = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| std::thread::available_parallelism().ok().map(|n| n.get()))
        .unwrap_or(8)
        .max(1);
    let t0 = std::time::Instant::now();
    let timing = std::env::var("TIMING").is_ok();
    let mark = |what: &str| {
        if timing {
            eprintln!("{:>8.1} ms  {}", t0.elapsed().as_secs_f64() * 1000.0, what);
        }
    };

    ts::init();
    let sm = load_small(&data);
    let links_in = Input::open(&data.join("links"));
    let events_in = Input::open(&data.join("events"));
    let clicks_in = Input::open(&data.join("clicks"));
    mark("small");

    // ---- links: parse in parallel ----
    let mut link_chunks: Vec<Piece> = Vec::new();
    links_in.pieces(CHUNK as u64, &mut link_chunks);
    let next = AtomicUsize::new(0);
    let link_parts: Vec<Mutex<Vec<(Key, Key)>>> = (0..threads).map(|_| Mutex::new(Vec::new())).collect();
    run_parallel(threads, |t| {
        let mut local = Vec::new();
        let mut buf = Vec::new();
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= link_chunks.len() {
                break;
            }
            let (a, b) = load(&links_in, link_chunks[i], &mut buf);
            scan::<2, _>(&buf[a..b], |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    local.push((pack(f[0]), pack(f[1])));
                }
            });
        }
        *link_parts[t].lock().unwrap() = local;
    });
    let link_parts: Vec<Vec<(Key, Key)>> = link_parts.into_iter().map(|m| m.into_inner().unwrap()).collect();
    mark("links parsed");

    // ---- events and clicks; thread 0 first joins the links into people ----
    let mut work: Vec<(bool, Piece)> = Vec::new();
    let event_bytes = events_in.bytes() as usize;
    let click_bytes = clicks_in.bytes() as usize;
    {
        let mut tmp = Vec::new();
        events_in.pieces(CHUNK as u64, &mut tmp);
        work.extend(tmp.drain(..).map(|p| (true, p)));
        clicks_in.pieces(CHUNK as u64, &mut tmp);
        work.extend(tmp.drain(..).map(|p| (false, p)));
    }
    let ev_shuffle: Shuffle<Ev> = Shuffle::new(event_bytes / 64 + 1024, BUCKETS);
    let people_cell: OnceLock<People> = OnceLock::new();
    let link_parts = Mutex::new(Some(link_parts));
    let next = AtomicUsize::new(0);
    // Clicks are parsed last; by then the people are known, so each click goes
    // straight into its person's bucket.
    let ck_shuffle: Shuffle<Ck> = Shuffle::new(click_bytes / 40 + 1024, BUCKETS);
    run_parallel(threads, |t| {
        if t == 0 {
            let parts = link_parts.lock().unwrap().take().unwrap();
            let _ = people_cell.set(build_people(parts));
            mark("people (thread 0)");
        }
        let c0 = thread_cpu();
        let mut w = ev_shuffle.writer();
        let mut cw = ck_shuffle.writer();
        let mut pending: Vec<Ck> = Vec::with_capacity(64);
        let mut buf: Vec<u8> = Vec::with_capacity(CHUNK + (128 << 10));
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= work.len() {
                break;
            }
            let (is_event, piece) = work[i];
            let inp = if is_event { &events_in } else { &clicks_in };
            let (chunk, lim): (&[u8], usize) = if inp.maps.is_empty() {
                // Fallback: no mapping.
                let (a, b) = load(inp, piece, &mut buf);
                // The buffer keeps zero padding after the data, so 16-byte
                // loads past any field stay inside it.
                (&buf[a..b], buf.as_ptr() as usize + buf.len())
            } else {
                let (m, a, b) = inp.view(piece);
                (&m[a..b], m.as_ptr() as usize + m.len())
            };
            if is_event {
                scan::<7, _>(chunk, |f| {
                    if let Some(ev) = parse_event(f, &sm, lim) {
                        w.push(bucket_of(ev.eid), ev);
                    }
                });
            } else {
                let people = people_cell.wait();
                // Resolve people in small batches so the lookups can be
                // prefetched.
                let mut flush = |pending: &mut Vec<Ck>| {
                    for c in pending.iter() {
                        people.prefetch(c.person);
                    }
                    for c in pending.drain(..) {
                        let mut c = c;
                        c.person = people.get(c.person);
                        cw.push(bucket_of(c.person), c);
                    }
                };
                scan::<4, _>(chunk, |f| {
                    if let Some(c) = parse_click(f, &sm, lim) {
                        pending.push(c);
                        if pending.len() == 32 {
                            flush(&mut pending);
                        }
                    }
                });
                flush(&mut pending);
            }
        }
        w.finish();
        cw.finish();
        add_cpu(0, thread_cpu() - c0);
    });
    let people = people_cell.get().unwrap();
    // How far ahead person lookups are prefetched.
    const AHEAD: usize = 24;
    mark("parse");

    // ---- dedup by event_id; bucket survivors and clicks by person ----
    let sv_shuffle: Shuffle<Sv> = Shuffle::new(event_bytes / 64 + 1024, BUCKETS);
    let next = AtomicUsize::new(0);
    run_parallel(threads, |_t| {
        let c0 = thread_cpu();
        let mut winners: Vec<u32> = Vec::new();
        let mut win: Vec<u64> = Vec::new();
        let mut w = sv_shuffle.writer();
        let mut seen: HashMap<Key, (i32, u32), KeyBuild> = HashMap::with_hasher(KeyBuild);
        loop {
            let b = next.fetch_add(1, Ordering::Relaxed);
            if b >= BUCKETS {
                break;
            }
            let (main, extra) = ev_shuffle.bucket(b);
            let all_len = main.len() + extra.len();
            let get = |i: usize| -> &Ev { if i < main.len() { &main[i] } else { &extra[i - main.len()] } };
            seen.clear();
            seen.reserve(all_len);
            // Keep the copy with the smallest ingest_ts; copies with equal
            // ingest_ts are identical, so the first one seen is kept.
            for i in 0..all_len {
                let ev = get(i);
                match seen.entry(ev.eid) {
                    hashbrown::hash_map::Entry::Vacant(v) => {
                        v.insert((ev.ingest, i as u32));
                    }
                    hashbrown::hash_map::Entry::Occupied(mut o) => {
                        if ev.ingest < o.get().0 {
                            o.insert((ev.ingest, i as u32));
                        }
                    }
                }
            }
            win.clear();
            win.resize(all_len.div_ceil(64), 0);
            for &(_, i) in seen.values() {
                win[i as usize / 64] |= 1 << (i % 64);
            }
            winners.clear();
            for (wi, &word) in win.iter().enumerate() {
                let mut bits = word;
                while bits != 0 {
                    let i = wi * 64 + bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    if get(i).pt != 0 {
                        winners.push(i as u32);
                    }
                }
            }
            for (k, &i) in winners.iter().enumerate() {
                if let Some(&a) = winners.get(k + AHEAD) {
                    people.prefetch(get(a as usize).user);
                }
                let ev = get(i as usize);
                let person = people.get(ev.user);
                w.push(bucket_of(person), Sv { person, ts: ev.ts, pt: ev.pt, cat: ev.cat });
            }
        }
        w.finish();
        add_cpu(1, thread_cpu() - c0);
    });
    drop(ev_shuffle);
    mark("dedup");

    // ---- sessions, attribution, aggregation ----
    let any_long = key::any_long();
    let day0 = sm.ws.div_euclid(DAY);
    let ndays = ((sm.we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ncamp = sm.camp_ids.len();
    let organic = ncamp as u32;
    let ud_dir = output.join("user_daily");
    let cd_dir = output.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir).expect("create user_daily");
    std::fs::create_dir_all(&cd_dir).expect("create campaign_daily");
    let next = AtomicUsize::new(0);
    // Largest buckets first, so a bucket holding a very busy person does not
    // start last and leave the other threads idle.
    let mut order: Vec<u32> = (0..BUCKETS as u32).collect();
    {
        let size = |b: usize| sv_shuffle.len(b) + 2 * ck_shuffle.len(b);
        let sizes: Vec<usize> = (0..BUCKETS).map(size).collect();
        order.sort_unstable_by_key(|&b| std::cmp::Reverse(sizes[b as usize]));
    }
    let camp_aggs: Vec<Mutex<CampAgg>> = (0..threads).map(|_| Mutex::new(Vec::new())).collect();
    run_parallel(threads, |t| {
        let c0 = thread_cpu();
        let mut agg: CampAgg = vec![[0; 4]; (ncamp + 1) * ndays];
        // Per campaign: clicks inside the current 7-day window, and the
        // session from which it has been covering sessions.
        let mut cnt: Vec<u32> = vec![0; ncamp + 1];
        let mut since: Vec<u32> = vec![0; ncamp + 1];
        // Per person: (day index, first session of that day).
        let mut day_runs: Vec<(u32, u32)> = Vec::new();
        let mut rows = out::UserRows::new(&sm.cat_names);
        let mut evs: Vec<Sv> = Vec::new();
        let mut cks: Vec<Ck> = Vec::new();
        let mut day_cats: Vec<(u16, i64)> = Vec::new();
        let mut ev_starts: Vec<u32> = Vec::new();
        let mut ck_starts: Vec<u32> = Vec::new();
        loop {
            let n = next.fetch_add(1, Ordering::Relaxed);
            if n >= BUCKETS {
                break;
            }
            let b = order[n] as usize;
            // Radix-partition this bucket's events and clicks by a few hash
            // bits of the person, then sort each small part by (person, ts).
            // Within every part, persons come out in key order for both.
            let (m, x) = sv_shuffle.bucket(b);
            radix_parts(m, &x, |e| e.person, &mut evs, &mut ev_starts);
            let (m, x) = ck_shuffle.bucket(b);
            radix_parts(m, &x, |c| c.person, &mut cks, &mut ck_starts);
            for p in 0..SUB {
                let g = &mut evs[ev_starts[p] as usize..ev_starts[p + 1] as usize];
                if g.len() <= 24 {
                    for q in 1..g.len() {
                        let mut r = q;
                        while r > 0 && (g[r - 1].person, g[r - 1].ts) > (g[r].person, g[r].ts) {
                            g.swap(r - 1, r);
                            r -= 1;
                        }
                    }
                } else {
                    g.sort_unstable_by_key(|e| ((e.person as u128) << 32) | e.ts as u128);
                }
                let g = &mut cks[ck_starts[p] as usize..ck_starts[p + 1] as usize];
                if any_long {
                    g.sort_unstable_by(|a, b| {
                        a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then_with(|| key_cmp(a.cid, b.cid))
                    });
                } else if g.len() <= 24 {
                    for q in 1..g.len() {
                        let mut r = q;
                        while r > 0 && (g[r - 1].person, g[r - 1].ts, g[r - 1].cid) > (g[r].person, g[r].ts, g[r].cid) {
                            g.swap(r - 1, r);
                            r -= 1;
                        }
                    }
                } else {
                    g.sort_unstable_by_key(|c| (c.person, c.ts, c.cid));
                }
            }
            let mut ci = 0usize;
            let mut i = 0usize;
            while i < evs.len() {
                let person = evs[i].person;
                let mut j = i + 1;
                while j < evs.len() && evs[j].person == person {
                    j += 1;
                }
                let part = sub_of(person);
                while ci < cks.len() && (sub_of(cks[ci].person), cks[ci].person) < (part, person) {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < cks.len() && cks[cj].person == person {
                    cj += 1;
                }
                let pc = &cks[ci..cj];
                ci = cj;

                // Walk sessions; days only grow, so one row is open at a time.
                let mut cur_day: i64 = i64::MIN;
                let (mut d_sess, mut d_evs, mut d_rev) = (0i64, 0i64, 0i64);
                day_cats.clear();
                day_runs.clear();
                // Clicks pc[lo..hi] are inside the window of the current session.
                let (mut lo, mut hi) = (0usize, 0usize);
                let mut ns: u32 = 0;
                let mut k = i;
                while k < j {
                    let start = evs[k].ts;
                    let mut e = k + 1;
                    while e < j && (evs[e].ts - evs[e - 1].ts) as i64 <= SESSION_GAP {
                        e += 1;
                    }
                    let day = (sm.base + start as i64).div_euclid(DAY);
                    if day != cur_day {
                        if cur_day != i64::MIN {
                            rows.push(person, cur_day, d_sess, d_evs, d_rev, top_cat(&day_cats));
                        }
                        cur_day = day;
                        d_sess = 0;
                        d_evs = 0;
                        d_rev = 0;
                        day_cats.clear();
                    }
                    let mut rev = 0i64;
                    let mut purchased = false;
                    for ev in &evs[k..e] {
                        if ev.pt >> 24 == T_PURCHASE as u32 {
                            let price = (ev.pt & PRICE_MASK) as i64;
                            purchased = true;
                            rev += price;
                            match day_cats.iter_mut().find(|c| c.0 == ev.cat) {
                                Some(c) => c.1 += price,
                                None => day_cats.push((ev.cat, price)),
                            }
                        }
                    }
                    d_sess += 1;
                    d_evs += (e - k) as i64;
                    d_rev += rev;

                    // Attribution over this person's clicks in [start - 7d, start].
                    // Both window ends only move forward. A campaign covers a
                    // run of consecutive sessions while it has clicks inside;
                    // each run is credited per day when it ends.
                    let di = (day - day0) as usize;
                    while hi < pc.len() && pc[hi].ts <= start {
                        let c = pc[hi].camp as usize;
                        if cnt[c] == 0 {
                            since[c] = ns;
                        }
                        cnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && (pc[lo].ts as i64) < start as i64 - WEEK {
                        let c = pc[lo].camp as usize;
                        cnt[c] -= 1;
                        if cnt[c] == 0 {
                            add_runs(&mut agg, &day_runs, c, ndays, since[c], ns);
                        }
                        lo += 1;
                    }
                    if day_runs.last().map_or(true, |r| r.0 != di as u32) {
                        day_runs.push((di as u32, ns));
                    }
                    ns += 1;
                    let credit = if hi > lo {
                        let last = pc[hi - 1].ts;
                        let mut f = hi - 1;
                        while f > lo && pc[f - 1].ts == last {
                            f -= 1;
                        }
                        pc[f].camp
                    } else {
                        organic
                    };
                    let a = &mut agg[credit as usize * ndays + di];
                    a[0] += 1;
                    a[1] += purchased as i64;
                    a[2] += rev;
                    k = e;
                }
                // Close the runs still open after the last session.
                for q in lo..hi {
                    let c = pc[q].camp as usize;
                    if cnt[c] != 0 {
                        cnt[c] = 0;
                        add_runs(&mut agg, &day_runs, c, ndays, since[c], ns);
                    }
                }
                if cur_day != i64::MIN {
                    rows.push(person, cur_day, d_sess, d_evs, d_rev, top_cat(&day_cats));
                }
                i = j;
            }
        }
        add_cpu(2, thread_cpu() - c0);
        let c1 = thread_cpu();
        if !rows.is_empty() || t == 0 {
            rows.write(&ud_dir.join(format!("part-{t:03}.parquet")));
        }
        add_cpu(3, thread_cpu() - c1);
        *camp_aggs[t].lock().unwrap() = agg;
    });
    mark("sessions+write");

    let mut total: CampAgg = vec![[0; 4]; (ncamp + 1) * ndays];
    for a in camp_aggs {
        for (x, y) in total.iter_mut().zip(a.into_inner().unwrap().iter()) {
            for q in 0..4 {
                x[q] += y[q];
            }
        }
    }
    let mut crow = out::CampRows::new();
    for c in 0..=ncamp {
        for d in 0..ndays {
            let a = total[c * ndays + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            let (id, ch): (&[u8], &[u8]) =
                if c == ncamp { (b"organic", b"organic") } else { (&sm.camp_ids[c], &sm.camp_channels[c]) };
            crow.push(id, ch, (day0 + d as i64) as i32, a);
        }
    }
    crow.write(&cd_dir.join("part-000.parquet"));
    mark("done");
    if timing {
        let c: Vec<f64> = CPU.iter().map(|x| x.load(Ordering::Relaxed) as f64 / 1e6).collect();
        eprintln!("cpu s: parse {:.3} dedup {:.3} sessions {:.3} write {:.3}", c[0], c[1], c[2], c[3]);
    }
    std::process::exit(0);
}

/// Parts per person bucket in the sessions stage.
const SUB: usize = 2048;

/// The part of a person within its bucket (hash bits not used by bucket_of).
#[inline(always)]
fn sub_of(k: Key) -> usize {
    hash_key(k) as usize & (SUB - 1)
}

/// Counting sort of `main` + `extra` into `out` by `sub_of(key(x))`;
/// `starts[p]..starts[p + 1]` is part p.
fn radix_parts<T: Copy, F: Fn(&T) -> Key>(main: &[T], extra: &[T], key: F, out: &mut Vec<T>, starts: &mut Vec<u32>) {
    let n = main.len() + extra.len();
    starts.clear();
    starts.resize(SUB + 1, 0);
    for x in main.iter().chain(extra.iter()) {
        starts[sub_of(key(x)) + 1] += 1;
    }
    for p in 0..SUB {
        starts[p + 1] += starts[p];
    }
    out.clear();
    out.reserve(n);
    let mut fill: Vec<u32> = starts[..SUB].to_vec();
    unsafe {
        let o = out.as_mut_ptr();
        for x in main.iter().chain(extra.iter()) {
            let p = sub_of(key(x));
            o.add(fill[p] as usize).write(*x);
            fill[p] += 1;
        }
        out.set_len(n);
    }
}

/// The category with the highest summed price; ties go to the smallest name,
/// which is the smallest rank.
/// Adds one assisted session to campaign `c` for every session in
/// `a..b`, on each session's day.
fn add_runs(agg: &mut CampAgg, day_runs: &[(u32, u32)], c: usize, ndays: usize, a: u32, b: u32) {
    if a >= b {
        return;
    }
    let mut r = day_runs.partition_point(|x| x.1 <= a) - 1;
    let mut from = a;
    while from < b {
        let end = day_runs.get(r + 1).map_or(b, |x| x.1.min(b));
        agg[c * ndays + day_runs[r].0 as usize][3] += (end - from) as i64;
        from = end;
        r += 1;
    }
}

fn top_cat(cats: &[(u16, i64)]) -> Option<u16> {
    let mut best: Option<(u16, i64)> = None;
    for &(c, s) in cats {
        best = match best {
            None => Some((c, s)),
            Some((bc, bs)) => {
                if s > bs || (s == bs && c < bc) {
                    Some((c, s))
                } else {
                    Some((bc, bs))
                }
            }
        };
    }
    best.map(|b| b.0)
}
