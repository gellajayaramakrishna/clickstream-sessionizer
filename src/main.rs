//! Shopping Sessions pipeline.
//!
//! Reads the click-stream CSVs under DATA_DIR, applies every rule in rules.md
//! and writes the two daily Parquet tables under OUTPUT_DIR.
//!
//! Stages:
//!   1. small inputs and links; all threads join linked IDs into people
//!   2. one pass over events and clicks: each valid event writes a 16-byte
//!      person record into its person's bucket, and every event line writes
//!      a 16-byte event-ID record (with the person record's slot) into an
//!      event-ID bucket; clicks go straight to their person's bucket
//!   3. per event-ID bucket: duplicate copies that lose are marked dead in a
//!      bitmap (nothing is moved)
//!   4. per person bucket, largest first: skip dead records, group, sort,
//!      sessionize, attribute, aggregate, write Parquet

mod arena;
mod csv;
mod input;
mod key;
mod out;
mod people;
mod ts;
mod uf;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use hashbrown::HashMap;

use crate::arena::{Arena, BLOCK};
use crate::csv::scan;
use crate::input::{load, Input, Piece};
use crate::key::{hash_key, key_cmp, pack, pack_fast, Key, KeyBuild};
use crate::people::People;
use crate::ts::{parse_price, parse_ts, parse_ts_fast, DAY};

const SESSION_GAP: i64 = 30 * 60 * 1000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;

/// Number of buckets, both by event ID and by person.
const BUCKETS: usize = 512;
const CHUNK: usize = 2 << 20;

const T_VIEW: u8 = 1;
const T_CART: u8 = 2;
const T_PURCHASE: u8 = 3;
const PRICE_MASK: u32 = (1 << 24) - 1;
/// Slot of an event line that is not a valid event.
const NO_SLOT: u32 = u32::MAX;
/// Category code meaning "look the category up by slot" (more than 254
/// categories).
const WIDE_CAT: u32 = 255;

/// A valid event in its person's bucket. `pt` holds the price in its low 24
/// bits and, for a purchase, category + 1 above them (0 for view and cart).
#[derive(Clone, Copy)]
#[repr(C)]
struct Pr {
    person: Key,
    ts: u32,
    pt: u32,
}

/// One event line in its event-ID bucket: ingest time in seconds (relative)
/// and the slot of its person record, or NO_SLOT if the line is invalid.
#[derive(Clone, Copy)]
#[repr(C)]
struct Er {
    eid: Key,
    ingest: i32,
    slot: u32,
}

/// A valid click in its person's bucket.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    person: Key,
    cid: Key,
    ts: u32,
    camp: u32,
}

/// An event as the sessions stage works with it.
#[derive(Clone, Copy)]
struct Sv {
    person: Key,
    ts: u32,
    price: u32,
    /// Category for a purchase, u16::MAX otherwise.
    cat: u16,
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

/// What one event line becomes.
enum Line {
    /// A valid event, not yet placed: (eid, ingest, user, ts, pt, cat).
    Valid(Key, i32, Key, u32, u32, u16),
    /// A line with an event ID that is not a valid event.
    Invalid(Key, i32),
}

#[inline(always)]
fn parse_event(f: &[&[u8]; 7], sm: &Small, lim: usize) -> Option<Line> {
    if f[0].is_empty() {
        return None;
    }
    let ok16 = |x: &[u8]| x.as_ptr() as usize + 16 <= lim;
    let eid = pack_fast(f[0], ok16(f[0]));
    let ingest = parse_ts(f[3]).unwrap_or(i64::MAX / 2);
    let ingest_rel = (ingest - sm.base).div_euclid(1000).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
    let bad = Some(Line::Invalid(eid, ingest_rel));
    let user = f[1];
    if user.is_empty() || f[5].is_empty() {
        return bad;
    }
    let typ = event_type(f[4]);
    if typ == 0 {
        return bad;
    }
    let ts = match parse_ts_fast(f[2], ok16(f[2])) {
        Some(t) if t >= sm.ws && t < sm.we => t,
        _ => return bad,
    };
    if ingest - ts > LATE {
        return bad;
    }
    let mut pt = 0u32;
    let mut cat = u16::MAX;
    if typ == T_PURCHASE {
        let price = match parse_price(f[6]) {
            Some(p) => p,
            None => return bad,
        };
        cat = *sm.product_cat.get(&pack_fast(f[5], ok16(f[5]))).unwrap_or(&sm.unknown_cat);
        let code = if (cat as u32) < WIDE_CAT - 1 { cat as u32 + 1 } else { WIDE_CAT };
        pt = price | code << 24;
    }
    let _ = (T_VIEW, T_CART);
    Some(Line::Valid(eid, ingest_rel, pack_fast(user, ok16(user)), (ts - sm.base) as u32, pt, cat))
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

    // ---- small inputs (one thread) while the others parse links ----
    let links_in = Input::open(&data.join("links"));
    let mut link_pieces: Vec<Piece> = Vec::new();
    links_in.pieces(CHUNK as u64 / 4, &mut link_pieces);
    let small_cell: OnceLock<Small> = OnceLock::new();
    let next = AtomicUsize::new(0);
    let link_parts: Vec<Mutex<Vec<(Key, Key)>>> = (0..threads).map(|_| Mutex::new(Vec::new())).collect();
    run_parallel(threads, |t| {
        if t == 0 {
            let _ = small_cell.set(load_small(&data));
        }
        let mut local = Vec::new();
        let mut buf = Vec::new();
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= link_pieces.len() {
                break;
            }
            let (a, b) = load(&links_in, link_pieces[i], &mut buf);
            scan::<2, _>(&buf[a..b], |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    local.push((pack(f[0]), pack(f[1])));
                }
            });
        }
        *link_parts[t].lock().unwrap() = local;
    });
    let sm = small_cell.into_inner().unwrap();
    let link_parts: Vec<Vec<(Key, Key)>> = link_parts.into_iter().map(|m| m.into_inner().unwrap()).collect();
    mark("small + links parsed");
    let people: People = uf::build(&link_parts, threads);
    drop(link_parts);
    mark("people");

    // ---- one pass over events and clicks ----
    let events_in = Input::open(&data.join("events"));
    let clicks_in = Input::open(&data.join("clicks"));
    let mut work: Vec<(bool, Piece)> = Vec::new();
    {
        let mut tmp = Vec::new();
        events_in.pieces(CHUNK as u64, &mut tmp);
        work.extend(tmp.drain(..).map(|p| (true, p)));
        clicks_in.pieces(CHUNK as u64, &mut tmp);
        work.extend(tmp.drain(..).map(|p| (false, p)));
    }
    // Every event line is at least 28 bytes and every click line 20, so
    // these bound the record counts.
    let event_bytes = events_in.bytes() as usize;
    let click_bytes = clicks_in.bytes() as usize;
    let pr_arena: Arena<Pr> = Arena::new(event_bytes / 28 + 1, BUCKETS, threads);
    let er_arena: Arena<Er> = Arena::new(event_bytes / 28 + 1, BUCKETS, threads);
    let ck_arena: Arena<Ck> = Arena::new(click_bytes / 20 + 1, BUCKETS, threads);
    let wide: Mutex<HashMap<u32, u16>> = Mutex::new(HashMap::new());
    let next = AtomicUsize::new(0);
    let filled: Vec<Mutex<Option<(arena::Filled, arena::Filled, arena::Filled)>>> =
        (0..threads).map(|_| Mutex::new(None)).collect();
    const BATCH: usize = 32;
    run_parallel(threads, |t| {
        let c0 = thread_cpu();
        let mut pw = pr_arena.writer();
        let mut ew = er_arena.writer();
        let mut cw = ck_arena.writer();
        let mut buf: Vec<u8> = Vec::new();
        // Valid events and clicks wait in small batches so their person
        // lookups can be prefetched together.
        let mut evb: Vec<(Key, i32, Key, u32, u32, u16)> = Vec::with_capacity(BATCH);
        let mut ckb: Vec<Ck> = Vec::with_capacity(BATCH);
        let mut my_wide: Vec<(u32, u16)> = Vec::new();
        let flush_ev = |evb: &mut Vec<(Key, i32, Key, u32, u32, u16)>,
                            pw: &mut arena::Writer<Pr>,
                            ew: &mut arena::Writer<Er>,
                            my_wide: &mut Vec<(u32, u16)>| {
            for e in evb.iter() {
                people.prefetch(e.2);
            }
            for &(eid, ingest, user, ts, pt, cat) in evb.iter() {
                let person = people.get(user);
                let slot = pw.push(bucket_of(person), Pr { person, ts, pt });
                if pt >> 24 == WIDE_CAT {
                    my_wide.push((slot, cat));
                }
                ew.push(bucket_of(eid), Er { eid, ingest, slot });
            }
            evb.clear();
        };
        let flush_ck = |ckb: &mut Vec<Ck>, cw: &mut arena::Writer<Ck>| {
            for c in ckb.iter() {
                people.prefetch(c.person);
            }
            for c in ckb.drain(..) {
                let mut c = c;
                c.person = people.get(c.person);
                cw.push(bucket_of(c.person), c);
            }
        };
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= work.len() {
                break;
            }
            let (is_event, piece) = work[i];
            let inp = if is_event { &events_in } else { &clicks_in };
            let (chunk, lim): (&[u8], usize) = if inp.maps.is_empty() {
                // Fallback: no mapping. The buffer keeps zero padding after
                // the data, so 16-byte loads past any field stay inside it.
                let (a, b) = load(inp, piece, &mut buf);
                (&buf[a..b], buf.as_ptr() as usize + buf.len())
            } else {
                let (m, a, b) = inp.view(piece);
                (&m[a..b], m.as_ptr() as usize + m.len())
            };
            if is_event {
                scan::<7, _>(chunk, |f| match parse_event(f, &sm, lim) {
                    Some(Line::Valid(eid, ingest, user, ts, pt, cat)) => {
                        evb.push((eid, ingest, user, ts, pt, cat));
                        if evb.len() == BATCH {
                            flush_ev(&mut evb, &mut pw, &mut ew, &mut my_wide);
                        }
                    }
                    Some(Line::Invalid(eid, ingest)) => {
                        ew.push(bucket_of(eid), Er { eid, ingest, slot: NO_SLOT });
                    }
                    None => {}
                });
                flush_ev(&mut evb, &mut pw, &mut ew, &mut my_wide);
            } else {
                scan::<4, _>(chunk, |f| {
                    if let Some(c) = parse_click(f, &sm, lim) {
                        ckb.push(c);
                        if ckb.len() == BATCH {
                            flush_ck(&mut ckb, &mut cw);
                        }
                    }
                });
                flush_ck(&mut ckb, &mut cw);
            }
        }
        if !my_wide.is_empty() {
            wide.lock().unwrap().extend(my_wide);
        }
        *filled[t].lock().unwrap() = Some((pw.finish(), ew.finish(), cw.finish()));
        add_cpu(0, thread_cpu() - c0);
    });
    let (mut pf, mut ef, mut cf) = (Vec::new(), Vec::new(), Vec::new());
    for m in filled {
        let (p, e, c) = m.into_inner().unwrap().unwrap();
        pf.push(p);
        ef.push(e);
        cf.push(c);
    }
    let pr_lists = arena::merge(pf, BUCKETS);
    let er_lists = arena::merge(ef, BUCKETS);
    let ck_lists = arena::merge(cf, BUCKETS);
    let wide = wide.into_inner().unwrap();
    mark("parse");

    // ---- dedup: mark the losing copies dead ----
    let dead: Vec<AtomicU64> = (0..pr_arena.slots() / 64 + 1).map(|_| AtomicU64::new(0)).collect();
    let kill = |s: u32| {
        if s != NO_SLOT {
            dead[s as usize / 64].fetch_or(1 << (s % 64), Ordering::Relaxed);
        }
    };
    let next = AtomicUsize::new(0);
    run_parallel(threads, |_t| {
        let c0 = thread_cpu();
        let mut seen: HashMap<Key, (i32, u32), KeyBuild> = HashMap::with_hasher(KeyBuild);
        loop {
            let b = next.fetch_add(1, Ordering::Relaxed);
            if b >= BUCKETS {
                break;
            }
            let n: usize = er_lists[b].iter().map(|x| x.1 as usize).sum();
            seen.clear();
            seen.reserve(n);
            // The copy with the smallest ingest_ts is kept; copies with equal
            // ingest_ts are identical, so the first one seen stays. Every
            // other copy is dead, and if the kept copy is invalid, so is the
            // whole event.
            for &(blk, cnt) in &er_lists[b] {
                for r in er_arena.block(blk, cnt) {
                    match seen.entry(r.eid) {
                        hashbrown::hash_map::Entry::Vacant(v) => {
                            v.insert((r.ingest, r.slot));
                        }
                        hashbrown::hash_map::Entry::Occupied(mut o) => {
                            let (ci, cs) = *o.get();
                            if r.ingest < ci {
                                kill(cs);
                                o.insert((r.ingest, r.slot));
                            } else {
                                kill(r.slot);
                            }
                        }
                    }
                }
            }
        }
        add_cpu(1, thread_cpu() - c0);
    });
    drop(er_arena);
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
    // Largest buckets first, so a bucket holding a very active person does
    // not leave one thread working alone at the end.
    let mut order: Vec<usize> = (0..BUCKETS).collect();
    let size_of = |b: usize| -> usize {
        pr_lists[b].iter().map(|x| x.1 as usize).sum::<usize>() + ck_lists[b].iter().map(|x| x.1 as usize).sum::<usize>()
    };
    let sizes: Vec<usize> = (0..BUCKETS).map(size_of).collect();
    order.sort_by_key(|&b| std::cmp::Reverse(sizes[b]));
    let next = AtomicUsize::new(0);
    let camp_aggs: Vec<Mutex<CampAgg>> = (0..threads).map(|_| Mutex::new(Vec::new())).collect();
    run_parallel(threads, |t| {
        let c0 = thread_cpu();
        let mut agg: CampAgg = vec![[0; 4]; (ncamp + 1) * ndays];
        let mut stamp: Vec<u64> = vec![0; ncamp + 1];
        let mut stamp_gen: u64 = 0;
        let mut rows = out::UserRows::new(&sm.cat_names);
        let mut evs: Vec<Sv> = Vec::new();
        let mut cks: Vec<Ck> = Vec::new();
        let mut day_cats: Vec<(u16, i64)> = Vec::new();
        let mut ev_starts: Vec<u32> = Vec::new();
        let mut ck_starts: Vec<u32> = Vec::new();
        loop {
            let k = next.fetch_add(1, Ordering::Relaxed);
            if k >= BUCKETS {
                break;
            }
            let b = order[k];
            // Live events of this bucket, radix-partitioned by a few hash
            // bits of the person; then each small part is sorted by
            // (person, ts). Within every part persons come out in key order
            // for both events and clicks.
            gather_events(&pr_arena, &pr_lists[b], &dead, &wide, &mut evs, &mut ev_starts);
            gather_clicks(&ck_arena, &ck_lists[b], &mut cks, &mut ck_starts);
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
                        if ev.cat != u16::MAX {
                            let price = ev.price as i64;
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
                    let di = (day - day0) as usize;
                    let hi = pc.partition_point(|c| c.ts <= start);
                    let lo = pc.partition_point(|c| (c.ts as i64) < start as i64 - WEEK);
                    let credit = if hi > lo {
                        let last = pc[hi - 1].ts;
                        let mut f = hi - 1;
                        while f > lo && pc[f - 1].ts == last {
                            f -= 1;
                        }
                        stamp_gen += 1;
                        for c in &pc[lo..hi] {
                            let s = &mut stamp[c.camp as usize];
                            if *s != stamp_gen {
                                *s = stamp_gen;
                                agg[c.camp as usize * ndays + di][3] += 1;
                            }
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
const SUB: usize = 4096;

/// The part of a person within its bucket (hash bits not used by bucket_of).
#[inline(always)]
fn sub_of(k: Key) -> usize {
    hash_key(k) as usize & (SUB - 1)
}

/// Collects a bucket's live events into `out`, partitioned by `sub_of`;
/// `starts[p]..starts[p + 1]` is part p.
fn gather_events(
    arena: &Arena<Pr>,
    blocks: &[(u32, u16)],
    dead: &[AtomicU64],
    wide: &HashMap<u32, u16>,
    out: &mut Vec<Sv>,
    starts: &mut Vec<u32>,
) {
    starts.clear();
    starts.resize(SUB + 1, 0);
    let is_dead = |slot: usize| dead[slot / 64].load(Ordering::Relaxed) >> (slot % 64) & 1 != 0;
    let mut n = 0usize;
    for &(blk, cnt) in blocks {
        let base = blk as usize * BLOCK;
        for (i, r) in arena.block(blk, cnt).iter().enumerate() {
            if !is_dead(base + i) {
                starts[sub_of(r.person) + 1] += 1;
                n += 1;
            }
        }
    }
    for p in 0..SUB {
        starts[p + 1] += starts[p];
    }
    out.clear();
    out.reserve(n);
    let mut fill: Vec<u32> = starts[..SUB].to_vec();
    unsafe {
        let o = out.as_mut_ptr();
        for &(blk, cnt) in blocks {
            let base = blk as usize * BLOCK;
            for (i, r) in arena.block(blk, cnt).iter().enumerate() {
                if is_dead(base + i) {
                    continue;
                }
                let code = r.pt >> 24;
                let cat = match code {
                    0 => u16::MAX,
                    WIDE_CAT => wide[&((base + i) as u32)],
                    c => (c - 1) as u16,
                };
                let p = sub_of(r.person);
                o.add(fill[p] as usize).write(Sv { person: r.person, ts: r.ts, price: r.pt & PRICE_MASK, cat });
                fill[p] += 1;
            }
        }
        out.set_len(n);
    }
}

/// Collects a bucket's clicks into `out`, partitioned like the events.
fn gather_clicks(arena: &Arena<Ck>, blocks: &[(u32, u16)], out: &mut Vec<Ck>, starts: &mut Vec<u32>) {
    starts.clear();
    starts.resize(SUB + 1, 0);
    let mut n = 0usize;
    for &(blk, cnt) in blocks {
        for c in arena.block(blk, cnt) {
            starts[sub_of(c.person) + 1] += 1;
        }
        n += cnt as usize;
    }
    for p in 0..SUB {
        starts[p + 1] += starts[p];
    }
    out.clear();
    out.reserve(n);
    let mut fill: Vec<u32> = starts[..SUB].to_vec();
    unsafe {
        let o = out.as_mut_ptr();
        for &(blk, cnt) in blocks {
            for c in arena.block(blk, cnt) {
                let p = sub_of(c.person);
                o.add(fill[p] as usize).write(*c);
                fill[p] += 1;
            }
        }
        out.set_len(n);
    }
}

/// The category with the highest summed price; ties go to the smallest name,
/// which is the smallest rank.
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
