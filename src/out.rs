//! A small Parquet writer for the two output tables.
//!
//! Each file holds one row group with one uncompressed PLAIN data page per
//! column. Rows are encoded straight into per-column byte buffers as they are
//! produced, so writing is little more than a memcpy. Metadata is written
//! with the Thrift compact protocol.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::key::{key_write, Key};

// Parquet physical types.
const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;
// Converted types.
const UTF8: i32 = 0;
const DATE: i32 = 6;

/// One column: its encoded PLAIN values, and its definition levels if it is
/// nullable.
pub struct Column {
    name: &'static str,
    ptype: i32,
    converted: Option<i32>,
    optional: bool,
    values: Vec<u8>,
    defs: Vec<bool>,
}

impl Column {
    fn new(name: &'static str, ptype: i32, converted: Option<i32>, optional: bool) -> Self {
        Column { name, ptype, converted, optional, values: Vec::new(), defs: Vec::new() }
    }
    #[inline(always)]
    fn i32(&mut self, v: i32) {
        self.values.extend_from_slice(&v.to_le_bytes());
    }
    #[inline(always)]
    fn i64(&mut self, v: i64) {
        self.values.extend_from_slice(&v.to_le_bytes());
    }
    #[inline(always)]
    fn bytes(&mut self, b: &[u8]) {
        self.values.extend_from_slice(&(b.len() as u32).to_le_bytes());
        self.values.extend_from_slice(b);
    }
}

// ---- Thrift compact protocol ----

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Thrift {
    fn new() -> Self {
        Thrift { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, t: u8) {
        let last = *self.last.last().unwrap();
        let delta = id - last;
        if delta > 0 && delta <= 15 {
            self.buf.push(((delta as u8) << 4) | t);
        } else {
            self.buf.push(t);
            self.zigzag(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zigzag(v);
    }
    fn string(&mut self, id: i16, s: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(s.len() as u64);
        self.buf.extend_from_slice(s);
    }
    fn begin_struct(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    /// Begins a struct that is a list element (no field header).
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        self.list_header(elem, n);
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

/// Definition levels with bit width 1 as one bit-packed run of the
/// RLE/bit-packed hybrid encoding, prefixed with its byte length.
fn def_levels(defs: &[bool]) -> Vec<u8> {
    let groups = defs.len().div_ceil(8);
    let mut run = Vec::with_capacity(groups + 10);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        run.push((h as u8) | 0x80);
        h >>= 7;
    }
    run.push(h as u8);
    let start = run.len();
    run.resize(start + groups, 0);
    for (i, &d) in defs.iter().enumerate() {
        if d {
            run[start + i / 8] |= 1 << (i % 8);
        }
    }
    let mut out = Vec::with_capacity(run.len() + 4);
    out.extend_from_slice(&(run.len() as u32).to_le_bytes());
    out.extend_from_slice(&run);
    out
}

fn write_file(path: &Path, cols: &[Column], rows: usize) {
    let mut f = std::io::BufWriter::with_capacity(4 << 20, File::create(path).expect("create parquet"));
    f.write_all(b"PAR1").unwrap();
    let mut offset: i64 = 4;
    let mut metas = Vec::with_capacity(cols.len());
    for c in cols {
        let defs = if c.optional { def_levels(&c.defs) } else { Vec::new() };
        let page_size = (defs.len() + c.values.len()) as i32;
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, page_size);
        h.i32(3, page_size);
        h.begin_struct(5);
        h.i32(1, rows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE
        h.i32(4, 3); // RLE
        h.end_struct();
        h.buf.push(0);
        f.write_all(&h.buf).unwrap();
        f.write_all(&defs).unwrap();
        f.write_all(&c.values).unwrap();
        let chunk_size = h.buf.len() as i64 + page_size as i64;
        metas.push((offset, chunk_size));
        offset += chunk_size;
    }

    let mut m = Thrift::new();
    m.i32(1, 1);
    m.list(2, T_STRUCT, cols.len() + 1);
    m.begin_elem();
    m.string(4, b"schema");
    m.i32(5, cols.len() as i32);
    m.end_struct();
    for c in cols {
        m.begin_elem();
        m.i32(1, c.ptype);
        m.i32(3, if c.optional { 1 } else { 0 });
        m.string(4, c.name.as_bytes());
        if let Some(ct) = c.converted {
            m.i32(6, ct);
            // LogicalType union: STRING = 1, DATE = 6, each an empty struct.
            m.begin_struct(10);
            m.begin_struct(if ct == DATE { 6 } else { 1 });
            m.end_struct();
            m.end_struct();
        }
        m.end_struct();
    }
    m.i64(3, rows as i64);
    m.list(4, T_STRUCT, 1);
    m.begin_elem();
    m.list(1, T_STRUCT, cols.len());
    let total: i64 = metas.iter().map(|x| x.1).sum();
    for (c, &(off, size)) in cols.iter().zip(&metas) {
        m.begin_elem();
        m.i64(2, off);
        m.begin_struct(3);
        m.i32(1, c.ptype);
        m.list(2, T_I32, 2);
        m.zigzag(0); // PLAIN
        m.zigzag(3); // RLE
        m.list(3, T_BINARY, 1);
        m.varint(c.name.len() as u64);
        m.buf.extend_from_slice(c.name.as_bytes());
        m.i32(4, 0); // UNCOMPRESSED
        m.i64(5, rows as i64);
        m.i64(6, size);
        m.i64(7, size);
        m.i64(9, off);
        m.end_struct();
        m.end_struct();
    }
    m.i64(2, total);
    m.i64(3, rows as i64);
    m.end_struct();
    m.buf.push(0);
    f.write_all(&m.buf).unwrap();
    f.write_all(&(m.buf.len() as u32).to_le_bytes()).unwrap();
    f.write_all(b"PAR1").unwrap();
    f.flush().unwrap();
}

/// The `user_daily` table, encoded as rows are pushed.
pub struct UserRows {
    cols: Vec<Column>,
    cat_names: Vec<Vec<u8>>,
    rows: usize,
    scratch: Vec<u8>,
}

impl UserRows {
    pub fn new(cat_names: &[Vec<u8>]) -> Self {
        UserRows {
            cols: vec![
                Column::new("user_id", BYTE_ARRAY, Some(UTF8), false),
                Column::new("day", INT32, Some(DATE), false),
                Column::new("sessions", INT64, None, false),
                Column::new("events", INT64, None, false),
                Column::new("revenue_cents", INT64, None, false),
                Column::new("top_category", BYTE_ARRAY, Some(UTF8), true),
            ],
            cat_names: cat_names.to_vec(),
            rows: 0,
            scratch: Vec::with_capacity(32),
        }
    }

    #[inline]
    pub fn push(&mut self, user: Key, day: i64, sessions: i64, events: i64, revenue: i64, top: Option<u16>) {
        self.scratch.clear();
        key_write(user, &mut self.scratch);
        self.cols[0].bytes(&self.scratch);
        self.cols[1].i32(day as i32);
        self.cols[2].i64(sessions);
        self.cols[3].i64(events);
        self.cols[4].i64(revenue);
        match top {
            Some(c) => {
                let name = &self.cat_names[c as usize];
                self.cols[5].values.extend_from_slice(&(name.len() as u32).to_le_bytes());
                self.cols[5].values.extend_from_slice(name);
                self.cols[5].defs.push(true);
            }
            None => self.cols[5].defs.push(false),
        }
        self.rows += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub fn write(&self, path: &Path) {
        write_file(path, &self.cols, self.rows);
    }
}

/// The `campaign_daily` table.
pub struct CampRows {
    cols: Vec<Column>,
    rows: usize,
}

impl CampRows {
    pub fn new() -> Self {
        CampRows {
            cols: vec![
                Column::new("campaign_id", BYTE_ARRAY, Some(UTF8), false),
                Column::new("channel", BYTE_ARRAY, Some(UTF8), false),
                Column::new("day", INT32, Some(DATE), false),
                Column::new("sessions", INT64, None, false),
                Column::new("purchasing_sessions", INT64, None, false),
                Column::new("revenue_cents", INT64, None, false),
                Column::new("assisted_sessions", INT64, None, false),
            ],
            rows: 0,
        }
    }

    pub fn push(&mut self, id: &[u8], channel: &[u8], day: i32, a: [i64; 4]) {
        self.cols[0].bytes(id);
        self.cols[1].bytes(channel);
        self.cols[2].i32(day);
        for q in 0..4 {
            self.cols[3 + q].i64(a[q]);
        }
        self.rows += 1;
    }

    pub fn write(&self, path: &Path) {
        write_file(path, &self.cols, self.rows);
    }
}
