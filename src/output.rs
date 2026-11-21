//! A minimal Parquet writer for flat tables: one row group, one PLAIN,
//! uncompressed data page (v1) per column, Thrift-compact metadata.
//! Supported columns: required UTF8 strings, optional UTF8 strings,
//! required DATE (INT32) and required INT64.

use std::fs::File;
use std::io::Write;
use std::path::Path;

pub enum Col<'a> {
    Str(Vec<&'a [u8]>),
    OptStr(Vec<Option<&'a [u8]>>),
    Date(Vec<i32>),
    I64(Vec<i64>),
}

impl Col<'_> {
    fn len(&self) -> usize {
        match self {
            Col::Str(v) => v.len(),
            Col::OptStr(v) => v.len(),
            Col::Date(v) => v.len(),
            Col::I64(v) => v.len(),
        }
    }
    /// (parquet physical type, converted type, logical type field id)
    fn types(&self) -> (i32, Option<i32>, Option<i16>) {
        match self {
            Col::Str(_) | Col::OptStr(_) => (6, Some(0), Some(1)), // BYTE_ARRAY, UTF8, STRING
            Col::Date(_) => (1, Some(6), Some(6)),                 // INT32, DATE, DATE
            Col::I64(_) => (2, None, None),                        // INT64
        }
    }
    fn optional(&self) -> bool {
        matches!(self, Col::OptStr(_))
    }
    /// Page body: definition levels (optional columns) then PLAIN values.
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Col::Str(v) => {
                for s in v {
                    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    out.extend_from_slice(s);
                }
            }
            Col::OptStr(v) => {
                // RLE/bit-packed hybrid, bit width 1, all bit-packed groups of 8.
                let groups = v.len().div_ceil(8);
                let mut lv = Vec::with_capacity(groups + 8);
                varint(&mut lv, ((groups as u64) << 1) | 1);
                for g in v.chunks(8) {
                    let mut b = 0u8;
                    for (i, x) in g.iter().enumerate() {
                        b |= (x.is_some() as u8) << i;
                    }
                    lv.push(b);
                }
                out.extend_from_slice(&(lv.len() as u32).to_le_bytes());
                out.extend_from_slice(&lv);
                for s in v.iter().flatten() {
                    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    out.extend_from_slice(s);
                }
            }
            Col::Date(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Col::I64(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
        }
    }
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Thrift compact protocol writer.
struct Thrift {
    out: Vec<u8>,
    last: Vec<i16>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Thrift {
    fn new() -> Self {
        Thrift { out: Vec::new(), last: vec![0] }
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        if delta > 0 && delta <= 15 {
            self.out.push(((delta as u8) << 4) | ty);
        } else {
            self.out.push(ty);
            let id = id as i64;
            varint(&mut self.out, ((id << 1) ^ (id >> 63)) as u64);
        }
        *last = id;
    }
    fn zz(&mut self, v: i64) {
        varint(&mut self.out, ((v << 1) ^ (v >> 63)) as u64);
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zz(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zz(v);
    }
    fn bin(&mut self, id: i16, b: &[u8]) {
        self.field(id, T_BINARY);
        self.raw_bin(b);
    }
    fn raw_bin(&mut self, b: &[u8]) {
        varint(&mut self.out, b.len() as u64);
        self.out.extend_from_slice(b);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.out.push(((n as u8) << 4) | elem);
        } else {
            self.out.push(0xF0 | elem);
            varint(&mut self.out, n as u64);
        }
    }
    /// Starts a struct as field `id` (or as a list element when `id` is None).
    fn begin(&mut self, id: Option<i16>) {
        if let Some(id) = id {
            self.field(id, T_STRUCT);
        }
        self.last.push(0);
    }
    fn end(&mut self) {
        self.out.push(0);
        self.last.pop();
    }
}

pub fn write(path: &Path, cols: &[(&str, Col)]) {
    let nrows = cols.first().map_or(0, |c| c.1.len());
    let mut file: Vec<u8> = Vec::new();
    file.extend_from_slice(b"PAR1");
    let mut chunks = Vec::new(); // (offset, total size)
    let mut body = Vec::new();
    for (_, col) in cols {
        assert_eq!(col.len(), nrows);
        body.clear();
        col.encode(&mut body);
        assert!(body.len() < i32::MAX as usize, "page too large");
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, body.len() as i32);
        h.i32(3, body.len() as i32);
        h.begin(Some(5));
        h.i32(1, nrows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE
        h.i32(4, 3); // RLE
        h.end();
        h.out.push(0);
        let off = file.len();
        file.extend_from_slice(&h.out);
        file.extend_from_slice(&body);
        chunks.push((off, file.len() - off));
    }

    let mut m = Thrift::new();
    m.i32(1, 1); // version
    m.list(2, T_STRUCT, cols.len() + 1);
    m.begin(None);
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32);
    m.end();
    for (name, col) in cols {
        let (ty, conv, logical) = col.types();
        m.begin(None);
        m.i32(1, ty);
        m.i32(3, if col.optional() { 1 } else { 0 });
        m.bin(4, name.as_bytes());
        if let Some(c) = conv {
            m.i32(6, c);
        }
        if let Some(l) = logical {
            m.begin(Some(10));
            m.begin(Some(l));
            m.end();
            m.end();
        }
        m.end();
    }
    m.i64(3, nrows as i64);
    m.list(4, T_STRUCT, 1);
    m.begin(None);
    m.list(1, T_STRUCT, cols.len());
    let mut total = 0i64;
    for ((name, col), &(off, size)) in cols.iter().zip(&chunks) {
        let (ty, _, _) = col.types();
        total += size as i64;
        m.begin(None);
        m.i64(2, off as i64);
        m.begin(Some(3));
        m.i32(1, ty);
        m.list(2, T_I32, 2);
        m.zz(0); // PLAIN
        m.zz(3); // RLE
        m.list(3, T_BINARY, 1);
        m.raw_bin(name.as_bytes());
        m.i32(4, 0); // UNCOMPRESSED
        m.i64(5, nrows as i64);
        m.i64(6, size as i64);
        m.i64(7, size as i64);
        m.i64(9, off as i64);
        m.end();
        m.end();
    }
    m.i64(2, total);
    m.i64(3, nrows as i64);
    m.end();
    m.bin(6, b"shopping-sessions");
    m.out.push(0);
    file.extend_from_slice(&m.out);
    file.extend_from_slice(&(m.out.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    let mut f = File::create(path).expect("create parquet");
    f.write_all(&file).expect("write parquet");
}
