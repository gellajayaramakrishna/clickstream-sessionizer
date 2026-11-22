use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use crate::{unpack, URow};

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_data_page_size_limit(8 << 20)
            .set_write_batch_size(1 << 20)
            .build(),
    )
}

const USER_MSG: &str = "message schema {
  required binary user_id (STRING);
  required int32 day (DATE);
  required int64 sessions;
  required int64 events;
  required int64 revenue_cents;
  optional binary top_category (STRING);
}";

const CAMP_MSG: &str = "message schema {
  required binary campaign_id (STRING);
  required binary channel (STRING);
  required int32 day (DATE);
  required int64 sessions;
  required int64 purchasing_sessions;
  required int64 revenue_cents;
  required int64 assisted_sessions;
}";

fn ba(b: &'static [u8]) -> ByteArray {
    ByteArray::from(Bytes::from_static(b))
}

pub fn write_user(path: &Path, rows: &[URow], cats: &[&'static [u8]]) {
    let schema = Arc::new(parse_message_type(USER_MSG).unwrap());
    let file = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(file, schema, props()).unwrap();
    if !rows.is_empty() {
        let mut rg = w.next_row_group().unwrap();
        let mut ci = 0;
        while let Some(mut col) = rg.next_column().unwrap() {
            match ci {
                0 => {
                    let v: Vec<ByteArray> = rows.iter().map(|r| ba(unpack(r.nref))).collect();
                    col.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                1 => {
                    let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                    col.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
                }
                2 | 3 | 4 => {
                    let v: Vec<i64> = rows
                        .iter()
                        .map(|r| match ci {
                            2 => r.sessions,
                            3 => r.events,
                            _ => r.revenue,
                        })
                        .collect();
                    col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
                _ => {
                    let defs: Vec<i16> = rows.iter().map(|r| (r.top >= 0) as i16).collect();
                    let v: Vec<ByteArray> = rows.iter().filter(|r| r.top >= 0).map(|r| ba(cats[r.top as usize])).collect();
                    col.typed::<ByteArrayType>().write_batch(&v, Some(&defs), None).unwrap();
                }
            }
            col.close().unwrap();
            ci += 1;
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}

pub fn write_campaign(path: &Path, ids: &[&'static [u8]], chs: &[&'static [u8]], rows: &[[i64; 5]]) {
    let schema = Arc::new(parse_message_type(CAMP_MSG).unwrap());
    let file = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(file, schema, props()).unwrap();
    if !rows.is_empty() {
        let mut rg = w.next_row_group().unwrap();
        let mut ci = 0;
        while let Some(mut col) = rg.next_column().unwrap() {
            match ci {
                0 => {
                    let v: Vec<ByteArray> = ids.iter().map(|b| ba(b)).collect();
                    col.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                1 => {
                    let v: Vec<ByteArray> = chs.iter().map(|b| ba(b)).collect();
                    col.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                2 => {
                    let v: Vec<i32> = rows.iter().map(|r| r[0] as i32).collect();
                    col.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
                }
                _ => {
                    let v: Vec<i64> = rows.iter().map(|r| r[ci - 2]).collect();
                    col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
            }
            col.close().unwrap();
            ci += 1;
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}
