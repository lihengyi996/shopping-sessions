//! Write the two Parquet tables the scorer reads. Uncompressed PLAIN columns
//! keep encode time tiny; row order does not matter.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::{Compression, Encoding};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use crate::ids::Id;

type Fail = Box<dyn std::error::Error + Send + Sync>;

const USER_SCHEMA: &str = "message user_daily {
    required binary user_id (UTF8);
    required int32 day (DATE);
    required int64 sessions;
    required int64 events;
    required int64 revenue_cents;
    optional binary top_category (UTF8);
}";

const CAMP_SCHEMA: &str = "message campaign_daily {
    required binary campaign_id (UTF8);
    required binary channel (UTF8);
    required int32 day (DATE);
    required int64 sessions;
    required int64 purchasing_sessions;
    required int64 revenue_cents;
    required int64 assisted_sessions;
}";

pub struct UserRow {
    pub person: Id,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    /// Category index into the product category table. None when the day has no purchase.
    pub top: Option<u16>,
}

#[derive(Clone, Copy, Default)]
pub struct CampCell {
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

fn writer(path: &Path, schema: &str) -> Result<SerializedFileWriter<File>, Fail> {
    let schema = Arc::new(parse_message_type(schema)?);
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(1 << 22)
            .set_write_batch_size(1 << 16)
            .build(),
    );
    Ok(SerializedFileWriter::new(File::create(path)?, schema, props)?)
}

pub fn write_user_daily(path: &Path, rows: &[UserRow], categories: &[Vec<u8>]) -> Result<(), Fail> {
    let mut file = writer(path, USER_SCHEMA)?;
    let mut group = file.next_row_group()?;
    let ids: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.person.bytes())).collect();
    write_bytes(&mut group, &ids, None)?;
    let days: Vec<i32> = rows.iter().map(|r| r.day).collect();
    write_i32(&mut group, &days)?;
    for pick in [
        (|r: &UserRow| r.sessions) as fn(&UserRow) -> i64,
        |r| r.events,
        |r| r.revenue,
    ] {
        let values: Vec<i64> = rows.iter().map(pick).collect();
        write_i64(&mut group, &values)?;
    }
    let levels: Vec<i16> = rows.iter().map(|r| i16::from(r.top.is_some())).collect();
    let tops: Vec<ByteArray> = rows
        .iter()
        .filter_map(|r| r.top)
        .map(|c| ByteArray::from(categories[c as usize].clone()))
        .collect();
    write_bytes(&mut group, &tops, Some(&levels))?;
    group.close()?;
    file.close()?;
    Ok(())
}

pub fn write_campaign_daily(
    path: &Path,
    cells: &[CampCell],
    days: usize,
    campaign_ids: &[Vec<u8>],
    channels: &[Vec<u8>],
    base_day: i32,
    organic_idx: usize,
) -> Result<(), Fail> {
    let mut ids = Vec::new();
    let mut chans = Vec::new();
    let mut day_col = Vec::new();
    let mut cols: [Vec<i64>; 4] = Default::default();
    for (c, id) in campaign_ids.iter().enumerate() {
        for d in 0..days {
            let cell = cells[c * days + d];
            if cell.sessions == 0 && cell.assisted == 0 {
                continue;
            }
            ids.push(ByteArray::from(id.clone()));
            chans.push(ByteArray::from(channels[c].clone()));
            day_col.push(base_day + d as i32);
            cols[0].push(cell.sessions);
            cols[1].push(cell.purchasing);
            cols[2].push(cell.revenue);
            let assisted = if c == organic_idx { 0 } else { cell.assisted };
            cols[3].push(assisted);
        }
    }
    let mut file = writer(path, CAMP_SCHEMA)?;
    let mut group = file.next_row_group()?;
    write_bytes(&mut group, &ids, None)?;
    write_bytes(&mut group, &chans, None)?;
    write_i32(&mut group, &day_col)?;
    for values in &cols {
        write_i64(&mut group, values)?;
    }
    group.close()?;
    file.close()?;
    Ok(())
}

type Group<'a> = parquet::file::writer::SerializedRowGroupWriter<'a, File>;

fn write_bytes(group: &mut Group<'_>, values: &[ByteArray], levels: Option<&[i16]>) -> Result<(), Fail> {
    let mut col = group.next_column()?.ok_or("missing column")?;
    col.typed::<ByteArrayType>().write_batch(values, levels, None)?;
    col.close()?;
    Ok(())
}

fn write_i32(group: &mut Group<'_>, values: &[i32]) -> Result<(), Fail> {
    let mut col = group.next_column()?.ok_or("missing column")?;
    col.typed::<Int32Type>().write_batch(values, None, None)?;
    col.close()?;
    Ok(())
}

fn write_i64(group: &mut Group<'_>, values: &[i64]) -> Result<(), Fail> {
    let mut col = group.next_column()?.ok_or("missing column")?;
    col.typed::<Int64Type>().write_batch(values, None, None)?;
    col.close()?;
    Ok(())
}
