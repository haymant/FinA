//! Typed columnar writer used by the streaming ETL engine (sonic-rs only).
#![allow(dead_code)]

use std::fs::File;
use std::sync::Arc;
use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

/// Typed representation of a column's cell values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColKind {
    Str,
    F64,
    I64,
    Bool,
}

/// A single output column: a name plus a typed, nullable list of values.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub kind: ColKind,
    pub strings: Vec<Option<String>>,
    pub f64s: Vec<Option<f64>>,
    pub i64s: Vec<Option<i64>>,
    pub bools: Vec<Option<bool>>,
}

impl Column {
    pub fn new(name: &str, kind: ColKind) -> Self {
        Column {
            name: name.to_string(),
            kind,
            strings: Vec::new(),
            f64s: Vec::new(),
            i64s: Vec::new(),
            bools: Vec::new(),
        }
    }

    pub fn kind_dtype(kind: ColKind) -> DataType {
        match kind {
            ColKind::Str => DataType::Utf8,
            ColKind::F64 => DataType::Float64,
            ColKind::I64 => DataType::Int64,
            ColKind::Bool => DataType::Boolean,
        }
    }

    pub fn push(&mut self, v: Option<Cell>) {
        let cell = v.unwrap_or(Cell::Null);
        match self.kind {
            ColKind::Str => self.strings.push(cell.into_str()),
            ColKind::F64 => self.f64s.push(cell.into_f64()),
            ColKind::I64 => self.i64s.push(cell.into_i64()),
            ColKind::Bool => self.bools.push(cell.into_bool()),
        }
    }

    pub fn dtype(&self) -> DataType {
        match self.kind {
            ColKind::Str => DataType::Utf8,
            ColKind::F64 => DataType::Float64,
            ColKind::I64 => DataType::Int64,
            ColKind::Bool => DataType::Boolean,
        }
    }

    /// Builds an array for rows in `[start, end)` only (used for chunked writes).
    fn slice_array(&self, start: usize, end: usize) -> ArrayRef {
        match self.kind {
            ColKind::Str => Arc::new(StringArray::from(
                self.strings[start..end]
                    .iter()
                    .map(|o| o.as_deref())
                    .collect::<Vec<_>>(),
            )),
            ColKind::F64 => Arc::new(Float64Array::from(self.f64s[start..end].to_vec())),
            ColKind::I64 => Arc::new(Int64Array::from(self.i64s[start..end].to_vec())),
            ColKind::Bool => Arc::new(BooleanArray::from(self.bools[start..end].to_vec())),
        }
    }
}

/// A temporary cell used while pushing, then coerced to the column type.
#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Str(String),
    F64(f64),
    I64(i64),
    Bool(bool),
}

impl Cell {
    fn into_str(self) -> Option<String> {
        match self {
            Cell::Null => None,
            Cell::Str(s) => Some(s),
            Cell::F64(f) => Some(format!("{}", f)),
            Cell::I64(i) => Some(format!("{}", i)),
            Cell::Bool(b) => Some(if b { "true" } else { "false" }.to_string()),
        }
    }
    fn into_f64(self) -> Option<f64> {
        match self {
            Cell::Null => None,
            Cell::F64(f) => Some(f),
            Cell::I64(i) => Some(i as f64),
            Cell::Bool(b) => Some(if b { 1.0 } else { 0.0 }),
            Cell::Str(s) => s.trim().parse().ok(),
        }
    }
    fn into_i64(self) -> Option<i64> {
        match self {
            Cell::Null => None,
            Cell::I64(i) => Some(i),
            Cell::F64(f) => Some(f as i64),
            Cell::Bool(b) => Some(if b { 1 } else { 0 }),
            Cell::Str(s) => s.trim().parse().ok(),
        }
    }
    fn into_bool(self) -> Option<bool> {
        match self {
            Cell::Null => None,
            Cell::Bool(b) => Some(b),
            Cell::I64(i) => Some(i != 0),
            Cell::F64(f) => Some(f != 0.0),
            Cell::Str(s) => match s.trim() {
                "true" | "True" | "1" => Some(true),
                "false" | "False" | "0" => Some(false),
                _ => None,
            },
        }
    }
}

impl From<&str> for Cell {
    fn from(v: &str) -> Self {
        Cell::Str(v.to_string())
    }
}
impl From<String> for Cell {
    fn from(v: String) -> Self {
        Cell::Str(v)
    }
}
impl From<f64> for Cell {
    fn from(v: f64) -> Self {
        Cell::F64(v)
    }
}
impl From<i64> for Cell {
    fn from(v: i64) -> Self {
        Cell::I64(v)
    }
}
impl From<bool> for Cell {
    fn from(v: bool) -> Self {
        Cell::Bool(v)
    }
}

/// Writes a set of columns to a single-column-group Parquet file at `path`.
/// All columns must share the same row count. Rows are written in multiple
/// row-groups so a very large string column (e.g. a full-record `json_blob`)
/// never exceeds Arrow's i32 offset limit or one giant in-memory batch.
pub fn write_parquet(path: &str, columns: &[Column]) -> Result<(), String> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, c.dtype(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let rows = columns.first().map(|c| c.len()).unwrap_or(0);
    if rows == 0 {
        return Ok(());
    }

    // Chunk into row-groups: 8192 rows each keeps each batch's offsets and
    // buffers modest even for wide/long string columns.
    const CHUNK: usize = 8192;

    let file = File::create(path).map_err(|e| format!("create {path}: {e}"))?;
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|e| format!("writer: {e}"))?;

    let mut start = 0;
    while start < rows {
        let end = (start + CHUNK).min(rows);
        let arrays: Vec<ArrayRef> = columns.iter().map(|c| c.slice_array(start, end)).collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| format!("batch build failed: {e}"))?;
        writer.write(&batch).map_err(|e| format!("parquet write: {e}"))?;
        start = end;
    }

    writer.close().map_err(|e| format!("parquet close: {e}"))?;
    Ok(())
}

/// Streaming Parquet writer: accumulates row-group batches over time. Columns
/// are provided incrementally (each call writes one row-group and clears them)
/// so the full dataset is never materialized in memory.
pub struct ParquetSink {
    schema: Arc<Schema>,
    writer: Option<parquet::arrow::ArrowWriter<File>>,
}

impl ParquetSink {
    /// Creates a sink for `fields` writing to `path`.
    pub fn new(path: &str, fields: &[(String, ColKind)]) -> Result<Self, String> {
        let fields: Vec<Field> = fields
            .iter()
            .map(|(n, k)| Field::new(n, Column::kind_dtype(*k), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let file = File::create(path).map_err(|e| format!("create {path}: {e}"))?;
        let props = parquet::file::properties::WriterProperties::builder().build();
        let writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| format!("writer: {e}"))?;
        Ok(ParquetSink {
            schema,
            writer: Some(writer),
        })
    }

    /// Writes the current rows of `columns` as one row-group and clears them.
    /// All columns must have equal length.
    pub fn write_chunk(&mut self, columns: &mut [Column]) -> Result<(), String> {
        let rows = columns.first().map(|c| c.len()).unwrap_or(0);
        if rows == 0 {
            return Ok(());
        }
        let arrays: Vec<ArrayRef> = columns
            .iter()
            .map(|c| c.slice_array(0, c.len()))
            .collect();
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| format!("batch build failed: {e}"))?;
        self.writer
            .as_mut()
            .unwrap()
            .write(&batch)
            .map_err(|e| format!("parquet write: {e}"))?;
        for c in columns.iter_mut() {
            c.clear();
        }
        Ok(())
    }

    /// Finalizes the file.
    pub fn close(mut self) -> Result<(), String> {
        if let Some(w) = self.writer.take() {
            w.close().map_err(|e| format!("parquet close: {e}"))?;
        }
        Ok(())
    }
}

impl Column {
    /// Human/safe string form of the value at row `i` (used for Hive partition
    /// paths and for join keys).
    pub fn cell_str(&self, i: usize) -> Option<String> {
        match self.kind {
            ColKind::Str => self.strings.get(i).cloned().flatten(),
            ColKind::F64 => self.f64s.get(i).cloned().flatten().map(|v| format!("{}", v)),
            ColKind::I64 => self.i64s.get(i).cloned().flatten().map(|v| v.to_string()),
            ColKind::Bool => self.bools.get(i).cloned().flatten().map(|v| v.to_string()),
        }
    }

    /// Returns a new column containing only the rows at the given indices.
    pub fn gather(&self, rows: &[usize]) -> Column {
        let mut c = Column::new(&self.name, self.kind);
        for &i in rows {
            c.push(self.get_cell(i));
        }
        c
    }

    pub fn get_cell(&self, i: usize) -> Option<Cell> {
        match self.kind {
            ColKind::Str => self.strings.get(i).cloned().flatten().map(Cell::Str),
            ColKind::F64 => self.f64s.get(i).cloned().flatten().map(Cell::F64),
            ColKind::I64 => self.i64s.get(i).cloned().flatten().map(Cell::I64),
            ColKind::Bool => self.bools.get(i).cloned().flatten().map(Cell::Bool),
        }
    }

    pub fn len(&self) -> usize {
        match self.kind {
            ColKind::Str => self.strings.len(),
            ColKind::F64 => self.f64s.len(),
            ColKind::I64 => self.i64s.len(),
            ColKind::Bool => self.bools.len(),
        }
    }

    /// Drops all buffered rows (after a row-group has been flushed to disk).
    pub fn clear(&mut self) {
        self.strings.clear();
        self.f64s.clear();
        self.i64s.clear();
        self.bools.clear();
    }
}

/// Writes `columns` to parquet under a Hive-style directory layout, e.g.
/// `<base>/name=AAPL/type=FCN/data.parquet`. The partition columns are removed
/// from the data file (HIVE convention) and expressed as directories. Null
/// partition values are written as `__HIVE_DEFAULT_PARTITION__`.
pub fn write_partitioned(base: &str, columns: &[Column], partition_by: &[String]) -> Result<(), String> {
    if partition_by.is_empty() {
        return write_parquet(base, columns);
    }
    let nrows = columns.first().map(|c| c.len()).unwrap_or(0);
    if nrows == 0 {
        return Ok(());
    }
    let part_idx: Vec<usize> = partition_by
        .iter()
        .map(|k| columns.iter().position(|c| &c.name == k))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| "partition column not found in dataset columns".to_string())?;

    // group row indices by partition value tuple
    let mut groups: Vec<(Vec<String>, Vec<usize>)> = Vec::new();
    for i in 0..nrows {
        let key: Vec<String> = partition_by
            .iter()
            .enumerate()
            .map(|(j, _)| {
                let c = &columns[part_idx[j]];
                c.cell_str(i).unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".to_string())
            })
            .collect();
        match groups.iter_mut().find(|(k, _)| *k == key) {
            Some((_, rows)) => rows.push(i),
            None => groups.push((key, vec![i])),
        }
    }

    for (key, rows) in groups {
        let mut dir = base.to_string();
        for (j, cv) in key.iter().enumerate() {
            dir = format!("{}/{}={}", dir, partition_by[j], cv);
        }
        std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir {dir}: {e}"))?;
        let data_cols: Vec<Column> = columns
            .iter()
            .enumerate()
            .filter(|(i, _)| !part_idx.contains(i))
            .map(|(_, c)| c.gather(&rows))
            .collect();
        let path = format!("{}/data.parquet", dir);
        write_parquet(&path, &data_cols)?;
    }
    Ok(())
}
