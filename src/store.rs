//! Store abstraction: a duckdb-style URI pointing at either a filesystem path
//! (JSON / parquet) or a duckdb table (in-memory or file-backed). Everything
//! heavy lives here in Rust; the Python layer only passes URIs through.

use crate::columnar::{ColKind, Column};
use duckdb::Connection;
use duckdb::types::Value as DbValue;

/// A parsed store URI.
#[derive(Debug, Clone)]
pub enum Uri {
    /// A filesystem path. `file:///abs/path`, `file://rel/path` or a bare path.
    /// As an input this is a JSON document; as an output this is a parquet file
    /// (or a directory when partitioning).
    File { path: String },
    /// An in-memory duckdb table (shared across the whole `run_pipelines` call).
    Memory { table: String },
    /// A duckdb file-backed table: `duckdb://<path>?table=<name>`.
    DuckdbFile { path: String, table: String },
}

impl Uri {
    pub fn parse(s: &str) -> Uri {
        let s = s.trim();
        if let Some(rest) = s.strip_prefix("file://") {
            return Uri::File { path: rest.to_string() };
        }
        if let Some(rest) = s.strip_prefix("memory://") {
            return Uri::Memory { table: rest.to_string() };
        }
        if let Some(rest) = s.strip_prefix("duckdb://") {
            // rest = "<path>?table=<name>" or "<path>"
            let (path, table) = match rest.find('?') {
                Some(i) => {
                    let p = &rest[..i];
                    let q = &rest[i + 1..];
                    let t = q
                        .split('&')
                        .find_map(|kv| kv.strip_prefix("table="))
                        .unwrap_or("");
                    (p.to_string(), t.to_string())
                }
                None => (rest.to_string(), String::new()),
            };
            return Uri::DuckdbFile { path, table };
        }
        Uri::File { path: s.to_string() }
    }
}

/// The shared store. A single in-memory duckdb connection backs every
/// `memory://` table (and is used for join resolution) for one `run_pipelines`
/// invocation.
pub struct SharedDb {
    mem: Connection,
    /// persistent duckdb connections, one per file path.
    files: Vec<(String, Connection)>,
}

impl SharedDb {
    pub fn new() -> Result<SharedDb, String> {
        let mem = Connection::open_in_memory()
            .map_err(|e| format!("open in-memory duckdb: {e}"))?;
        Ok(SharedDb { mem, files: Vec::new() })
    }

    fn file_conn(&mut self, path: &str) -> Result<&Connection, String> {
        if !self.files.iter().any(|(p, _)| p == path) {
            let c = Connection::open(path).map_err(|e| format!("open duckdb file {path}: {e}"))?;
            self.files.push((path.to_string(), c));
        }
        let idx = self.files.iter().position(|(p, _)| p == path).unwrap();
        Ok(&self.files[idx].1)
    }

    /// Returns a queryable connection + qualified table name for a URI.
    fn resolve(&mut self, uri: &Uri) -> Result<(&Connection, String), String> {
        match uri {
            Uri::Memory { table } => {
                let t = if table.is_empty() {
                    "t".to_string()
                } else {
                    quote_ident(table)
                };
                Ok((&self.mem, t))
            }
            Uri::DuckdbFile { path, table } => {
                let c = self.file_conn(path)?;
                let t = if table.is_empty() {
                    "t".to_string()
                } else {
                    quote_ident(table)
                };
                Ok((c, t))
            }
            Uri::File { .. } => Err("store write/read requires a duckdb target".to_string()),
        }
    }

    /// Create a table and append typed columns. Used for `memory://`/`duckdb://`
    /// outputs and for the temporary datasets needed by joins.
    pub fn write_columns(
        &mut self,
        uri: &Uri,
        name: &str,
        columns: &[Column],
        row_count: usize,
    ) -> Result<(), String> {
        let (conn, _) = self.resolve(uri)?;
        let cols = match uri {
            Uri::Memory { table } => {
                if table.is_empty() {
                    name.to_string()
                } else {
                    table.clone()
                }
            }
            Uri::DuckdbFile { table, .. } => {
                if table.is_empty() {
                    name.to_string()
                } else {
                    table.clone()
                }
            }
            Uri::File { .. } => return Err("store write requires a duckdb target".to_string()),
        };
        create_and_append(conn, &cols, columns, row_count)
    }

    /// Read a named table's rows as JSON array bytes (for `memory://`/`duckdb://`
    /// *sources*). Each row becomes a JSON object keyed by column name.
    pub fn read_table_json(&mut self, uri: &Uri, name: &str) -> Result<Vec<u8>, String> {
        let (conn, table) = self.resolve(uri)?;
        let t = match uri {
            Uri::Memory { table } if table.is_empty() => quote_ident(name),
            Uri::DuckdbFile { table, .. } if table.is_empty() => quote_ident(name),
            _ => table.clone(),
        };
        let sql = format!("SELECT * FROM {t}");
        let mut stmt = conn.prepare(&sql).map_err(|e| format!("prepare: {e}"))?;
        let names: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|c| c.to_string())
            .collect();
        let mut rows = stmt.query([]).map_err(|e| format!("query: {e}"))?;
        let mut out = String::from("[");
        let mut first = true;
        while let Some(row) = rows.next().map_err(|e| format!("row: {e}"))? {
            if !first {
                out.push(',');
            }
            first = false;
            out.push('{');
            for (i, n) in names.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push('"');
                out.push_str(&escape_json(n));
                out.push('"');
                out.push(':');
                out.push_str(&cell_json(row, i)?);
            }
            out.push('}');
        }
        out.push(']');
        Ok(out.into_bytes())
    }

    /// Runs the LEFT JOIN producing a set of right-side columns (as cells) aligned
    /// one-per-row with the left (self) table. `self_cols` are written into a temp
    /// table named `self_table`; the join key is `self_key` (a column in that temp
    /// table).
    pub fn left_join(
        &mut self,
        self_table: &str,
        self_key: &str,
        right_uri: &Uri,
        right_key: &str,
        right_cols: &[String],
        right_kinds: &[ColKind],
        order_col: &str,
    ) -> Result<Vec<Vec<crate::columnar::Cell>>, String> {
        let (conn, right_table) = self.resolve(right_uri)?;
        let st = quote_ident(self_table);
        let lk = quote_ident(self_key);
        let rk = quote_ident(right_key);
        let oc = quote_ident(order_col);
        let sel: Vec<String> = right_cols
            .iter()
            .map(|c| format!("r.{}", quote_ident(c)))
            .collect();
        let sql = format!(
            "SELECT {} FROM {st} AS l LEFT JOIN {right_table} AS r ON l.{lk} = r.{rk} ORDER BY l.{oc}",
            sel.join(", ")
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| format!("prepare join: {e}"))?;
        let mut rows = stmt.query([]).map_err(|e| format!("query: {e}"))?;
        let mut result: Vec<Vec<crate::columnar::Cell>> = Vec::new();
        while let Some(row) = rows.next().map_err(|e| format!("row: {e}"))? {
            let mut cells: Vec<crate::columnar::Cell> = Vec::with_capacity(right_cols.len());
            for (i, kind) in right_kinds.iter().enumerate() {
                cells.push(get_cell(&row, i, *kind)?);
            }
            result.push(cells);
        }
        Ok(result)
    }

    /// Drop a temporary table (cleanup).
    pub fn drop_table(&mut self, name: &str, uri: &Uri) -> Result<(), String> {
        let (conn, _) = self.resolve(uri)?;
        let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {}", quote_ident(name)));
        Ok(())
    }
}

fn get_cell(row: &duckdb::Row<'_>, idx: usize, kind: ColKind) -> Result<crate::columnar::Cell, String> {
    use crate::columnar::Cell;
    Ok(match kind {
        ColKind::Str => row.get::<_, Option<String>>(idx).map_err(|e| format!("str: {e}"))?.map_or(Cell::Null, Cell::Str),
        ColKind::F64 => row.get::<_, Option<f64>>(idx).map_err(|e| format!("f64: {e}"))?.map_or(Cell::Null, Cell::F64),
        ColKind::I64 => row.get::<_, Option<i64>>(idx).map_err(|e| format!("i64: {e}"))?.map_or(Cell::Null, Cell::I64),
        ColKind::Bool => row.get::<_, Option<bool>>(idx).map_err(|e| format!("bool: {e}"))?.map_or(Cell::Null, Cell::Bool),
    })
}

fn cell_json(row: &duckdb::Row<'_>, idx: usize) -> Result<String, String> {
    use duckdb::types::ValueRef;
    match row.get_ref(idx).map_err(|e| format!("get_ref: {e}"))? {
        ValueRef::Null => Ok("null".to_string()),
        ValueRef::Boolean(b) => Ok(if b { "true" } else { "false" }.to_string()),
        ValueRef::TinyInt(v) => Ok(v.to_string()),
        ValueRef::SmallInt(v) => Ok(v.to_string()),
        ValueRef::Int(v) => Ok(v.to_string()),
        ValueRef::BigInt(v) => Ok(v.to_string()),
        ValueRef::UTinyInt(v) => Ok(v.to_string()),
        ValueRef::USmallInt(v) => Ok(v.to_string()),
        ValueRef::UInt(v) => Ok(v.to_string()),
        ValueRef::UBigInt(v) => Ok(v.to_string()),
        ValueRef::Float(v) => Ok(format!("{}", v)),
        ValueRef::Double(v) => Ok(format!("{}", v)),
        ValueRef::Decimal(_) => Ok(format!("{}", row.get::<_, f64>(idx).unwrap_or(0.0))),
        ValueRef::Text(v) => {
            let s = String::from_utf8_lossy(v);
            Ok(format!("\"{}\"", escape_json(&s)))
        }
        ValueRef::Blob(v) => Ok(format!("\"{}\"", escape_json(&String::from_utf8_lossy(v)))),
        ValueRef::Date32(v) => Ok(format!("{}", v)),
        ValueRef::Time64(_, v) => Ok(format!("{}", v)),
        ValueRef::Timestamp(_, v) => Ok(format!("{}", v)),
        ValueRef::Interval { .. } => {
            Ok(format!("\"{}\"", escape_json(&row.get::<_, String>(idx).unwrap_or_default())))
        }
        _ => Ok("null".to_string()),
    }
}

pub(crate) fn escape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn duck_type(kind: ColKind) -> &'static str {
    match kind {
        ColKind::Str => "TEXT",
        ColKind::F64 => "DOUBLE",
        ColKind::I64 => "BIGINT",
        ColKind::Bool => "BOOLEAN",
    }
}

/// CREATE TABLE (if absent) and append all rows via the appender.
fn create_and_append(
    conn: &Connection,
    table: &str,
    columns: &[Column],
    row_count: usize,
) -> Result<(), String> {
    let col_defs: Vec<String> = columns
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.name), duck_type(c.kind)))
        .collect();
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {table} ({})",
        col_defs.join(", ")
    );
    conn.execute_batch(&ddl).map_err(|e| format!("create table: {e}"))?;

    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    let mut app = conn
        .appender_with_columns(table, &names)
        .map_err(|e| format!("appender: {e}"))?;

    for i in 0..row_count {
        let mut row: Vec<DbValue> = Vec::with_capacity(columns.len());
        for c in columns {
            row.push(cell_to_db(c, i));
        }
        app.append_row(duckdb::appender_params_from_iter(row.iter()))
            .map_err(|e| format!("append row: {e}"))?;
    }
    app.flush().map_err(|e| format!("append flush: {e}"))?;
    Ok(())
}

fn cell_to_db(c: &Column, i: usize) -> DbValue {
    match c.kind {
        ColKind::Str => match c.strings.get(i).cloned().flatten() {
            None => DbValue::Null,
            Some(s) => DbValue::Text(s),
        },
        ColKind::F64 => match c.f64s.get(i).cloned().flatten() {
            None => DbValue::Null,
            Some(v) => DbValue::Double(v),
        },
        ColKind::I64 => match c.i64s.get(i).cloned().flatten() {
            None => DbValue::Null,
            Some(v) => DbValue::BigInt(v),
        },
        ColKind::Bool => match c.bools.get(i).cloned().flatten() {
            None => DbValue::Null,
            Some(v) => DbValue::Boolean(v),
        },
    }
}
