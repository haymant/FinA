use std::time::Instant;
use arrow_array::ArrayRef;
use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
use crate::columnar::{Cell, ColKind, Column, write_partitioned};
use crate::config::{Dataset, Field, Join, PipelinesConfig, Source};
use crate::lazy::{compile, eval_condition_lazy, eval_node, expr_kind, joined_projection, resolve_path, Ctx, Node};
use crate::native::NValue;
use crate::sonic::Sonic;
use crate::store::{SharedDb, SharedStore, Uri};
use sonic_rs::JsonValueTrait;

/// Per-dataset run summary for reporting.
pub struct LazyRunResult {
    pub timing: Vec<(String, f64)>,
    pub datasets: Vec<(String, usize)>,
}

/// Runs the full set of pipelines inline (shared in-memory store, serial).
pub fn run_pipelines(cfg: &PipelinesConfig) -> Result<LazyRunResult, String> {
    let mut db = SharedDb::new()?;
    let mut timing = Vec::new();
    let mut datasets = Vec::new();
    for pipe in &cfg.pipelines {
        run_one_pipeline(&mut db, pipe, None, &mut timing, &mut datasets)?;
    }
    Ok(LazyRunResult { timing, datasets })
}

/// Runs a single named pipeline in its own store, optionally with a per-task
/// context exposed to fields/filters via `$task.*` (used by the scheduler's
/// fan-out workers, see docs/scheduler.md).
#[allow(dead_code)]
pub fn run_pipeline_task(
    cfg: &PipelinesConfig,
    name: &str,
    task: Option<&sonic_rs::Value>,
) -> Result<LazyRunResult, String> {
    run_pipeline_task_shared(cfg, name, task, None)
}

/// Like [`run_pipeline_task`], but shares one in-memory duckdb backend across
/// every task of a scheduled run (see [`crate::store::SharedStore`]). This is
/// what makes `memory://` tables written by an earlier stage/task visible to
/// later ones.
pub fn run_pipeline_task_shared(
    cfg: &PipelinesConfig,
    name: &str,
    task: Option<&sonic_rs::Value>,
    store: Option<&SharedStore>,
) -> Result<LazyRunResult, String> {
    let pipe = cfg
        .pipelines
        .iter()
        .find(|p| p.name.as_deref().unwrap_or("unnamed") == name)
        .ok_or_else(|| format!("no pipeline named '{name}'"))?;
    // fan-out workers get worker-unique output targets: rewrite `{task}` (the
    // scheduler task id), `{index}` / `{of}` (the partition ordinal) anywhere
    // they appear in a dataset `to.uri`. Without a task (plain run) the
    // placeholders collapse to a single shared target.
    let mut pipe = pipe.clone();
    if task.is_some() {
        for ds in pipe.datasets.iter_mut() {
            ds.to.uri = task_uri(&ds.to.uri, task.expect("checked above"));
        }
    }
    let mut timing = Vec::new();
    let mut datasets = Vec::new();
    let task_son = task.map(|v| Sonic(v.clone()));
    match store {
        Some(store) => store.with(|db| {
            run_one_pipeline(db, &pipe, task_son.as_ref(), &mut timing, &mut datasets)
        })?,
        None => {
            let mut db = SharedDb::new()?;
            run_one_pipeline(&mut db, &pipe, task_son.as_ref(), &mut timing, &mut datasets)?;
        }
    }
    Ok(LazyRunResult { timing, datasets })
}

/// Substitute fan-out placeholders in an output URI from the task value
/// (`{"id":..., "ctx":{"partition":{"index":...,"of":...}}}`). Unknown
/// placeholders fall back to their literal text.
fn task_uri(uri: &str, task: &sonic_rs::Value) -> String {
    if !uri.contains('{') {
        return uri.to_string();
    }
    let id = task
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("task");
    let safe: String = id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    let idx = task
        .get("ctx")
        .and_then(|c| c.get("partition"))
        .and_then(|p| p.get("index"))
        .and_then(|v| v.as_i64())
        .map(|i| i.to_string())
        .unwrap_or_default();
    let of = task
        .get("ctx")
        .and_then(|c| c.get("partition"))
        .and_then(|p| p.get("of"))
        .and_then(|v| v.as_i64())
        .map(|i| i.to_string())
        .unwrap_or_default();
    uri.replace("{task}", &safe)
        .replace("{index}", &idx)
        .replace("{of}", &of)
}

fn run_one_pipeline(
    db: &mut SharedDb,
    pipe: &crate::config::Pipeline,
    task: Option<&Sonic>,
    timing: &mut Vec<(String, f64)>,
    datasets: &mut Vec<(String, usize)>,
) -> Result<(), String> {
    let pname = pipe.name.as_deref().unwrap_or("unnamed");
    let pt = Instant::now();

    // resolve each declared source to its JSON bytes (once per pipeline)
    let mut source_bytes: Vec<(String, Vec<u8>)> = Vec::new();
    for src in &pipe.sources {
        let bytes = read_source(db, src)?;
        source_bytes.push((src.name.clone(), bytes));
    }

    // default source for a dataset: its `source` name, else the first source
    let default_source = |ds: &Dataset| -> Result<String, String> {
        if ds.source.is_empty() {
            source_bytes
                .first()
                .map(|(n, _)| n.clone())
                .ok_or_else(|| "no source defined".to_string())
        } else {
            Ok(ds.source.clone())
        }
    };

    // group datasets by their default source so one streaming pass covers
    // every dataset that shares the same input bytes (the input is parsed
    // once per group instead of once per dataset).
    let mut groups: Vec<(String, Vec<&Dataset>)> = Vec::new();
    for ds in &pipe.datasets {
        let src = default_source(ds)?;
        match groups.iter_mut().find(|(s, _)| *s == src) {
            Some((_, v)) => v.push(ds),
            None => groups.push((src, vec![ds])),
        }
    }

    for (src_name, dss) in groups {
        let input = source_bytes
            .iter()
            .find(|(n, _)| *n == src_name)
            .map(|(_, b)| b)
            .ok_or_else(|| format!("dataset references unknown source '{src_name}'"))?;

        let mut accs: Vec<DatasetAcc> = dss.iter().copied().map(|ds| DatasetAcc::new(ds, task)).collect();
        let scan_start = Instant::now();
        scan_group(&mut accs, input, task);
        let scan_ms = scan_start.elapsed().as_secs_f64() * 1000.0;
        let share = scan_ms / accs.len() as f64;

        for acc in &mut accs {
            let ds_start = Instant::now();
            let rows = assemble_and_write(db, acc)?;
            let own_ms = ds_start.elapsed().as_secs_f64() * 1000.0;
            timing.push((
                format!("pipeline '{pname}' dataset '{}'", acc.ds.name),
                share + own_ms,
            ));
            datasets.push((acc.ds.name.clone(), rows));
        }
    }

    timing.push((
        format!("pipeline '{pname}' total"),
        pt.elapsed().as_secs_f64() * 1000.0,
    ));
    Ok(())
}

/// Field projection: either evaluated against the default JSON source, or read
/// from a materialized join column.
enum Proj {
    Local(String),
    Join { col: String, kind: ColKind },
}

/// Yields each top-level `{...}` record's byte slice in a JSON array document,
/// without materializing the whole DOM.
pub struct RecordScanner<'a> {
    bytes: &'a [u8],
    i: usize,
    done: bool,
}

impl<'a> RecordScanner<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        RecordScanner { bytes, i: 0, done: false }
    }
}

impl<'a> Iterator for RecordScanner<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.done {
            return None;
        }
        let b = self.bytes;
        let n = b.len();
        while self.i < n && b[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
        if self.i >= n {
            self.done = true;
            return None;
        }
        if b[self.i] == b'[' {
            self.i += 1;
        }
        while self.i < n && (b[self.i].is_ascii_whitespace() || b[self.i] == b',') {
            self.i += 1;
        }
        if self.i >= n || b[self.i] == b']' {
            self.done = true;
            return None;
        }
        let start = self.i;
        let mut depth: isize = 0;
        let mut in_str = false;
        let mut esc = false;
        while self.i < n {
            let c = b[self.i];
            if in_str {
                if esc {
                    esc = false;
                } else if c == b'\\' {
                    esc = true;
                } else if c == b'"' {
                    in_str = false;
                }
            } else {
                match c {
                    b'"' => in_str = true,
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth <= 0 {
                            self.i += 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            self.i += 1;
        }
        Some(&b[start..self.i])
    }
}

/// Parses a compiled field expr into a `Proj`.
fn classify(f: &Field, join: Option<&Join>) -> Proj {
    if let Some(j) = join {
        if let Some((col, kind)) = joined_projection(&j.alias, &f.expression) {
            return Proj::Join { col, kind };
        }
    }
    Proj::Local(f.expression.clone())
}

struct JoinInfo {
    alias: String,
    right_uri: Uri,
    right_key: String,
    right_cols: Vec<String>,
    right_kinds: Vec<ColKind>,
    left_key_node: Node,
    left_key_kind: ColKind,
}

pub(crate) fn read_source(db: &mut SharedDb, src: &Source) -> Result<Vec<u8>, String> {
    let uri = Uri::parse(&src.uri);
    match uri {
        Uri::File { path } => {
            let fmt = src.format.as_deref().unwrap_or("json");
            let raw = std::fs::read(&path).map_err(|e| format!("read source {}: {e}", path))?;
            match fmt {
                "parquet" => parquet_to_json(&path),
                "json" => match &src.json_path {
                    Some(jp) if !jp.is_empty() => extract_json_path(&raw, jp),
                    _ => Ok(raw),
                },
                other => Err(format!("unsupported source format '{other}' for {}", path)),
            }
        }
        other => db.read_table_json(&other, &src.name),
    }
}

/// Reads a parquet file and re-serializes its rows as a JSON array (used when a
/// `file://` source declares `format: parquet`).
fn parquet_to_json(path: &str) -> Result<Vec<u8>, String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(path).map_err(|e| format!("open parquet {path}: {e}"))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| format!("parquet meta {path}: {e}"))?;
    let names: Vec<String> = builder.schema().fields().iter().map(|f| f.name().clone()).collect();
    let mut reader = builder.build().map_err(|e| format!("parquet read {path}: {e}"))?;
    let mut out = String::from("[");
    let mut first = true;
    for batch in &mut reader {
        let batch = batch.map_err(|e| format!("parquet batch: {e}"))?;
        for r in 0..batch.num_rows() {
            if !first {
                out.push(',');
            }
            first = false;
            out.push('{');
            for (c, name) in names.iter().enumerate() {
                if c > 0 {
                    out.push(',');
                }
                out.push('"');
                out.push_str(&crate::store::escape_json(name));
                out.push('"');
                out.push(':');
                out.push_str(&arrow_cell_json(batch.column(c), r));
            }
            out.push('}');
        }
    }
    out.push(']');
    Ok(out.into_bytes())
}

fn arrow_cell_json(arr: &ArrayRef, row: usize) -> String {
    if arr.is_null(row) {
        return "null".to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return format!("\"{}\"", crate::store::escape_json(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return if a.value(row) { "true" } else { "false" }.to_string();
    }
    "null".to_string()
}

/// Navigates a JSON document to `path` (e.g. `$.underlyings`) and returns that
/// subtree re-serialized as bytes.
fn extract_json_path(bytes: &[u8], path: &str) -> Result<Vec<u8>, String> {
    let v: sonic_rs::Value = sonic_rs::from_slice(bytes).map_err(|e| format!("parse json: {e}"))?;
    let mut cur = v;
    for seg in path.trim().trim_start_matches('$').trim_start_matches('.').split('.') {
        if seg.is_empty() {
            continue;
        }
        cur = cur.get(seg).cloned().ok_or_else(|| format!("json_path: missing '{seg}'"))?;
    }
    sonic_rs::to_vec(&cur).map_err(|e| format!("serialize json_path: {e}"))
}

/// Per-dataset extraction state. Multiple `DatasetAcc`s that share one source
/// are scanned together in a single streaming pass: each record is parsed once
/// and evaluated against every dataset in the group.
struct DatasetAcc<'a> {
    ds: &'a Dataset,
    projs: Vec<Proj>,
    kinds: Vec<ColKind>,
    local_nodes: Vec<Node>,
    local_out_idx: Vec<usize>,
    self_cols: Vec<Column>,
    lk_col: Option<Column>,
    join: Option<JoinInfo>,
    /// Optional per-row filter (`Dataset.filter`), e.g.
    /// `$.name IN $task.ctx.units` for fan-out workers.
    filter: Option<Node>,
    /// Whether this dataset assembles a new JSON root per row (see
    /// `Dataset.row_root`). When set, field evaluation happens after the join
    /// materializes (in `assemble_and_write`), so during scanning we only
    /// capture the raw source record and the unwound element.
    row_root: bool,
    /// Compiled field nodes for row_root assembly (every field, in projs order).
    root_nodes: Vec<Node>,
    /// Raw source record JSON per output row (row_root datasets only).
    raw_rows: Vec<String>,
    /// Unwound element JSON per output row: the rule `output_alias` + the raw
    /// element text (row_root datasets with unwind rules only).
    raw_elems: Vec<Option<(String, String)>>,
    /// Per-task context (`$task`) so row_root field expressions may still
    /// address `$task.*` at assemble time.
    task_ctx: Option<Sonic>,
}

impl<'a> DatasetAcc<'a> {
    fn new(ds: &'a Dataset, task: Option<&Sonic>) -> Self {
        // classify projections
        let projs: Vec<Proj> = ds.fields.iter().map(|f| classify(f, ds.join.as_ref())).collect();
        let kinds: Vec<ColKind> = projs
            .iter()
            .map(|p| match p {
                Proj::Join { kind, .. } => *kind,
                Proj::Local(e) => expr_kind(e),
            })
            .collect();

        // compile local nodes (excluding join projections) in local order
        let mut local_nodes: Vec<Node> = Vec::new();
        let mut local_out_idx: Vec<usize> = Vec::new(); // output column indices that are local
        for (idx, p) in projs.iter().enumerate() {
            if let Proj::Local(expr) = p {
                local_nodes.push(compile(expr));
                local_out_idx.push(idx);
            }
        }

        // join key: compile left_key as a local expr producing a string column
        let join = match &ds.join {
            None => None,
            Some(j) => {
                let right_uri = Uri::parse(&j.target);
                let right_cols = j.columns.clone();
                let right_kinds: Vec<ColKind> = right_cols
                    .iter()
                    .map(|c| {
                        for p in projs.iter() {
                            if let Proj::Join { col, kind } = p {
                                if col == c {
                                    return *kind;
                                }
                            }
                        }
                        ColKind::Str
                    })
                    .collect();
                let key_node = compile(&j.left_key);
                Some(JoinInfo {
                    alias: j.alias.clone(),
                    right_uri,
                    right_key: j.right_key.clone(),
                    right_cols,
                    right_kinds,
                    left_key_node: key_node,
                    left_key_kind: ColKind::Str,
                })
            }
        };

        let self_cols: Vec<Column> = local_out_idx
            .iter()
            .map(|&o| Column::new(&ds.fields[o].name, kinds[o]))
            .collect();
        let lk_col: Option<Column> = join.as_ref().map(|_| Column::new("__lk0", ColKind::Str));
        let filter = ds.filter.as_deref().map(compile);
        // row_root datasets: fields are evaluated against the assembled root in
        // proj order, so keep a compiled node per field for the assemble step
        // and skip streaming projection entirely.
        let root_nodes = ds.fields.iter().map(|f| compile(&f.expression)).collect();

        DatasetAcc {
            ds,
            projs,
            kinds,
            local_nodes: if ds.row_root.is_some() { Vec::new() } else { local_nodes },
            local_out_idx,
            self_cols,
            lk_col,
            join,
            filter,
            row_root: ds.row_root.is_some(),
            root_nodes,
            raw_rows: Vec::new(),
            raw_elems: Vec::new(),
            task_ctx: task.map(|t| Sonic(t.0.clone())),
        }
    }

    /// Evaluate one record (or one unwound element) against this dataset.
    fn eval(&mut self, ctx: Ctx<'_, Sonic>) {
        if self.row_root {
            // capture what the assembled root needs; field eval happens at
            // assemble time (after the join materializes).
            self.raw_rows.push(ctx.raw.to_string());
            if let Some(elem) = ctx.elem {
                let alias = ctx.alias.unwrap_or("").to_string();
                let raw = elem.raw_text().unwrap_or_default();
                self.raw_elems.push(Some((alias, raw)));
            } else {
                self.raw_elems.push(None);
            }
            if let (Some(col), Some(j)) = (self.lk_col.as_mut(), &self.join) {
                let lc = eval_node(&j.left_key_node, &ctx).to_cell(j.left_key_kind);
                col.push(Some(lc));
            }
            return;
        }
        for (ci, node) in self.local_nodes.iter().enumerate() {
            let cell = eval_node(node, &ctx).to_cell(self.kinds[self.local_out_idx[ci]]);
            self.self_cols[ci].push(Some(cell));
        }
        if let (Some(col), Some(j)) = (self.lk_col.as_mut(), &self.join) {
            let lc = eval_node(&j.left_key_node, &ctx).to_cell(j.left_key_kind);
            col.push(Some(lc));
        }
    }

    /// Whether this output row passes the dataset filter (None = pass all).
    fn keep(&self, ctx: Ctx<'_, Sonic>) -> bool {
        match &self.filter {
            None => true,
            Some(n) => truthy(&eval_node(n, &ctx)),
        }
    }
}

/// Bare truthiness for filter evaluation.
fn truthy(v: &crate::lazy::LazyVal) -> bool {
    use crate::lazy::LazyVal;
    match v {
        LazyVal::Bool(b) => *b,
        LazyVal::Null => false,
        _ => true,
    }
}

/// One streaming pass over `input`, evaluating each record against every dataset
/// in `accs`. Datasets are independent accumulators sharing the parse. `task` is
/// the per-task context exposed to `$task.*` expressions (None for plain runs).
fn scan_group<'a>(accs: &mut [DatasetAcc<'a>], input: &'a [u8], task: Option<&'a Sonic>) {
    let scanner = RecordScanner::new(input);
    for slice in scanner {
        let s = std::str::from_utf8(slice).unwrap_or("");
        let Ok(v) = sonic_rs::from_str::<sonic_rs::Value>(s).map(Sonic) else { continue };
        if v.is_null() {
            continue;
        }
        for acc in accs.iter_mut() {
            if acc.ds.dataset_type == "unwound" {
                for k in 0..acc.ds.unwind_rules.len() {
                    let rule = &acc.ds.unwind_rules[k];
                    if !eval_condition_lazy(&rule.condition, Ctx::report_task(&v, s, task)) {
                        continue;
                    }
                    let Some(arr) = resolve_path(Ctx::report_task(&v, s, task), &rule.unwind_path)
                    else {
                        continue;
                    };
                    let Some(arr_len) = arr.array_len() else { continue };
                    for i in 0..arr_len {
                        let Some(elem) = arr.get_idx(i) else { continue };
                        let alias = if rule.output_alias.is_empty() {
                            rule.name.clone()
                        } else {
                            rule.output_alias.clone()
                        };
                        let uctx = Ctx {
                            base: &v,
                            raw: s,
                            elem: Some(elem),
                            alias: Some(&alias),
                            task,
                        };
                        if acc.keep(uctx) {
                            acc.eval(uctx);
                        }
                    }
                }
            } else {
                let rctx = Ctx::report_task(&v, s, task);
                if acc.keep(rctx) {
                    acc.eval(rctx);
                }
            }
        }
    }
}

/// Materializes the join (if any), assembles the output columns in field order,
/// and writes the dataset to its target. Returns the row count.
fn assemble_and_write(db: &mut SharedDb, acc: &mut DatasetAcc<'_>) -> Result<usize, String> {
    if acc.row_root {
        return assemble_row_root(db, acc);
    }
    let ds = acc.ds;
    let row_count = if acc.join.is_some() {
        acc.lk_col.as_ref().map(|c| c.len()).unwrap_or(0)
    } else {
        acc.self_cols.first().map(|c| c.len()).unwrap_or(0)
    };

    let joined_cells: Option<Vec<Vec<Cell>>> = match &acc.join {
        Some(j) if row_count > 0 => {
            // temp table: self_cols + __lk0 + __rid
            let mut tmp_cols: Vec<Column> = acc.self_cols.clone();
            if let Some(lc) = &acc.lk_col {
                tmp_cols.push(lc.clone());
            }
            let mut rid = Column::new("__rid", ColKind::I64);
            for i in 0..row_count {
                rid.push(Some(Cell::I64(i as i64)));
            }
            tmp_cols.push(rid);
            let tmp_table = format!("__ds_{}", sanitize(&ds.name));
            let tmp_uri = Uri::Memory { table: tmp_table.clone() };
            db.write_columns(&tmp_uri, &tmp_table, &tmp_cols, row_count)?;
            let cells = db.left_join(
                &tmp_table,
                "__lk0",
                &j.right_uri,
                &j.right_key,
                &j.right_cols,
                &j.right_kinds,
                "__rid",
            )?;
            let mem = Uri::Memory { table: String::new() };
            db.drop_table(&tmp_table, &mem)?;
            Some(cells)
        }
        _ => None,
    };

    // build output columns in original field order. When there is no join every
    // field is a local projection in order, so the extracted columns already ARE
    // the output columns — take them to avoid a full second copy in memory.
    let out_cols: Vec<Column> = if acc.join.is_none() {
        std::mem::take(&mut acc.self_cols)
    } else {
        let mut out_cols: Vec<Column> = Vec::with_capacity(ds.fields.len());
        for (o, _) in acc.projs.iter().enumerate() {
            out_cols.push(Column::new(&ds.fields[o].name, acc.kinds[o]));
        }
        for r in 0..row_count {
            for (o, p) in acc.projs.iter().enumerate() {
                let cell = match p {
                    Proj::Local(_) => {
                        // find which local slot produced this output col
                        let li = acc.local_out_idx.iter().position(|x| *x == o).unwrap();
                        acc.self_cols[li].get_cell(r).unwrap_or(Cell::Null)
                    }
                    Proj::Join { col, .. } => {
                        let mut idx = 0usize;
                        if let Some(j) = &acc.join {
                            idx = j.right_cols.iter().position(|c| c == col).unwrap_or(0);
                        }
                        match &joined_cells {
                            Some(rows) => rows
                                .get(r)
                                .and_then(|v| v.get(idx).cloned())
                                .unwrap_or(Cell::Null),
                            None => Cell::Null,
                        }
                    }
                };
                out_cols[o].push(Some(cell));
            }
        }
        out_cols
    };

    // write to target
    write_target(db, &ds.to.uri, &ds.name, &out_cols, &ds.to.partition_by, &ds.to.format)?;

    Ok(row_count)
}

/// Assembles one new JSON root per output row
/// (`{"instrument": <raw record>, "leg": <unwound element>, "market": {joined...}}`,
/// whatever `Dataset.row_root` declares), parses it, then evaluates every field
/// expression against that root so extracted fields land in separate typed
/// columns. Rows amount = join key rows (row_root datasets always carry a join
/// key column, captured during scanning). No join -> one row per captured record.
fn assemble_row_root(db: &mut SharedDb, acc: &mut DatasetAcc<'_>) -> Result<usize, String> {
    let ds = acc.ds;
    let row_count = acc.raw_rows.len();
    let rr = ds.row_root.as_ref().expect("row_root dataset").clone();
    let join_alias = acc.join.as_ref().map(|j| j.alias.clone());

    // materialize the join row per output row (indexed by row)
    let joined_cells: Option<Vec<Vec<Cell>>> = match &acc.join {
        Some(j) if row_count > 0 => {
            let mut tmp_cols: Vec<Column> = Vec::new();
            if let Some(lc) = &acc.lk_col {
                tmp_cols.push(lc.clone());
            }
            let mut rid = Column::new("__rid", ColKind::I64);
            for i in 0..row_count {
                rid.push(Some(Cell::I64(i as i64)));
            }
            tmp_cols.push(rid);
            let tmp_table = format!("__ds_{}", sanitize(&ds.name));
            let tmp_uri = Uri::Memory { table: tmp_table.clone() };
            db.write_columns(&tmp_uri, &tmp_table, &tmp_cols, row_count)?;
            let cells = db.left_join(
                &tmp_table,
                "__lk0",
                &j.right_uri,
                &j.right_key,
                &j.right_cols,
                &j.right_kinds,
                "__rid",
            )?;
            let mem = Uri::Memory { table: String::new() };
            db.drop_table(&tmp_table, &mem)?;
            Some(cells)
        }
        _ => None,
    };

    // output columns, one per declared field
    let mut out_cols: Vec<Column> = ds
        .fields
        .iter()
        .zip(&acc.kinds)
        .map(|(f, k)| Column::new(&f.name, *k))
        .collect();
    let nodes = &acc.root_nodes;

    for r in 0..row_count {
        let record_raw = acc.raw_rows.get(r).cloned().unwrap_or_default();
        let elem = acc.raw_elems.get(r).cloned().flatten();

        let mut body = String::from("{");
        let pushy = |body: &mut String, key: &str, value: String, first: bool| {
            if !first {
                body.push(',');
            }
            body.push_str(&json_escape(key));
            body.push(':');
            body.push_str(&value);
        };
        let mut first = true;
        for (key, part) in &rr {
            let part = part.trim();
            let value = if part == "$" {
                record_raw.clone()
            } else if join_alias.as_deref() == Some(part) {
                let mut obj = String::from("{");
                let mut f2 = true;
                if let (Some(j), Some(rows)) = (&acc.join, &joined_cells) {
                    for (ci, col) in j.right_cols.iter().enumerate() {
                        if !f2 {
                            obj.push(',');
                        }
                        f2 = false;
                        obj.push_str(&json_escape(col));
                        obj.push(':');
                        let cell = rows
                            .get(r)
                            .and_then(|row| row.get(ci))
                            .cloned()
                            .unwrap_or(Cell::Null);
                        obj.push_str(&cell_to_json(&cell));
                    }
                }
                obj.push('}');
                obj
            } else if elem.as_ref().is_some_and(|(a, _)| a == part) {
                elem.as_ref().map(|(_, t)| t.clone()).unwrap_or_default()
            } else {
                return Err(format!(
                    "row_root child '{key}' references unknown source '{part}' \
                     (expected '$', a join alias, or an unwind rule alias)"
                ));
            };
            pushy(&mut body, key, value, first);
            first = false;
        }
        body.push('}');

        let v: sonic_rs::Value = sonic_rs::from_str(&body)
            .map_err(|e| format!("assemble row_root for '{}': {e}", ds.name))?;
        let sv = Sonic(v);
        let ctx = match &acc.task_ctx {
            Some(t) => Ctx::report_task(&sv, &body, Some(t)),
            None => Ctx::report_task(&sv, &body, None),
        };
        for (ci, node) in nodes.iter().enumerate() {
            let cell = eval_node(node, &ctx).to_cell(acc.kinds[ci]);
            out_cols[ci].push(Some(cell));
        }
    }

    write_target(db, &ds.to.uri, &ds.name, &out_cols, &ds.to.partition_by, &ds.to.format)?;
    Ok(row_count)
}

/// JSON-encode a `Cell` value (object members of an assembled row_root).
fn cell_to_json(c: &Cell) -> String {
    match c {
        Cell::Null => "null".to_string(),
        Cell::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Cell::I64(i) => i.to_string(),
        Cell::F64(f) => f.to_string(),
        Cell::Str(s) => json_escape(s),
    }
}

/// JSON string literal escaping (subset sufficient for object keys/values).
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
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
    out.push('"');
    out
}

fn write_target(
    db: &mut SharedDb,
    uri: &str,
    name: &str,
    columns: &[Column],
    partition_by: &[String],
    format: &str,
) -> Result<(), String> {
    let u = Uri::parse(uri);
    match &u {
        Uri::File { path } => {
            if !format.is_empty() && format != "parquet" {
                return Err(format!("unsupported output format '{format}' for file target {path}"));
            }
            let p = if path.ends_with(".parquet") {
                path.clone()
            } else if partition_by.is_empty() {
                format!("{}/{}.parquet", path, name)
            } else {
                path.to_string()
            };
            if let Some(parent) = std::path::Path::new(&p).parent() {
                std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
            }
            write_partitioned(&p, columns, partition_by)
        }
        Uri::Memory { table } => {
            let t = if table.is_empty() { name.to_string() } else { table.clone() };
            let row_count = columns.first().map(|c| c.len()).unwrap_or(0);
            let table_uri = Uri::Memory { table: t };
            db.write_columns(&table_uri, name, columns, row_count)
        }
        Uri::DuckdbFile { path, table } => {
            let t = if table.is_empty() { name.to_string() } else { table.clone() };
            let row_count = columns.first().map(|c| c.len()).unwrap_or(0);
            let table_uri = Uri::DuckdbFile { path: path.clone(), table: t };
            db.write_columns(&table_uri, name, columns, row_count)
        }
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}
