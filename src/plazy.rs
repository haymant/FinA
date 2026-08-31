use std::time::Instant;
use crate::columnar::ParquetSink;
use crate::config::Config;
use crate::lazy::{compile, eval_condition_lazy, eval_node, expr_kind, resolve_path, Ctx};
use crate::native::NValue;

pub struct LazyRunResult {
    pub timing: Vec<(String, f64)>,
    pub datasets: Vec<(String, usize)>,
}

/// Yields each top-level `{...}` record's byte slice in a JSON array document,
/// without materializing the whole DOM. Handles nested arrays/objects/strings.
/// If the document is a single object (not an array), it is yielded once.
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

struct DsRun {
    name: String,
    is_unwound: bool,
    unwind_rules_cond: Vec<String>,
    unwind_paths: Vec<String>,
    unwind_aliases: Vec<String>,
    nodes: Vec<crate::lazy::Node>,
    cols: Vec<crate::columnar::Column>,
    sink: ParquetSink,
    rows: usize,
    total_rows: usize,
    extract_time: f64,
    write_time: f64,
}

/// Runs the whole ETL in a streaming fashion over `input`, parsing each record
/// with backend `parse` (which must yield an owned native `V`), evaluating the
/// YAML field expressions natively against `V`, and streaming columnar output.
pub fn run_lazy_stream<V: NValue>(
    cfg: &Config,
    input: &[u8],
    out_dir: &str,
    mut parse: impl FnMut(&[u8]) -> Result<V, ()>,
) -> Result<LazyRunResult, String> {
    std::fs::create_dir_all(out_dir).map_err(|e| format!("mkdir {out_dir}: {e}"))?;

    let mut runs: Vec<DsRun> = Vec::new();
    for ds in &cfg.datasets {
        let fields: Vec<(String, crate::columnar::ColKind)> = ds
            .fields
            .iter()
            .map(|f| (f.name.clone(), expr_kind(&f.expression)))
            .collect();
        let nodes: Vec<crate::lazy::Node> =
            ds.fields.iter().map(|f| compile(&f.expression)).collect();
        let cols: Vec<crate::columnar::Column> = fields
            .iter()
            .map(|(n, k)| crate::columnar::Column::new(n, *k))
            .collect();
        let out = format!("{}/{}.parquet", out_dir, ds.name);
        let sink = crate::columnar::ParquetSink::new(&out, &fields)?;
        runs.push(DsRun {
            name: ds.name.clone(),
            is_unwound: ds.dataset_type == "unwound",
            unwind_rules_cond: ds.unwind_rules.iter().map(|r| r.condition.clone()).collect(),
            unwind_paths: ds.unwind_rules.iter().map(|r| r.unwind_path.clone()).collect(),
            unwind_aliases: ds.unwind_rules.iter().map(|r| r.output_alias.clone()).collect(),
            nodes,
            cols,
            sink,
            rows: 0,
            total_rows: 0,
            extract_time: 0.0,
            write_time: 0.0,
        });
    }

    const FLUSH: usize = 4096;
    let _ = std::str::from_utf8(input).map_err(|e| format!("input not utf8: {e}"))?;
    let scanner = RecordScanner::new(input);
    let mut parse_ms = 0.0;
    for slice in scanner {
        let s = std::str::from_utf8(slice).unwrap_or("");
        let pt = Instant::now();
        let parsed = parse(slice);
        parse_ms += pt.elapsed().as_secs_f64() * 1000.0;
        let Ok(v) = parsed else { continue };
        if v.is_null() {
            continue;
        }
        for r in runs.iter_mut() {
            let et = Instant::now();
            if r.is_unwound {
                let mut emitted = 0usize;
                for k in 0..r.unwind_rules_cond.len() {
                    if !eval_condition_lazy(&r.unwind_rules_cond[k], Ctx::report(&v, s)) {
                        continue;
                    }
                    let Some(arr) = resolve_path(Ctx::report(&v, s), &r.unwind_paths[k]) else {
                        continue;
                    };
                    let Some(arr_len) = arr.array_len() else { continue };
                    for i in 0..arr_len {
                        let Some(elem) = arr.get_idx(i) else { continue };
                        let ctx = Ctx::unwound(&v, s, elem, &r.unwind_aliases[k]);
                        for ci in 0..r.nodes.len() {
                            let kind = r.cols[ci].kind;
                            let cell = eval_node(&r.nodes[ci], &ctx).to_cell(kind);
                            r.cols[ci].push(Some(cell));
                        }
                        emitted += 1;
                    }
                }
                r.rows += emitted;
                r.total_rows += emitted;
            } else {
                let ctx = Ctx::report(&v, s);
                for ci in 0..r.nodes.len() {
                    let kind = r.cols[ci].kind;
                    let cell = eval_node(&r.nodes[ci], &ctx).to_cell(kind);
                    r.cols[ci].push(Some(cell));
                }
                r.rows += 1;
                r.total_rows += 1;
            }
            r.extract_time += et.elapsed().as_secs_f64() * 1000.0;
            if r.rows >= FLUSH {
                let wt = Instant::now();
                r.sink.write_chunk(&mut r.cols)?;
                r.write_time += wt.elapsed().as_secs_f64() * 1000.0;
                r.rows = 0;
            }
        }
    }

    // Flush remaining rows and close sinks.
    let mut timing = vec![("json parsing (streamed)".to_string(), parse_ms)];
    let mut datasets = Vec::new();
    for mut r in runs {
        if r.rows > 0 {
            let wt = Instant::now();
            r.sink.write_chunk(&mut r.cols)?;
            r.write_time += wt.elapsed().as_secs_f64() * 1000.0;
        }
        r.sink.close()?;
        datasets.push((r.name.clone(), r.total_rows));
        timing.push((format!("dataset '{}' (lazy extract)", r.name), r.extract_time));
        timing.push((format!("parquet write '{}'", r.name), r.write_time));
    }

    Ok(LazyRunResult { timing, datasets })
}
