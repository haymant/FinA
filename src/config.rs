use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// Official FinA ETL schema (see docs/schema.md).
//
// The root document declares a list of *pipelines*, each of which declares any
// number of named *sources* (inputs) and *datasets* (outputs). Datasets may be
// pointed at individual stores (file / duckdb in-memory / duckdb file) via a URI
// and may join against previously produced tables.
//
// An optional `execution` block controls how the scheduler fans the pipelines
// out (see docs/scheduler.md): a list of *stages*, each a *group* of nodes that
// run in parallel; stages run serially. Nodes are either a pipeline name (one
// task) or a fan-out node `{pipeline, partition: "partition(src.field, N)"}`
// that splits the record universe into N chunks and schedules one task per
// chunk. Absent `execution`, pipelines run serially in declaration order.

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PipelinesConfig {
    pub pipelines: Vec<Pipeline>,
    #[serde(default)]
    pub execution: Vec<ExecutionStage>,
}

impl Default for PipelinesConfig {
    fn default() -> Self {
        PipelinesConfig { pipelines: Vec::new(), execution: Vec::new() }
    }
}

/// One scheduler stage: a `group` of nodes that run in parallel; stages run
/// serially (a barrier). A stage with `mode: serial` runs its group one-by-one.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExecutionStage {
    #[serde(default)]
    pub group: Vec<GroupNode>,
    #[serde(default = "default_parallel")]
    pub mode: String,
}

fn default_parallel() -> String {
    "parallel".to_string()
}

/// A group member: either a bare pipeline name, or a fan-out node.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum GroupNode {
    /// `- name` — a plain pipeline reference; one task.
    Name(String),
    /// `- {pipeline: name, partition: "partition(src.field, workers)"}` — one
    /// task per partition.
    Node {
        pipeline: String,
        #[serde(default)]
        partition: Option<String>,
        #[serde(default)]
        priority: Option<u32>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Pipeline {
    pub name: Option<String>,
    #[serde(default)]
    pub sources: Vec<Source>,
    #[serde(default)]
    pub datasets: Vec<Dataset>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Source {
    pub name: String,
    /// Duckdb-style store URI. `file:///path` (or a bare path) reads a JSON
    /// document; `memory://<table>` / `duckdb://<file>?table=<t>` read an
    /// existing table from the shared store.
    #[serde(default)]
    pub uri: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub json_path: Option<String>,
}

/// A store target. Kept as a single string for compactness; parsed at run time.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Output {
    /// The store URI this dataset is written to:
    ///   * `file:///...` or a bare path  -> parquet file (or directory)
    ///   * `memory://<table>`            -> in-memory duckdb table
    ///   * `duckdb://<file>?table=<t>`   -> duckdb file-backed table
    #[serde(default)]
    pub uri: String,
    #[serde(default)]
    pub format: String,
    #[serde(default)]
    pub partition_by: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Dataset {
    pub name: String,
    #[serde(rename = "type")]
    pub dataset_type: String,
    /// Default source (a `Source.name`) this dataset reads from. `$` resolves to
    /// this source's records.
    #[serde(default)]
    pub source: String,
    /// Output store definition (URI + optional hive partition columns).
    #[serde(default)]
    pub to: Output,
    #[serde(default)]
    pub fields: Vec<Field>,
    #[serde(default)]
    pub unwind_rules: Vec<UnwindRule>,
    /// Optional left-join against an existing table (see docs/schema.md).
    #[serde(default)]
    pub join: Option<Join>,
    /// Optional per-row filter evaluated per output row (e.g.
    /// `$.name IN $task.ctx.units` for partition fan-out). Rows failing the filter
    /// are dropped for this dataset only.
    #[serde(default)]
    pub filter: Option<String>,
    /// Optional "assemble a new JSON root per output row". Maps a child key to
    /// what it contains:
    ///   * `$`             — this source's raw record (the whole JSON record)
    ///   * `<unwindAlias>` — the raw JSON of the unwound element for this row
    ///   * `<joinAlias>`   — the full joined row (the join's selected columns)
    /// Field expressions then address children of this assembled root, e.g.
    /// `$.instrument.symbol`, `$.leg.type`, `cast($.market.spot as double)`.
    /// Extracted fields become separate typed columns rather than one JSON blob.
    /// This powers the scheduler's "combine raw instrument + joined market JSON
    /// under a new root" fan-out outputting Parquet (see docs/scheduler.md).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_root: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Field {
    pub name: String,
    pub expression: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnwindRule {
    pub name: String,
    pub condition: String,
    pub unwind_path: String,
    pub output_alias: String,
}

/// A left-join of this dataset's rows against a table produced earlier (usually
/// by a prior pipeline stored to `memory://` or `duckdb://`).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Join {
    /// Column alias (source prefix) exposed to field expressions, e.g. `mkt`,
    /// so a joined field is written `mkt.spot`.
    pub alias: String,
    /// The table to join against (a store URI).
    pub target: String,
    /// Expression for this dataset's own join key (e.g. `$.u` — the unwound
    /// alias). Evaluated per row against the default source.
    pub left_key: String,
    /// Right-table key column.
    pub right_key: String,
    /// Columns selected from the right table, exposed under `alias` (e.g. `spot`).
    #[serde(default)]
    pub columns: Vec<String>,
}
