use serde::Deserialize;

// Official sonicetl ETL schema (see docs/schema.md).
//
// The root document declares a list of *pipelines*, each of which declares any
// number of named *sources* (inputs) and *datasets* (outputs). Datasets may be
// pointed at individual stores (file / duckdb in-memory / duckdb file) via a URI
// and may join against previously produced tables.

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PipelinesConfig {
    pub pipelines: Vec<Pipeline>,
}

impl Default for PipelinesConfig {
    fn default() -> Self {
        PipelinesConfig { pipelines: Vec::new() }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Pipeline {
    pub name: Option<String>,
    #[serde(default)]
    pub sources: Vec<Source>,
    #[serde(default)]
    pub datasets: Vec<Dataset>,
}

#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, Deserialize, Default)]
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

#[derive(Debug, Clone, Deserialize)]
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct Field {
    pub name: String,
    pub expression: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UnwindRule {
    pub name: String,
    pub condition: String,
    pub unwind_path: String,
    pub output_alias: String,
}

/// A left-join of this dataset's rows against a table produced earlier (usually
/// by a prior pipeline stored to `memory://` or `duckdb://`).
#[derive(Debug, Clone, Deserialize)]
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
