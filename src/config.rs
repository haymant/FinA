use serde::Deserialize;

// Fields represent the YAML pipeline config schema; some are informational
// (e.g. output format) and intentionally unused by this JSON extractor.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub pipeline_name: Option<String>,
    pub source: Source,
    #[serde(default)]
    pub datasets: Vec<Dataset>,
    #[serde(default)]
    pub output: Output,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct Source {
    pub file_path: String,
    #[serde(default)]
    pub json_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Dataset {
    pub name: String,
    #[serde(rename = "type")]
    pub dataset_type: String,
    #[serde(default)]
    pub fields: Vec<Field>,
    #[serde(default)]
    pub unwind_rules: Vec<UnwindRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Field {
    pub name: String,
    pub expression: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct UnwindRule {
    pub name: String,
    pub condition: String,
    pub unwind_path: String,
    pub output_alias: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Output {
    #[serde(default)]
    pub format: String,
    #[serde(default)]
    pub output_directory: String,
    #[serde(default)]
    pub partition_by: Vec<String>,
}
