use regex::Regex;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_to_string(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

fn list_files(dir: &Path, suffix: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display())) {
        let entry = entry.unwrap_or_else(|e| panic!("dir entry error in {}: {e}", dir.display()));
        let path = entry.path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.ends_with(suffix))
            .unwrap_or(false)
        {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn extract_catalog_metrics() -> BTreeSet<String> {
    let catalog_path = repo_root().join("docs/telemetry/metric-catalog.md");
    let text = read_to_string(&catalog_path);

    let start = text
        .find("## Metric Catalog")
        .unwrap_or_else(|| panic!("missing '## Metric Catalog' in {}", catalog_path.display()));
    let end = text
        .find("## Dashboard Coverage Requirements")
        .unwrap_or_else(|| {
            panic!(
                "missing '## Dashboard Coverage Requirements' in {}",
                catalog_path.display()
            )
        });
    let section = &text[start..end];

    let re = Regex::new(r"\|\s*`([a-zA-Z0-9_.]+)`\s*\|")
        .expect("valid regex for metric catalog parsing");

    re.captures_iter(section)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .collect()
}

fn extract_cardinalsin_metric_tokens(text: &str) -> BTreeSet<String> {
    let token_re = Regex::new(r"[a-zA-Z_:][a-zA-Z0-9_:]*").expect("valid token regex");
    token_re
        .find_iter(text)
        .map(|m| m.as_str().to_string())
        .filter(|name| name.starts_with("cardinalsin_"))
        .collect()
}

fn normalize_prom_metric(name: &str) -> String {
    for suffix in ["_bucket", "_sum", "_count"] {
        if let Some(stripped) = name.strip_suffix(suffix) {
            return stripped.to_string();
        }
    }
    name.to_string()
}

#[test]
fn dashboard_and_query_pack_metrics_are_in_catalog() {
    let catalog_metrics = extract_catalog_metrics();
    let mut referenced = BTreeSet::new();

    let dashboards = list_files(
        &repo_root().join("deploy/grafana/provisioning/dashboards"),
        ".json",
    );
    for file in dashboards {
        let text = read_to_string(&file);
        for metric in extract_cardinalsin_metric_tokens(&text) {
            referenced.insert(normalize_prom_metric(&metric));
        }
    }

    let promql_files = list_files(&repo_root().join("scripts/telemetry/query-pack"), ".promql");
    for file in promql_files {
        let text = read_to_string(&file);
        for metric in extract_cardinalsin_metric_tokens(&text) {
            referenced.insert(normalize_prom_metric(&metric));
        }
    }

    let missing: Vec<String> = referenced
        .difference(&catalog_metrics)
        .cloned()
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "metrics referenced by dashboards/query pack are missing from docs/telemetry/metric-catalog.md: {:?}",
        missing
    );
}

#[test]
fn critical_epic65_metrics_remain_documented() {
    let catalog_metrics = extract_catalog_metrics();
    let required = [
        "cardinalsin_ingester_write_rows_total",
        "cardinalsin_ingester_write_latency_seconds",
        "cardinalsin_ingester_wal_operations_total",
        "cardinalsin_ingester_buffer_fullness_ratio",
        "cardinalsin_query_requests_total",
        "cardinalsin_query_latency_seconds",
        "cardinalsin_query_bytes_scanned_total",
        "cardinalsin_query_cache_hits_total",
        "cardinalsin_query_cache_misses_total",
        "cardinalsin_compaction_cycle_duration_seconds",
        "cardinalsin_compaction_jobs_total",
        "cardinalsin_compaction_l0_pending_files",
        "cardinalsin_metadata_cas_attempts_total",
        "cardinalsin_metadata_lease_operations_total",
        "cardinalsin_split_actions_total",
    ];

    let missing: Vec<&str> = required
        .iter()
        .copied()
        .filter(|metric| !catalog_metrics.contains(*metric))
        .collect();

    assert!(
        missing.is_empty(),
        "critical telemetry metrics are missing from docs/telemetry/metric-catalog.md: {:?}",
        missing
    );
}
