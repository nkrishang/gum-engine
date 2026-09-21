//! Guard rail for the project's main axis of change: adding chains.
//!
//! Everything chain-specific must live behind `ChainAdapter` (`src/chain/`). If the pipeline, funds,
//! store, API or any other core module starts branching on a chain id, a kind or a chain's name, adding
//! the next chain stops being "one adapter file + config" — so this test fails the build instead.

use std::path::{Path, PathBuf};

/// Core code: everything except `src/chain/` (adapters live there) and `src/config.rs`.
const CORE: &[&str] =
    &["api.rs", "domain.rs", "engine.rs", "error.rs", "leader.rs", "queue.rs", "signer.rs", "stats.rs", "telemetry.rs", "webhook.rs", "funds", "pipeline", "rpc", "store"];

const FORBIDDEN: &[(&str, &str)] = &[
    ("kinds::", "core code must not reach into a specific chain kind"),
    ("Monad", "chain name in core code"),
    ("Arbitrum", "chain name in core code"),
    ("OpStack", "chain name in core code"),
    ("Optimism", "chain name in core code"),
    ("Anvil", "chain name in core code"),
    ("\"monad\"", "kind string in core code"),
    ("\"opstack\"", "kind string in core code"),
    ("\"arbitrum\"", "kind string in core code"),
    ("\"geth\"", "kind string in core code"),
    ("31337", "hard-coded chain id"),
    ("8453", "hard-coded chain id"),
    ("42161", "hard-coded chain id"),
    (".kind()", "branching on the adapter kind (only reporting it is allowed, see ALLOWED)"),
];

/// The only tolerated mentions: reporting the kind string to operators.
const ALLOWED: &[(&str, &str)] = &[("api.rs", "\"kind\": c.adapter.kind()"), ("leader.rs", "kind = chain.adapter.kind()")];

fn rust_files(path: &Path, out: &mut Vec<PathBuf>) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).expect("readable source directory") {
            rust_files(&entry.expect("dir entry").path(), out);
        }
    } else if path.extension().is_some_and(|e| e == "rs") {
        out.push(path.to_path_buf());
    }
}

#[test]
fn core_modules_never_branch_on_a_chain() {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    for entry in CORE {
        let path = src.join(entry);
        assert!(path.exists(), "core path {} is gone; update the CORE list in this test", path.display());
        rust_files(&path, &mut files);
    }

    let mut violations = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("readable source file");
        let name = file.file_name().unwrap().to_string_lossy().to_string();
        for (number, line) in text.lines().enumerate() {
            for (needle, why) in FORBIDDEN {
                if !line.contains(needle) {
                    continue;
                }
                if ALLOWED.iter().any(|(f, allowed)| *f == name && line.contains(allowed)) {
                    continue;
                }
                violations.push(format!("{}:{}: `{needle}` — {why}\n    {}", file.strip_prefix(&src).unwrap().display(), number + 1, line.trim()));
            }
        }
    }
    assert!(violations.is_empty(), "chain-specific code leaked out of src/chain/:\n{}\n\nMove the behaviour behind ChainAdapter (src/chain/adapter.rs).", violations.join("\n"));
}

#[test]
fn every_registered_kind_has_fixtures_and_an_adapter_file() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for kind in gum_engine::chain::registry::known_kinds() {
        assert!(root.join("src/chain/kinds").join(format!("{kind}.rs")).exists(), "kind `{kind}` has no src/chain/kinds/{kind}.rs");
        for file in ["errors.json", "receipts.json"] {
            assert!(root.join("../../fixtures").join(kind).join(file).exists(), "kind `{kind}` has no fixtures/{kind}/{file}");
        }
    }
}
