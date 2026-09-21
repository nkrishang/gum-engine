//! Maps the `kind` string of a `[chains.*]` block to its adapter. Registering a new kind is one line here.

use std::sync::Arc;

use super::{adapter::ChainAdapter, kinds};

pub fn adapter_for(kind: &str) -> Option<Arc<dyn ChainAdapter>> {
    let adapter: Arc<dyn ChainAdapter> = match kind {
        "geth" => Arc::new(kinds::geth::Geth),
        "opstack" => Arc::new(kinds::opstack::OpStack),
        "arbitrum" => Arc::new(kinds::arbitrum::Arbitrum),
        "monad" => Arc::new(kinds::monad::Monad),
        _ => return None,
    };
    Some(adapter)
}

pub fn known_kinds() -> &'static [&'static str] {
    &["geth", "opstack", "arbitrum", "monad"]
}
