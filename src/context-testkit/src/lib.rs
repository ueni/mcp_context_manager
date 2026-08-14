//! Differential fixtures and synthetic repositories for the native migration.

use context_core::ContextPackRequest;

pub mod request_identity;

pub fn minimal_pack_request(prompt: &str) -> ContextPackRequest {
    serde_json::from_value(serde_json::json!({ "prompt": prompt }))
        .expect("the built-in request fixture is valid")
}
