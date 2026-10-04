use anyhow::Result;
use context_index::ProjectIndex;
use context_testkit::request_identity::{evaluate, materialize_repository, parse_corpus};

const CORPUS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benchmarks/corpora/request-identity-v1.json"
));

fn main() -> Result<()> {
    let corpus = parse_corpus(CORPUS)?;
    let root = tempfile::tempdir()?;
    materialize_repository(&corpus, root.path())?;
    let index = ProjectIndex::build(root.path())?;
    let report = evaluate(&corpus, &index)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
