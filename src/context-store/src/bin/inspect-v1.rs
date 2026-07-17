use std::{env, path::Path};

fn main() -> anyhow::Result<()> {
    let path = env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: inspect-v1 <copied-python-lmdb>"))?;
    let inspection = context_store::inspect_v1(Path::new(&path))?;
    println!("{}", serde_json::to_string(&inspection)?);
    Ok(())
}
