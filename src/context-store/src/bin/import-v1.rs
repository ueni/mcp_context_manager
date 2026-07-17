use std::{env, path::Path};

fn main() -> anyhow::Result<()> {
    let mut arguments = env::args().skip(1);
    let project_state = arguments.next().ok_or_else(|| {
        anyhow::anyhow!("usage: import-v1 <project-state> <python-lmdb> [references-dir]")
    })?;
    let python_lmdb = arguments.next().ok_or_else(|| {
        anyhow::anyhow!("usage: import-v1 <project-state> <python-lmdb> [references-dir]")
    })?;
    let references = arguments.next();
    if arguments.next().is_some() {
        anyhow::bail!("too many arguments");
    }
    let store = context_store::StateStore::open(&project_state)?;
    let manifest = store.import_v1(
        Path::new(&python_lmdb),
        references.as_deref().map(Path::new),
    )?;
    println!("{}", serde_json::to_string(&manifest)?);
    Ok(())
}
