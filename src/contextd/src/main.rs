#[tokio::main]
async fn main() -> anyhow::Result<()> {
    contextd::run_from_env().await
}
