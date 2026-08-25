use anyhow::{Context, Result};
use happy_wakey_api_server::{router, AppState, Config};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    let state = AppState::connect(&config).await?;
    let listener = TcpListener::bind(&config.bind)
        .await
        .with_context(|| format!("bind {}", config.bind))?;
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve Happy Wakey API")
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
