use anyhow::{Context, Result};
use happy_wakey_api_server::{
    flags,
    nats::{JetStreamConfig, JetStreamWorker},
    router,
    tcp::{TcpServer, TcpServerConfig},
    AppState, Config,
};
use tokio::{
    net::TcpListener,
    sync::watch,
    task::JoinSet,
    time::{timeout, Duration},
};

#[tokio::main]
async fn main() -> Result<()> {
    if let Some(output) = flags::process_control().map_err(std::io::Error::other)? {
        print!("{output}");
        return Ok(());
    }
    let config = Config::from_env()?;
    let state = AppState::connect(&config).await?;
    let listener = TcpListener::bind(&config.bind)
        .await
        .with_context(|| format!("bind {}", config.bind))?;
    let tcp_server = match TcpServerConfig::from_env()? {
        Some(tcp_config) => Some(TcpServer::bind(tcp_config, state.clone()).await?),
        None => None,
    };
    let jetstream_worker = match JetStreamConfig::from_env()? {
        Some(nats_config) => Some(JetStreamWorker::connect(nats_config, state.clone()).await?),
        None => None,
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut services = JoinSet::new();
    let mut http_shutdown = shutdown_rx.clone();
    services.spawn(async move {
        axum::serve(
            listener,
            ores_middleware::frameworks::axum::install_from_env(
                router(state),
                env!("CARGO_PKG_NAME"),
            )?,
        )
        .with_graceful_shutdown(async move {
            let _ = http_shutdown.changed().await;
        })
        .await
        .context("serve Happy Wakey HTTP API")
    });
    if let Some(server) = tcp_server {
        services.spawn(server.serve(shutdown_rx.clone()));
    }
    if let Some(worker) = jetstream_worker {
        services.spawn(worker.serve(shutdown_rx));
    }

    let premature = tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            None
        }
        joined = services.join_next() => Some(
            joined.context("service task set ended unexpectedly")?
                .context("service task panicked")?
        ),
    };
    let _ = shutdown_tx.send(true);
    timeout(Duration::from_secs(10), async {
        while let Some(joined) = services.join_next().await {
            joined.context("service task panicked")??;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("service shutdown timed out")??;

    if let Some(outcome) = premature {
        outcome?;
        anyhow::bail!("a Happy Wakey service ended unexpectedly");
    }
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await.context("listen for shutdown")
}
