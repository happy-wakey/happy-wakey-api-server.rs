pub mod auth;
pub mod entity;
pub mod error;
pub mod handlers;
pub mod reducer;
pub mod scheduler;

use std::{env, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    routing::{get, post},
    Router,
};
use next_loggers::{Logger, Options};
use reqwest::redirect::Policy;
use sea_orm::{Database, DatabaseConnection};
use tower_http::trace::TraceLayer;

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub bind: String,
    pub shared_auth_base: String,
    pub shared_auth_audience: String,
    pub introspect_secret: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            database_url: env::var("DATABASE_URL").context("DATABASE_URL is required")?,
            bind: env::var("HAPPY_WAKEY_API_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into()),
            shared_auth_base: env::var("HAPPY_WAKEY_SHARED_AUTH_BASE")
                .unwrap_or_else(|_| "https://auth.oresoftware.dev".into()),
            shared_auth_audience: env::var("HAPPY_WAKEY_SHARED_AUTH_AUDIENCE")
                .unwrap_or_else(|_| "happy-wakey".into()),
            introspect_secret: env::var("HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET")
                .context("HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET is required")?
                .trim()
                .to_owned(),
        })
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.introspect_secret.is_empty(),
            "introspection secret is empty"
        );
        anyhow::ensure!(
            self.shared_auth_base.starts_with("https://"),
            "Shared Auth must use HTTPS"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct AppState {
    pub db: DatabaseConnection,
    pub auth: auth::SharedAuth,
    pub telemetry: Arc<Logger>,
}

impl AppState {
    pub async fn connect(config: &Config) -> Result<Self> {
        config.validate()?;
        let db = Database::connect(&config.database_url)
            .await
            .context("connect to PostgreSQL/CockroachDB")?;
        let http = reqwest::Client::builder()
            .redirect(Policy::none())
            .build()
            .context("build Shared Auth client")?;
        Ok(Self {
            db,
            auth: auth::SharedAuth::new(
                http,
                config.shared_auth_base.clone(),
                config.shared_auth_audience.clone(),
                config.introspect_secret.clone(),
            ),
            telemetry: Arc::new(Logger::new(Options {
                app_name: "happy-wakey-api-server".into(),
                ..Options::default()
            })),
        })
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(handlers::health))
        .route(
            "/v1/alarms",
            get(handlers::list_alarms).post(handlers::create_alarm),
        )
        .route(
            "/v1/occurrences/{occurrence_id}/transitions",
            post(handlers::transition_occurrence),
        )
        .route("/v1/sync/pull", get(handlers::pull_changes))
        .route("/v1/sync/push", post(handlers::push_changes))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
