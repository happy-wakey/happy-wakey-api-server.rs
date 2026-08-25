use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use async_nats::{
    jetstream::{
        self,
        consumer::{self, AckPolicy},
        message::{AckKind, PublishMessage},
        stream::{RetentionPolicy, StorageType},
    },
    ConnectOptions,
};
use bytes::Bytes;
use futures_util::StreamExt;
use happy_wakey_interfaces::AsyncOperationSignal;
use next_loggers::{json, Map};
use tokio::sync::watch;

use crate::{
    async_operations::{self, REQUEST_SUBJECT, RESPONSE_SUBJECT_PREFIX},
    operation, AppState,
};

const MAX_SIGNAL_BYTES: usize = 4 * 1024;
const DEFAULT_REQUEST_STREAM: &str = "HAPPY_WAKEY_OPERATIONS";
const DEFAULT_RESPONSE_STREAM: &str = "HAPPY_WAKEY_RESPONSES";
const DEFAULT_CONSUMER: &str = "happy-wakey-api";
const RETRY_DELAY: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub struct JetStreamConfig {
    pub url: String,
    pub credentials_path: PathBuf,
    pub request_stream: String,
    pub response_stream: String,
    pub consumer: String,
}

impl JetStreamConfig {
    pub fn from_env() -> Result<Option<Self>> {
        let Some(url) = optional_env("HAPPY_WAKEY_NATS_URL") else {
            return Ok(None);
        };
        let config = Self {
            url,
            credentials_path: PathBuf::from(required_env("HAPPY_WAKEY_NATS_CREDENTIALS_FILE")?),
            request_stream: optional_env("HAPPY_WAKEY_NATS_REQUEST_STREAM")
                .unwrap_or_else(|| DEFAULT_REQUEST_STREAM.into()),
            response_stream: optional_env("HAPPY_WAKEY_NATS_RESPONSE_STREAM")
                .unwrap_or_else(|| DEFAULT_RESPONSE_STREAM.into()),
            consumer: optional_env("HAPPY_WAKEY_NATS_CONSUMER")
                .unwrap_or_else(|| DEFAULT_CONSUMER.into()),
        };
        config.validate()?;
        Ok(Some(config))
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.url.starts_with("tls://"),
            "HAPPY_WAKEY_NATS_URL must use tls://"
        );
        let authority = self
            .url
            .strip_prefix("tls://")
            .context("NATS TLS URL is malformed")?
            .split('/')
            .next()
            .unwrap_or_default();
        anyhow::ensure!(
            !authority.is_empty() && !authority.contains('@'),
            "NATS credentials must come from the credentials file, not the URL"
        );
        for (name, value) in [
            ("request stream", &self.request_stream),
            ("response stream", &self.response_stream),
            ("consumer", &self.consumer),
        ] {
            anyhow::ensure!(safe_topology_name(value), "invalid {name} name");
        }
        Ok(())
    }
}

pub struct JetStreamWorker {
    context: jetstream::Context,
    consumer: consumer::PullConsumer,
    state: AppState,
}

impl JetStreamWorker {
    pub async fn connect(config: JetStreamConfig, state: AppState) -> Result<Self> {
        config.validate()?;
        let options = ConnectOptions::with_credentials_file(&config.credentials_path)
            .await
            .context("load NATS credentials file")?
            .require_tls(true)
            .name("happy-wakey-api-server")
            .connection_timeout(Duration::from_secs(5))
            .subscription_capacity(512);
        let client = options
            .connect(config.url.clone())
            .await
            .context("connect to NATS over TLS")?;
        let context = jetstream::new(client);

        let request_stream = context
            .get_stream(&config.request_stream)
            .await
            .context("get pre-provisioned request stream")?;
        validate_request_stream(request_stream.cached_info())?;
        let response_stream = context
            .get_stream(&config.response_stream)
            .await
            .context("get pre-provisioned response stream")?;
        validate_response_stream(response_stream.cached_info())?;
        let consumer: consumer::PullConsumer = request_stream
            .get_consumer(&config.consumer)
            .await
            .map_err(|error| {
                anyhow::anyhow!("get pre-provisioned durable pull consumer: {error}")
            })?;
        validate_consumer(consumer.cached_info(), &config.consumer)?;

        Ok(Self {
            context,
            consumer,
            state,
        })
    }

    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut messages = self
            .consumer
            .messages()
            .await
            .context("start durable JetStream pull consumer")?;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    changed.context("JetStream shutdown channel closed")?;
                    return Ok(());
                }
                next = messages.next() => {
                    let Some(next) = next else {
                        anyhow::bail!("durable JetStream consumer ended");
                    };
                    let message = next.context("receive durable JetStream message")?;
                    self.handle(message).await?;
                }
            }
        }
    }

    async fn handle(&self, message: jetstream::Message) -> Result<()> {
        let operation_id = match decode_signal(&message.payload) {
            Ok(operation_id) => operation_id,
            Err(()) => {
                emit(&self.state, "async_signal", "terminated", true);
                message.ack_with(AckKind::Term).await.map_err(|error| {
                    anyhow::anyhow!("terminate invalid JetStream signal: {error}")
                })?;
                return Ok(());
            }
        };

        let response = match async_operations::process_outbox(&self.state, operation_id).await {
            Ok(Some(response)) => response,
            Ok(None) => {
                emit(&self.state, "async_operation", "unknown", true);
                message.ack_with(AckKind::Term).await.map_err(|error| {
                    anyhow::anyhow!("terminate unknown async operation: {error}")
                })?;
                return Ok(());
            }
            Err(_) => {
                emit(&self.state, "async_operation", "retry", true);
                message
                    .ack_with(AckKind::Nak(Some(RETRY_DELAY)))
                    .await
                    .map_err(|error| {
                        anyhow::anyhow!("request async operation redelivery: {error}")
                    })?;
                return Ok(());
            }
        };

        let payload = operation::encode_response(response);
        let subject = async_operations::response_subject(operation_id);
        let message_id = format!("happy-wakey-response-{operation_id}");
        self.context
            .send_publish(
                subject,
                PublishMessage::build()
                    .payload(Bytes::from(payload))
                    .message_id(message_id),
            )
            .await
            .context("publish durable async response")?
            .await
            .context("await durable async response acknowledgement")?;
        message.ack().await.map_err(|error| {
            anyhow::anyhow!("ack async request after durable response acknowledgement: {error}")
        })?;
        emit(&self.state, "async_operation", "completed", false);
        Ok(())
    }
}

fn decode_signal(payload: &[u8]) -> Result<uuid::Uuid, ()> {
    if payload.len() > MAX_SIGNAL_BYTES {
        return Err(());
    }
    let signal = serde_json::from_slice::<AsyncOperationSignal>(payload).map_err(|_| ())?;
    async_operations::validate_signal(&signal).map_err(|_| ())
}

fn validate_request_stream(info: &jetstream::stream::Info) -> Result<()> {
    let config = &info.config;
    anyhow::ensure!(
        config.storage == StorageType::File,
        "request stream must use file storage"
    );
    anyhow::ensure!(
        config.retention == RetentionPolicy::WorkQueue,
        "request stream must use work-queue retention"
    );
    anyhow::ensure!(
        config
            .subjects
            .iter()
            .any(|subject| subject == REQUEST_SUBJECT),
        "request stream does not own the canonical subject"
    );
    anyhow::ensure!(
        !config.duplicate_window.is_zero(),
        "request stream must configure a duplicate window"
    );
    Ok(())
}

fn validate_response_stream(info: &jetstream::stream::Info) -> Result<()> {
    let config = &info.config;
    anyhow::ensure!(
        config.storage == StorageType::File,
        "response stream must use file storage"
    );
    anyhow::ensure!(
        config.retention == RetentionPolicy::Limits,
        "response stream must use limits retention"
    );
    anyhow::ensure!(
        config
            .subjects
            .iter()
            .any(|subject| subject == &format!("{RESPONSE_SUBJECT_PREFIX}.*")),
        "response stream does not own canonical response subjects"
    );
    anyhow::ensure!(
        config.allow_direct,
        "response stream must allow direct last-message reads"
    );
    anyhow::ensure!(
        !config.duplicate_window.is_zero(),
        "response stream must configure a duplicate window"
    );
    Ok(())
}

fn validate_consumer(info: &consumer::Info, expected_name: &str) -> Result<()> {
    let config = &info.config;
    anyhow::ensure!(
        config.deliver_subject.is_none(),
        "async consumer must be pull-based"
    );
    anyhow::ensure!(
        config.durable_name.as_deref() == Some(expected_name),
        "async consumer must be the configured durable"
    );
    anyhow::ensure!(
        config.ack_policy == AckPolicy::Explicit,
        "async consumer must use explicit acknowledgements"
    );
    anyhow::ensure!(
        config.filter_subject == REQUEST_SUBJECT,
        "async consumer must filter the canonical request subject"
    );
    anyhow::ensure!(
        (2..=100).contains(&config.max_deliver),
        "async consumer max_deliver must be between 2 and 100"
    );
    anyhow::ensure!(
        (1..=4096).contains(&config.max_ack_pending),
        "async consumer max_ack_pending must be between 1 and 4096"
    );
    Ok(())
}

fn emit(state: &AppState, operation: &str, status: &str, failed: bool) {
    let mut fields = Map::new();
    fields.insert("operation".into(), json!(operation));
    fields.insert("status".into(), json!(status));
    fields.insert("lane".into(), json!("jetstream"));
    let event = if failed {
        state
            .telemetry
            .error(vec![json!("happy_wakey.api.transport")])
    } else {
        state
            .telemetry
            .info(vec![json!("happy_wakey.api.transport")])
    };
    let _ = event
        .add_fields(fields)
        .add_tags(["happy-wakey", "api", "jetstream"])
        .send();
}

fn safe_topology_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn required_env(name: &str) -> Result<String> {
    optional_env(name).with_context(|| format!("{name} is required when JetStream is enabled"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_decoding_is_strict_bounded_and_credential_free() {
        let valid = br#"{"schema":"happy-wakey.async-operation.signal.v1","operation_id":"018f5cc6-6d8b-7b2a-9f38-269e6a7b1f11"}"#;
        assert!(decode_signal(valid).is_ok());
        assert!(decode_signal(br#"{"schema":"happy-wakey.async-operation.signal.v1","operation_id":"018f5cc6-6d8b-7b2a-9f38-269e6a7b1f11","bearer_token":"attack"}"#).is_err());
        assert!(decode_signal(&vec![b'x'; MAX_SIGNAL_BYTES + 1]).is_err());
    }

    #[test]
    fn topology_names_and_tls_urls_fail_closed() {
        assert!(safe_topology_name("HAPPY_WAKEY_OPERATIONS"));
        assert!(!safe_topology_name("bad.name"));
        let config = JetStreamConfig {
            url: "nats://localhost:4222".into(),
            credentials_path: PathBuf::from("unused.creds"),
            request_stream: DEFAULT_REQUEST_STREAM.into(),
            response_stream: DEFAULT_RESPONSE_STREAM.into(),
            consumer: DEFAULT_CONSUMER.into(),
        };
        assert!(config.validate().is_err());
    }
}
