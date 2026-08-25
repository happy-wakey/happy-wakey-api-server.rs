use std::{env, fs::File, io::BufReader, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
    time::timeout,
};
use tokio_rustls::{rustls, server::TlsStream, TlsAcceptor};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::{operation, AppState};

const DEFAULT_MAX_CONNECTIONS: usize = 256;
const DEFAULT_MAX_REQUESTS_PER_CONNECTION: usize = 128;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub struct TcpServerConfig {
    pub bind: String,
    pub certificate_path: PathBuf,
    pub private_key_path: PathBuf,
    pub max_connections: usize,
    pub max_requests_per_connection: usize,
    pub idle_timeout: Duration,
}

impl TcpServerConfig {
    pub fn from_env() -> Result<Option<Self>> {
        let Some(bind) = optional_env("HAPPY_WAKEY_API_TCP_BIND") else {
            return Ok(None);
        };
        let certificate_path = required_path("HAPPY_WAKEY_API_TCP_TLS_CERT")?;
        let private_key_path = required_path("HAPPY_WAKEY_API_TCP_TLS_KEY")?;
        Ok(Some(Self {
            bind,
            certificate_path,
            private_key_path,
            max_connections: bounded_env(
                "HAPPY_WAKEY_API_TCP_MAX_CONNECTIONS",
                DEFAULT_MAX_CONNECTIONS,
                1,
                4096,
            )?,
            max_requests_per_connection: bounded_env(
                "HAPPY_WAKEY_API_TCP_MAX_REQUESTS_PER_CONNECTION",
                DEFAULT_MAX_REQUESTS_PER_CONNECTION,
                1,
                4096,
            )?,
            idle_timeout: Duration::from_secs(bounded_env(
                "HAPPY_WAKEY_API_TCP_IDLE_TIMEOUT_SECONDS",
                DEFAULT_IDLE_TIMEOUT.as_secs() as usize,
                1,
                300,
            )? as u64),
        }))
    }
}

pub struct TcpServer {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    state: AppState,
    config: TcpServerConfig,
    permits: Arc<Semaphore>,
}

impl TcpServer {
    pub async fn bind(config: TcpServerConfig, state: AppState) -> Result<Self> {
        let tls = load_tls_config(&config)?;
        let listener = TcpListener::bind(&config.bind)
            .await
            .with_context(|| format!("bind persistent TLS service at {}", config.bind))?;
        Ok(Self {
            listener,
            acceptor: TlsAcceptor::from(Arc::new(tls)),
            state,
            permits: Arc::new(Semaphore::new(config.max_connections)),
            config,
        })
    }

    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    changed.context("persistent TLS shutdown channel closed")?;
                    return Ok(());
                }
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted.context("accept persistent TLS connection")?;
                    let Ok(permit) = self.permits.clone().try_acquire_owned() else {
                        continue;
                    };
                    let acceptor = self.acceptor.clone();
                    let state = self.state.clone();
                    let config = self.config.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let Ok(stream) = acceptor.accept(stream).await else {
                            return;
                        };
                        let _ = serve_connection(stream, state, &config).await;
                    });
                }
            }
        }
    }
}

async fn serve_connection(
    stream: TlsStream<TcpStream>,
    state: AppState,
    config: &TcpServerConfig,
) -> Result<()> {
    let mut framed = bounded_framed(stream, operation::MAX_REQUEST_BYTES);
    for _ in 0..config.max_requests_per_connection {
        let Some(frame) = timeout(config.idle_timeout, framed.next())
            .await
            .context("persistent TLS connection idle timeout")?
            .transpose()
            .context("read length-delimited service operation")?
        else {
            return Ok(());
        };
        let response = operation::execute_bytes(&state, &frame).await;
        framed
            .send(Bytes::from(response))
            .await
            .context("write length-delimited service operation")?;
    }
    Ok(())
}

fn bounded_framed<T>(stream: T, max_frame_length: usize) -> Framed<T, LengthDelimitedCodec>
where
    T: AsyncRead + AsyncWrite,
{
    LengthDelimitedCodec::builder()
        .length_field_length(4)
        .max_frame_length(max_frame_length)
        .new_framed(stream)
}

fn load_tls_config(config: &TcpServerConfig) -> Result<rustls::ServerConfig> {
    let mut certificate_reader = BufReader::new(
        File::open(&config.certificate_path).context("open persistent TLS certificate")?,
    );
    let certificates = rustls_pemfile::certs(&mut certificate_reader)
        .collect::<std::io::Result<Vec<_>>>()
        .context("parse persistent TLS certificate")?;
    anyhow::ensure!(!certificates.is_empty(), "TLS certificate chain is empty");

    let mut private_key_reader =
        BufReader::new(File::open(&config.private_key_path).context("open persistent TLS key")?);
    let private_key = rustls_pemfile::private_key(&mut private_key_reader)
        .context("parse persistent TLS key")?
        .context("TLS private key is missing")?;
    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .context("certificate and private key do not match")
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn required_path(name: &str) -> Result<PathBuf> {
    optional_env(name)
        .map(PathBuf::from)
        .with_context(|| format!("{name} is required when persistent TLS is enabled"))
}

fn bounded_env(name: &str, default: usize, minimum: usize, maximum: usize) -> Result<usize> {
    let value = match optional_env(name) {
        Some(value) => value
            .parse::<usize>()
            .with_context(|| format!("{name} must be an integer"))?,
        None => default,
    };
    anyhow::ensure!(
        (minimum..=maximum).contains(&value),
        "{name} must be between {minimum} and {maximum}"
    );
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_codec_is_length_delimited_and_bounded() {
        let (client, server) = tokio::io::duplex(1024);
        let mut client = bounded_framed(client, 64);
        let mut server = bounded_framed(server, 64);
        client.send(Bytes::from_static(b"request")).await.unwrap();
        assert_eq!(server.next().await.unwrap().unwrap(), b"request"[..]);

        let (client, server) = tokio::io::duplex(1024);
        let mut client = bounded_framed(client, 128);
        let mut server = bounded_framed(server, 8);
        client.send(Bytes::from_static(b"oversized")).await.unwrap();
        assert!(server.next().await.unwrap().is_err());
    }

    #[test]
    fn numeric_limits_are_fail_closed() {
        assert!(bounded_env("HAPPY_WAKEY_TEST_UNSET", 0, 1, 4).is_err());
        assert_eq!(bounded_env("HAPPY_WAKEY_TEST_UNSET", 4, 1, 4).unwrap(), 4);
    }
}
