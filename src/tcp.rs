use std::{fs::File, io::BufReader, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
    time::timeout,
};
use tokio_rustls::{rustls, server::TlsStream, TlsAcceptor};

use crate::{operation, AppState};

const DEFAULT_MAX_CONNECTIONS: usize = 256;
const DEFAULT_MAX_REQUESTS_PER_CONNECTION: usize = 128;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub struct TcpServerConfig {
    pub bind: String,
    pub certificate_path: PathBuf,
    pub private_key_path: PathBuf,
    pub client_ca_path: PathBuf,
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
            client_ca_path: required_path("HAPPY_WAKEY_API_TCP_CLIENT_CA_CERT")?,
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
    mut stream: TlsStream<TcpStream>,
    state: AppState,
    config: &TcpServerConfig,
) -> Result<()> {
    for _ in 0..config.max_requests_per_connection {
        let Some(frame) = timeout(
            config.idle_timeout,
            read_frame(&mut stream, operation::MAX_REQUEST_BYTES),
        )
        .await
        .context("persistent TLS connection idle timeout")?
        .context("read length-delimited service operation")?
        else {
            return Ok(());
        };
        let response = operation::execute_bytes(&state, &frame).await;
        timeout(
            config.idle_timeout,
            write_frame(&mut stream, &response, operation::MAX_RESPONSE_BYTES),
        )
        .await
        .context("persistent TLS response write timeout")?
        .context("write length-delimited service operation")?;
    }
    Ok(())
}

async fn read_frame<T>(stream: &mut T, maximum: usize) -> Result<Option<Vec<u8>>>
where
    T: AsyncRead + Unpin,
{
    let mut length = [0_u8; 4];
    if let Err(error) = stream.read_exact(&mut length).await {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(error).context("read frame length");
    }
    let length = u32::from_be_bytes(length) as usize;
    anyhow::ensure!(length <= maximum, "frame exceeded its byte limit");
    let mut payload = vec![0_u8; length];
    stream
        .read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    Ok(Some(payload))
}

async fn write_frame<T>(stream: &mut T, payload: &[u8], maximum: usize) -> Result<()>
where
    T: AsyncWrite + Unpin,
{
    anyhow::ensure!(payload.len() <= maximum, "frame exceeded its byte limit");
    let length = u32::try_from(payload.len()).context("frame length exceeded u32")?;
    stream
        .write_all(&length.to_be_bytes())
        .await
        .context("write frame length")?;
    stream
        .write_all(payload)
        .await
        .context("write frame payload")?;
    stream.flush().await.context("flush response frame")
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
    let mut client_ca_reader = BufReader::new(
        File::open(&config.client_ca_path).context("open persistent TLS client CA bundle")?,
    );
    let client_ca_certificates = rustls_pemfile::certs(&mut client_ca_reader)
        .collect::<std::io::Result<Vec<_>>>()
        .context("parse persistent TLS client CA bundle")?;
    anyhow::ensure!(
        !client_ca_certificates.is_empty(),
        "TLS client CA bundle is empty"
    );
    let mut client_roots = rustls::RootCertStore::empty();
    for certificate in client_ca_certificates {
        client_roots
            .add(certificate)
            .context("TLS client CA bundle contains an invalid certificate")?;
    }
    let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(client_roots))
        .build()
        .context("build persistent TLS client certificate verifier")?;
    rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certificates, private_key)
        .context("certificate and private key do not match")
}

fn optional_env(name: &str) -> Option<String> {
    crate::flags::var(name)
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
    async fn frame_codec_has_asymmetric_request_and_response_bounds() {
        let (client, server) = tokio::io::duplex(1024);
        let mut client = client;
        let mut server = server;
        write_frame(&mut client, b"request", 64).await.unwrap();
        assert_eq!(
            read_frame(&mut server, 64).await.unwrap().unwrap(),
            b"request"
        );

        let (client, server) = tokio::io::duplex(1024);
        let mut client = client;
        let mut server = server;
        write_frame(&mut client, b"oversized", 128).await.unwrap();
        assert!(read_frame(&mut server, 8).await.is_err());

        let response = vec![b'x'; operation::MAX_REQUEST_BYTES + 1];
        let (mut client, mut server) = tokio::io::duplex(response.len() + 8);
        write_frame(&mut client, &response, operation::MAX_RESPONSE_BYTES)
            .await
            .unwrap();
        assert_eq!(
            read_frame(&mut server, operation::MAX_RESPONSE_BYTES)
                .await
                .unwrap()
                .unwrap(),
            response
        );
    }

    #[test]
    fn numeric_limits_are_fail_closed() {
        assert!(bounded_env("HAPPY_WAKEY_TEST_UNSET", 0, 1, 4).is_err());
        assert_eq!(bounded_env("HAPPY_WAKEY_TEST_UNSET", 4, 1, 4).unwrap(), 4);
    }
}
