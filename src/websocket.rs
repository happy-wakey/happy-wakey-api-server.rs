//! Authenticated, bounded WebSocket service-operation transport.
//!
//! The upgrade authenticates the caller once to reject anonymous connections.
//! Every operation still carries and revalidates its bearer through the same
//! canonical executor used by TLS/TCP and NATS. Persistent transport state is
//! therefore never treated as an authorization cache.

use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{header::ORIGIN, HeaderMap},
    response::Response,
};
use tokio::time::timeout;

use crate::{error::ApiFailure, operation, AppState};

const MAX_REQUESTS_PER_CONNECTION: usize = 128;
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn upgrade(
    State(state): State<AppState>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Result<Response, ApiFailure> {
    validate_origin(&headers, &state.websocket_allowed_origins)?;
    state.auth.authenticate(&headers).await?;

    Ok(websocket
        .max_message_size(operation::MAX_REQUEST_BYTES)
        .max_frame_size(operation::MAX_REQUEST_BYTES)
        .on_upgrade(move |socket| serve(socket, state)))
}

async fn serve(mut socket: WebSocket, state: AppState) {
    for _ in 0..MAX_REQUESTS_PER_CONNECTION {
        let message = match timeout(IDLE_TIMEOUT, socket.recv()).await {
            Ok(Some(Ok(message))) => message,
            _ => return,
        };
        let request = match message {
            Message::Text(text) => text.as_bytes().to_vec(),
            Message::Binary(bytes) => bytes.to_vec(),
            Message::Ping(payload) => {
                if socket.send(Message::Pong(payload)).await.is_err() {
                    return;
                }
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => return,
        };
        let response = operation::execute_bytes(&state, &request).await;
        let response = String::from_utf8(response)
            .unwrap_or_else(|_| r#"{"status":"unavailable"}"#.to_owned());
        if socket.send(Message::Text(response.into())).await.is_err() {
            return;
        }
    }
    let _ = socket.send(Message::Close(None)).await;
}

fn validate_origin(headers: &HeaderMap, allowed: &[String]) -> Result<(), ApiFailure> {
    let Some(origin) = headers.get(ORIGIN) else {
        return Ok(());
    };
    let origin = origin
        .to_str()
        .map_err(|_| ApiFailure::Unauthorized)?
        .trim_end_matches('/');
    if allowed.iter().any(|allowed| allowed == origin) {
        Ok(())
    } else {
        Err(ApiFailure::Unauthorized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_origins_are_exact_and_service_clients_may_be_originless() {
        let allowed = [String::from("https://app.hawky.pro")];
        assert!(validate_origin(&HeaderMap::new(), &allowed).is_ok());

        let mut accepted = HeaderMap::new();
        accepted.insert(ORIGIN, "https://app.hawky.pro".parse().unwrap());
        assert!(validate_origin(&accepted, &allowed).is_ok());

        let mut denied = HeaderMap::new();
        denied.insert(ORIGIN, "https://evil.example".parse().unwrap());
        assert!(matches!(
            validate_origin(&denied, &allowed),
            Err(ApiFailure::Unauthorized)
        ));
    }
}
