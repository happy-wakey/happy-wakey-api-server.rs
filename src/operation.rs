use axum::http::StatusCode;
use happy_wakey_interfaces::{
    ApiError, ServiceOperation, ServiceOperationRequest, ServiceOperationResponse,
    ServiceOperationStatus,
};
use next_loggers::{json, Map};
use uuid::Uuid;

use crate::{error::ApiFailure, handlers, AppState};

pub const REQUEST_SCHEMA: &str = "happy-wakey.service-operation.request.v1";
pub const RESPONSE_SCHEMA: &str = "happy-wakey.service-operation.response.v1";
pub const MAX_REQUEST_BYTES: usize = 32 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 900 * 1024;
const NIL_OPERATION_ID: &str = "00000000-0000-0000-0000-000000000000";

pub async fn execute(
    state: &AppState,
    request: ServiceOperationRequest,
) -> ServiceOperationResponse {
    let operation_id = match validate_request(&request) {
        Ok(operation_id) => operation_id,
        Err(failure) => return failure_response(&request.operation_id, failure),
    };

    let result = async {
        let identity = state.auth.authenticate_token(&request.bearer_token).await?;
        match request.operation {
            ServiceOperation::ListAlarms => {
                handlers::list_alarms_for_subject(&state.db, &identity.subject).await
            }
        }
    }
    .await;

    match result {
        Ok(alarms) => {
            emit(
                state,
                "service_operation.list_alarms",
                StatusCode::OK,
                false,
            );
            success_response(&operation_id.to_string(), alarms)
        }
        Err(failure) => {
            emit(
                state,
                "service_operation.list_alarms",
                failure_status(&failure),
                true,
            );
            failure_response(&operation_id.to_string(), failure)
        }
    }
}

pub async fn execute_bytes(state: &AppState, bytes: &[u8]) -> Vec<u8> {
    let response = if bytes.len() > MAX_REQUEST_BYTES {
        failure_response(
            NIL_OPERATION_ID,
            ApiFailure::Invalid("service operation request exceeded its byte limit".into()),
        )
    } else {
        match serde_json::from_slice::<ServiceOperationRequest>(bytes) {
            Ok(request) => execute(state, request).await,
            Err(_) => failure_response(
                NIL_OPERATION_ID,
                ApiFailure::Invalid("service operation request was malformed".into()),
            ),
        }
    };
    encode_response(response)
}

pub fn encode_response(response: ServiceOperationResponse) -> Vec<u8> {
    let encoded = serde_json::to_vec(&response).unwrap_or_else(|_| {
        serde_json::to_vec(&failure_response(
            &response.operation_id,
            ApiFailure::Contract("service response could not be encoded".into()),
        ))
        .expect("static service failure response must serialize")
    });
    if encoded.len() <= MAX_RESPONSE_BYTES {
        return encoded;
    }
    serde_json::to_vec(&failure_response(
        &response.operation_id,
        ApiFailure::Contract("service response exceeded its byte limit".into()),
    ))
    .expect("static oversized response must serialize")
}

pub fn validate_request(request: &ServiceOperationRequest) -> Result<Uuid, ApiFailure> {
    if request.schema != REQUEST_SCHEMA {
        return Err(ApiFailure::Invalid(
            "unsupported service operation schema".into(),
        ));
    }
    let operation_id = Uuid::parse_str(&request.operation_id)
        .map_err(|_| ApiFailure::Invalid("operation_id must be a UUID".into()))?;
    if request.bearer_token.is_empty()
        || request.bearer_token.len() > 16 * 1024
        || request.bearer_token.chars().any(char::is_whitespace)
        || request.bearer_token.chars().any(char::is_control)
    {
        return Err(ApiFailure::Unauthorized);
    }
    Ok(operation_id)
}

pub(crate) fn success_response(
    operation_id: &str,
    alarms: Vec<happy_wakey_interfaces::Alarm>,
) -> ServiceOperationResponse {
    ServiceOperationResponse {
        schema: RESPONSE_SCHEMA.to_owned(),
        operation_id: operation_id.to_owned(),
        status: ServiceOperationStatus::Ok,
        alarms,
        error: None,
    }
}

pub(crate) fn failure_response(
    operation_id: &str,
    failure: ApiFailure,
) -> ServiceOperationResponse {
    let (status, code, message, retryable) = match failure {
        ApiFailure::Unauthorized => (
            ServiceOperationStatus::Unauthorized,
            "unauthorized",
            "authentication failed",
            false,
        ),
        ApiFailure::Invalid(_) => (
            ServiceOperationStatus::Invalid,
            "invalid_request",
            "service operation was invalid",
            false,
        ),
        ApiFailure::AuthUnavailable | ApiFailure::ServiceUnavailable | ApiFailure::Database(_) => (
            ServiceOperationStatus::Unavailable,
            "service_unavailable",
            "service operation is temporarily unavailable",
            true,
        ),
        ApiFailure::NotFound | ApiFailure::Conflict(_) | ApiFailure::Contract(_) => (
            ServiceOperationStatus::Unavailable,
            "service_unavailable",
            "service operation could not be completed",
            false,
        ),
    };
    ServiceOperationResponse {
        schema: RESPONSE_SCHEMA.to_owned(),
        operation_id: Uuid::parse_str(operation_id)
            .map(|value| value.to_string())
            .unwrap_or_else(|_| NIL_OPERATION_ID.to_owned()),
        status,
        alarms: Vec::new(),
        error: Some(ApiError {
            code: code.into(),
            message: message.into(),
            retryable,
            trace_id: None,
        }),
    }
}

fn failure_status(failure: &ApiFailure) -> StatusCode {
    match failure {
        ApiFailure::Unauthorized => StatusCode::UNAUTHORIZED,
        ApiFailure::Invalid(_) => StatusCode::UNPROCESSABLE_ENTITY,
        ApiFailure::NotFound => StatusCode::NOT_FOUND,
        ApiFailure::Conflict(_) => StatusCode::CONFLICT,
        ApiFailure::AuthUnavailable
        | ApiFailure::ServiceUnavailable
        | ApiFailure::Database(_)
        | ApiFailure::Contract(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn emit(state: &AppState, operation: &str, status: StatusCode, failed: bool) {
    let mut fields = Map::new();
    fields.insert("operation".into(), json!(operation));
    fields.insert("status".into(), json!(status.as_u16()));
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
        .add_tags(["happy-wakey", "api", "transport"])
        .send();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> ServiceOperationRequest {
        ServiceOperationRequest {
            schema: REQUEST_SCHEMA.into(),
            operation_id: "018f5cc6-6d8b-7b2a-9f38-269e6a7b1f11".into(),
            bearer_token: "synthetic.test.token".into(),
            operation: ServiceOperation::ListAlarms,
        }
    }

    #[test]
    fn request_validation_is_strict_and_bounded() {
        assert!(validate_request(&request()).is_ok());

        let mut wrong_schema = request();
        wrong_schema.schema = "happy-wakey.service-operation.request.v2".into();
        assert!(matches!(
            validate_request(&wrong_schema),
            Err(ApiFailure::Invalid(_))
        ));

        let mut bad_id = request();
        bad_id.operation_id = "not-a-uuid".into();
        assert!(matches!(
            validate_request(&bad_id),
            Err(ApiFailure::Invalid(_))
        ));

        let mut injected = request();
        injected.bearer_token = "token\r\ninjected".into();
        assert!(matches!(
            validate_request(&injected),
            Err(ApiFailure::Unauthorized)
        ));
    }

    #[test]
    fn unknown_fields_and_oversized_responses_fail_closed() {
        let mut value = serde_json::to_value(request()).unwrap();
        value["owner_id"] = json!("attacker-selected-owner");
        assert!(serde_json::from_value::<ServiceOperationRequest>(value).is_err());

        let response = ServiceOperationResponse {
            schema: RESPONSE_SCHEMA.into(),
            operation_id: request().operation_id,
            status: ServiceOperationStatus::Ok,
            alarms: Vec::new(),
            error: None,
        };
        assert!(encode_response(response).len() < MAX_RESPONSE_BYTES);
    }
}
