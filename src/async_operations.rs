use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{Duration, Utc};
use happy_wakey_interfaces::{
    AsyncOperationAccepted, AsyncOperationRequest, AsyncOperationSignal, ServiceOperation,
    ServiceOperationResponse,
};
use sea_orm::{
    sea_query::OnConflict, ActiveModelTrait, ActiveValue::Set, EntityTrait, QuerySelect,
    TransactionTrait,
};
use uuid::Uuid;

use crate::{entity::async_operation, error::ApiFailure, handlers, operation, AppState};

pub const REQUEST_SCHEMA: &str = "happy-wakey.async-operation.request.v1";
pub const ACCEPTED_SCHEMA: &str = "happy-wakey.async-operation.accepted.v1";
pub const SIGNAL_SCHEMA: &str = "happy-wakey.async-operation.signal.v1";
pub const REQUEST_SUBJECT: &str = "dd.remote.web_api.happy-wakey.request";
pub const RESPONSE_SUBJECT_PREFIX: &str = "happy-wakey.responses";
const OUTBOX_TTL: Duration = Duration::minutes(5);

pub async fn register(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AsyncOperationRequest>,
) -> Result<(StatusCode, Json<AsyncOperationAccepted>), ApiFailure> {
    if !state.async_operations_enabled {
        return Err(ApiFailure::ServiceUnavailable);
    }
    let operation_id = validate_request(&request)?;
    let identity = state.auth.authenticate(&headers).await?;
    let now = Utc::now();
    let transaction = state.db.begin().await?;
    async_operation::Entity::insert(async_operation::ActiveModel {
        operation_id: Set(operation_id),
        owner_id: Set(identity.subject.clone()),
        operation: Set(operation_name(request.operation).into()),
        status: Set("pending".into()),
        response: Set(None),
        expires_at: Set(now + OUTBOX_TTL),
        created_at: Set(now),
        completed_at: Set(None),
    })
    .on_conflict(
        OnConflict::column(async_operation::Column::OperationId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(&transaction)
    .await?;

    let existing = async_operation::Entity::find_by_id(operation_id)
        .lock_exclusive()
        .one(&transaction)
        .await?
        .ok_or_else(|| ApiFailure::Contract("async outbox claim disappeared".into()))?;
    if existing.owner_id != identity.subject
        || existing.operation != operation_name(request.operation)
        || existing.expires_at <= now
    {
        return Err(ApiFailure::Conflict(
            "operation_id was already used or has expired".into(),
        ));
    }
    transaction.commit().await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(AsyncOperationAccepted {
            schema: ACCEPTED_SCHEMA.into(),
            operation_id: operation_id.to_string(),
            response_subject: response_subject(operation_id),
        }),
    ))
}

pub async fn process_outbox(
    state: &AppState,
    operation_id: Uuid,
) -> Result<Option<ServiceOperationResponse>, ApiFailure> {
    let transaction = state.db.begin().await?;
    let Some(model) = async_operation::Entity::find_by_id(operation_id)
        .lock_exclusive()
        .one(&transaction)
        .await?
    else {
        transaction.commit().await?;
        return Ok(None);
    };

    if model.status == "completed" {
        let response = model
            .response
            .ok_or_else(|| ApiFailure::Contract("completed outbox row has no response".into()))?;
        let response = serde_json::from_value(response)
            .map_err(|_| ApiFailure::Contract("stored outbox response is invalid".into()))?;
        transaction.commit().await?;
        return Ok(Some(response));
    }
    if model.status != "pending" {
        return Err(ApiFailure::Contract(
            "outbox row has an unknown status".into(),
        ));
    }

    let response = if model.expires_at <= Utc::now() {
        operation::failure_response(&operation_id.to_string(), ApiFailure::ServiceUnavailable)
    } else {
        match model.operation.as_str() {
            "list_alarms" => {
                let alarms =
                    handlers::list_alarms_for_subject(&transaction, &model.owner_id).await?;
                operation::success_response(&operation_id.to_string(), alarms)
            }
            _ => {
                return Err(ApiFailure::Contract(
                    "outbox row has an unknown operation".into(),
                ));
            }
        }
    };

    let mut active: async_operation::ActiveModel = model.into();
    active.status = Set("completed".into());
    active.response = Set(Some(serde_json::to_value(&response).map_err(|_| {
        ApiFailure::Contract("outbox response could not serialize".into())
    })?));
    active.completed_at = Set(Some(Utc::now()));
    active.update(&transaction).await?;
    transaction.commit().await?;
    Ok(Some(response))
}

pub fn validate_request(request: &AsyncOperationRequest) -> Result<Uuid, ApiFailure> {
    if request.schema != REQUEST_SCHEMA {
        return Err(ApiFailure::Invalid(
            "unsupported async operation schema".into(),
        ));
    }
    Uuid::parse_str(&request.operation_id)
        .map_err(|_| ApiFailure::Invalid("operation_id must be a UUID".into()))
}

pub fn validate_signal(signal: &AsyncOperationSignal) -> Result<Uuid, ApiFailure> {
    if signal.schema != SIGNAL_SCHEMA {
        return Err(ApiFailure::Invalid(
            "unsupported async signal schema".into(),
        ));
    }
    Uuid::parse_str(&signal.operation_id)
        .map_err(|_| ApiFailure::Invalid("operation_id must be a UUID".into()))
}

pub fn response_subject(operation_id: Uuid) -> String {
    format!("{RESPONSE_SUBJECT_PREFIX}.{operation_id}")
}

fn operation_name(operation: ServiceOperation) -> &'static str {
    match operation {
        ServiceOperation::ListAlarms => "list_alarms",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> AsyncOperationRequest {
        AsyncOperationRequest {
            schema: REQUEST_SCHEMA.into(),
            operation_id: "018f5cc6-6d8b-7b2a-9f38-269e6a7b1f11".into(),
            operation: ServiceOperation::ListAlarms,
        }
    }

    #[test]
    fn async_contract_never_contains_a_bearer_or_owner() {
        let request = serde_json::to_value(request()).unwrap();
        let signal = serde_json::to_value(AsyncOperationSignal {
            schema: SIGNAL_SCHEMA.into(),
            operation_id: "018f5cc6-6d8b-7b2a-9f38-269e6a7b1f11".into(),
        })
        .unwrap();
        for value in [request, signal] {
            let object = value.as_object().unwrap();
            assert!(!object.contains_key("bearer_token"));
            assert!(!object.contains_key("owner_id"));
            assert!(!value.to_string().contains("token"));
        }
    }

    #[test]
    fn request_signal_and_response_subject_are_exact() {
        let operation_id = validate_request(&request()).unwrap();
        assert_eq!(
            response_subject(operation_id),
            "happy-wakey.responses.018f5cc6-6d8b-7b2a-9f38-269e6a7b1f11"
        );
        assert!(validate_signal(&AsyncOperationSignal {
            schema: SIGNAL_SCHEMA.into(),
            operation_id: operation_id.to_string(),
        })
        .is_ok());

        let mut wrong = request();
        wrong.schema = "happy-wakey.async-operation.request.v2".into();
        assert!(validate_request(&wrong).is_err());
    }
}
