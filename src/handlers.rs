use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, Utc};
use happy_wakey_interfaces::{
    Alarm, AlarmOccurrence, AlarmOccurrenceState, AlarmTransitionEvent, ChangeOperation,
    CreateAlarmRequest, SyncChange, SyncEnvelope, TransitionAlarmRequest, TransitionAlarmResponse,
    TransitionDisposition,
};
use next_loggers::{json, Map};
use rust_decimal::prelude::ToPrimitive;
use sea_orm::{
    sea_query::OnConflict, ActiveModelTrait, ActiveValue::Set, ColumnTrait, ConnectionTrait,
    DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    entity::{alarm, occurrence, sync_change, transition_receipt},
    error::ApiFailure,
    reducer, scheduler, AppState,
};

pub async fn health(State(state): State<AppState>) -> Result<Json<Value>, ApiFailure> {
    state.db.ping().await?;
    Ok(Json(json!({"status":"ok"})))
}

pub async fn list_alarms(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<Alarm>>, ApiFailure> {
    let identity = state.auth.authenticate(&headers).await?;
    let alarms = list_alarms_for_subject(&state.db, &identity.subject).await?;
    emit(&state, "alarm.list", StatusCode::OK, false);
    Ok(Json(alarms))
}

pub(crate) async fn list_alarms_for_subject<C: ConnectionTrait>(
    db: &C,
    subject: &str,
) -> Result<Vec<Alarm>, ApiFailure> {
    let alarms = alarm::Entity::find()
        .filter(alarm::Column::OwnerId.eq(subject))
        .order_by_asc(alarm::Column::CreatedAt)
        .all(db)
        .await?
        .into_iter()
        .map(alarm_contract)
        .collect::<Result<_, _>>()?;
    Ok(alarms)
}

pub async fn create_alarm(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateAlarmRequest>,
) -> Result<impl IntoResponse, ApiFailure> {
    let identity = state.auth.authenticate(&headers).await?;
    validate_create(&request)?;
    let transition_id = parse_uuid("transition_id", &request.transition_id)?;
    let request_hash = hash(&request)?;
    let now = Utc::now();
    let local_time = scheduler::parse_local_time(&request.local_time)?;
    let scheduled_for = if request.enabled {
        Some(scheduler::next_occurrence(
            now,
            local_time,
            &request.time_zone,
            &request.weekdays,
        )?)
    } else {
        None
    };
    let transaction = state.db.begin().await?;
    let claim = claim_receipt(
        &transaction,
        transition_id,
        &identity.subject,
        None,
        &request_hash,
    )
    .await?;
    if !claim.claimed {
        let alarm: Alarm = serde_json::from_value(claim.model.response)
            .map_err(|error| ApiFailure::Contract(error.to_string()))?;
        transaction.commit().await?;
        emit(&state, "alarm.create.replay", StatusCode::CREATED, false);
        return Ok((StatusCode::CREATED, Json(alarm)));
    }

    let alarm_id = Uuid::new_v4();
    let tags = serde_json::to_value(&request.tags)
        .map_err(|error| ApiFailure::Invalid(error.to_string()))?;
    let model = alarm::ActiveModel {
        id: Set(alarm_id),
        owner_id: Set(identity.subject.clone()),
        label: Set(request.label.trim().to_owned()),
        local_time: Set(local_time),
        time_zone: Set(request.time_zone.clone()),
        weekdays: Set(serde_json::to_value(&request.weekdays)
            .map_err(|error| ApiFailure::Invalid(error.to_string()))?),
        enabled: Set(request.enabled),
        sound: Set(request.sound.clone()),
        volume: Set(rust_decimal::Decimal::try_from(request.volume as f64)
            .map_err(|_| ApiFailure::Invalid("volume is not representable".into()))?),
        gradual_seconds: Set(i32::try_from(request.gradual_seconds)
            .map_err(|_| ApiFailure::Invalid("gradual_seconds is too large".into()))?),
        tags: Set(tags),
        generation: Set(1),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(&transaction)
    .await?;

    if let Some(scheduled_for) = scheduled_for {
        occurrence::ActiveModel {
            id: Set(Uuid::new_v4()),
            alarm_id: Set(alarm_id),
            owner_id: Set(identity.subject.clone()),
            scheduled_for: Set(scheduled_for),
            state: Set("scheduled".into()),
            generation: Set(1),
            active_transition_id: Set(None),
            snooze_until: Set(None),
            acknowledged_at: Set(None),
            completed_at: Set(None),
            updated_at: Set(now),
        }
        .insert(&transaction)
        .await?;
    }

    let response = alarm_contract(model)?;
    finalize_receipt(
        claim.model,
        "applied",
        serde_json::to_value(&response).map_err(|error| ApiFailure::Contract(error.to_string()))?,
        &transaction,
    )
    .await?;
    transaction.commit().await?;
    emit(&state, "alarm.create", StatusCode::CREATED, false);
    Ok((StatusCode::CREATED, Json(response)))
}

pub async fn transition_occurrence(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(occurrence_id): Path<String>,
    Json(request): Json<TransitionAlarmRequest>,
) -> Result<Json<TransitionAlarmResponse>, ApiFailure> {
    let identity = state.auth.authenticate(&headers).await?;
    let occurrence_id = parse_uuid("occurrence_id", &occurrence_id)?;
    let transition_id = parse_uuid("transition_id", &request.transition_id)?;
    let request_hash = hash(&request)?;
    let snooze_until = request
        .snooze_until
        .as_deref()
        .map(parse_time)
        .transpose()?;
    if request.event != AlarmTransitionEvent::Snooze && snooze_until.is_some() {
        return Err(ApiFailure::Invalid(
            "snooze_until is only valid for a snooze transition".into(),
        ));
    }

    let transaction = state.db.begin().await?;
    let claim = claim_receipt(
        &transaction,
        transition_id,
        &identity.subject,
        Some(occurrence_id),
        &request_hash,
    )
    .await?;
    if !claim.claimed {
        let response = serde_json::from_value(claim.model.response)
            .map_err(|error| ApiFailure::Contract(error.to_string()))?;
        transaction.commit().await?;
        emit(
            &state,
            "occurrence.transition.replay",
            StatusCode::OK,
            false,
        );
        return Ok(Json(response));
    }

    let current = occurrence::Entity::find_by_id(occurrence_id)
        .filter(occurrence::Column::OwnerId.eq(&identity.subject))
        .lock_exclusive()
        .one(&transaction)
        .await?
        .ok_or(ApiFailure::NotFound)?;
    let current_state = parse_state(&current.state)?;
    let actual_generation = u64::try_from(current.generation)
        .map_err(|_| ApiFailure::Contract("negative occurrence generation".into()))?;
    let decision = reducer::decide(
        current_state,
        request.event,
        actual_generation,
        request.expected_generation,
        snooze_until.is_some(),
    );
    let now = Utc::now();
    let model = if decision.disposition == TransitionDisposition::Applied {
        let mut update: occurrence::ActiveModel = current.into();
        update.state = Set(state_name(decision.next).into());
        update.generation = Set(i64::try_from(actual_generation + 1)
            .map_err(|_| ApiFailure::Conflict("generation overflow".into()))?);
        update.active_transition_id = Set(Some(transition_id));
        update.updated_at = Set(now);
        match decision.next {
            AlarmOccurrenceState::Acknowledged => update.acknowledged_at = Set(Some(now)),
            AlarmOccurrenceState::Snoozed => update.snooze_until = Set(snooze_until),
            AlarmOccurrenceState::Completed => update.completed_at = Set(Some(now)),
            AlarmOccurrenceState::Firing => update.snooze_until = Set(None),
            _ => {}
        }
        update.update(&transaction).await?
    } else {
        current
    };
    let occurrence = occurrence_contract(model)?;
    let response = TransitionAlarmResponse {
        disposition: decision.disposition,
        occurrence,
        error: (decision.disposition == TransitionDisposition::Rejected).then(|| {
            happy_wakey_interfaces::ApiError {
                code: "transition_rejected".into(),
                message: "event is not valid from the current occurrence state".into(),
                retryable: false,
                trace_id: None,
            }
        }),
    };
    finalize_receipt(
        claim.model,
        disposition_name(decision.disposition),
        serde_json::to_value(&response).map_err(|error| ApiFailure::Contract(error.to_string()))?,
        &transaction,
    )
    .await?;
    transaction.commit().await?;
    emit(&state, "occurrence.transition", StatusCode::OK, false);
    Ok(Json(response))
}

#[derive(Deserialize)]
pub struct PullQuery {
    #[serde(default = "default_cursor")]
    cursor: String,
    #[serde(default = "default_limit")]
    limit: u64,
}

fn default_cursor() -> String {
    "0".into()
}

fn default_limit() -> u64 {
    100
}

pub async fn pull_changes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<PullQuery>,
) -> Result<Json<SyncEnvelope>, ApiFailure> {
    let identity = state.auth.authenticate(&headers).await?;
    let cursor: i64 = query
        .cursor
        .parse()
        .map_err(|_| ApiFailure::Invalid("cursor must be a non-negative integer".into()))?;
    if cursor < 0 || query.limit == 0 || query.limit > 500 {
        return Err(ApiFailure::Invalid(
            "cursor must be non-negative and limit must be 1..=500".into(),
        ));
    }
    let mut rows = sync_change::Entity::find()
        .filter(sync_change::Column::Scope.eq(&identity.subject))
        .filter(sync_change::Column::Sequence.gt(cursor))
        .order_by_asc(sync_change::Column::Sequence)
        .limit(Some(query.limit + 1))
        .all(&state.db)
        .await?;
    let has_more = rows.len() > query.limit as usize;
    rows.truncate(query.limit as usize);
    let next_cursor = rows.last().map(|row| row.sequence).unwrap_or(cursor);
    let changes = rows
        .into_iter()
        .map(change_contract)
        .collect::<Result<_, _>>()?;
    emit(&state, "sync.pull", StatusCode::OK, false);
    Ok(Json(SyncEnvelope {
        schema: "happy-wakey.sync-envelope.v1".into(),
        cursor: next_cursor.to_string(),
        changes,
        has_more,
    }))
}

pub async fn push_changes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(changes): Json<Vec<SyncChange>>,
) -> Result<Json<SyncEnvelope>, ApiFailure> {
    let identity = state.auth.authenticate(&headers).await?;
    if changes.len() > 500 {
        return Err(ApiFailure::Invalid(
            "at most 500 changes may be pushed".into(),
        ));
    }
    let transaction = state.db.begin().await?;
    for change in &changes {
        if change.scope != identity.subject || change.actor_id != identity.subject {
            return Err(ApiFailure::Invalid(
                "scope and actor_id must equal the Shared Auth subject".into(),
            ));
        }
        let active = sync_change::ActiveModel {
            sequence: sea_orm::ActiveValue::NotSet,
            change_id: Set(parse_uuid("change_id", &change.change_id)?),
            scope: Set(change.scope.clone()),
            collection: Set(change.collection.clone()),
            entity_id: Set(parse_uuid("entity_id", &change.entity_id)?),
            operation: Set(operation_name(change.operation).into()),
            generation: Set(i64::try_from(change.generation)
                .map_err(|_| ApiFailure::Invalid("generation is too large".into()))?),
            actor_id: Set(identity.subject.clone()),
            document: Set(change.document.clone()),
            occurred_at: Set(parse_time(&change.occurred_at)?),
        };
        sync_change::Entity::insert(active)
            .on_conflict(
                OnConflict::column(sync_change::Column::ChangeId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&transaction)
            .await?;
    }
    let cursor = sync_change::Entity::find()
        .filter(sync_change::Column::Scope.eq(&identity.subject))
        .order_by_desc(sync_change::Column::Sequence)
        .one(&transaction)
        .await?
        .map(|row| row.sequence)
        .unwrap_or(0);
    transaction.commit().await?;
    emit(&state, "sync.push", StatusCode::OK, false);
    Ok(Json(SyncEnvelope {
        schema: "happy-wakey.sync-envelope.v1".into(),
        cursor: cursor.to_string(),
        changes,
        has_more: false,
    }))
}

struct ReceiptClaim {
    model: transition_receipt::Model,
    claimed: bool,
}

async fn claim_receipt(
    transaction: &DatabaseTransaction,
    transition_id: Uuid,
    owner_id: &str,
    occurrence_id: Option<Uuid>,
    request_hash: &str,
) -> Result<ReceiptClaim, ApiFailure> {
    let insert = transition_receipt::ActiveModel {
        transition_id: Set(transition_id),
        owner_id: Set(owner_id.to_owned()),
        occurrence_id: Set(occurrence_id),
        request_hash: Set(request_hash.to_owned()),
        disposition: Set("pending".into()),
        response: Set(Value::Null),
        created_at: Set(Utc::now()),
    };
    let rows_affected = transition_receipt::Entity::insert(insert)
        .on_conflict(
            OnConflict::column(transition_receipt::Column::TransitionId)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(transaction)
        .await?;
    let model = transition_receipt::Entity::find_by_id(transition_id)
        .one(transaction)
        .await?
        .ok_or_else(|| ApiFailure::Contract("receipt claim disappeared".into()))?;
    if model.owner_id != owner_id
        || model.occurrence_id != occurrence_id
        || model.request_hash != request_hash
    {
        return Err(ApiFailure::Conflict(
            "transition_id was already used for a different request".into(),
        ));
    }
    if rows_affected == 0 && model.response.is_null() {
        return Err(ApiFailure::Conflict(
            "transition is already being processed; retry safely".into(),
        ));
    }
    Ok(ReceiptClaim {
        model,
        claimed: rows_affected == 1,
    })
}

async fn finalize_receipt(
    model: transition_receipt::Model,
    disposition: &str,
    response: Value,
    transaction: &DatabaseTransaction,
) -> Result<(), ApiFailure> {
    let mut active: transition_receipt::ActiveModel = model.into();
    active.disposition = Set(disposition.into());
    active.response = Set(response);
    active.update(transaction).await?;
    Ok(())
}

fn validate_create(request: &CreateAlarmRequest) -> Result<(), ApiFailure> {
    if request.label.trim().is_empty() || request.label.chars().count() > 160 {
        return Err(ApiFailure::Invalid(
            "label must contain 1..=160 characters".into(),
        ));
    }
    if request.sound.trim().is_empty() || request.sound.len() > 128 {
        return Err(ApiFailure::Invalid(
            "sound must contain 1..=128 bytes".into(),
        ));
    }
    if !request.volume.is_finite() || !(0.0..=1.0).contains(&request.volume) {
        return Err(ApiFailure::Invalid("volume must be between 0 and 1".into()));
    }
    if request.tags.len() > 32 || request.tags.iter().any(|tag| tag.len() > 64) {
        return Err(ApiFailure::Invalid(
            "tags exceed the bounded contract".into(),
        ));
    }
    Ok(())
}

fn alarm_contract(model: alarm::Model) -> Result<Alarm, ApiFailure> {
    Ok(Alarm {
        id: model.id.to_string(),
        label: model.label,
        local_time: model.local_time.format("%H:%M:%S").to_string(),
        time_zone: model.time_zone,
        weekdays: serde_json::from_value(model.weekdays)
            .map_err(|error| ApiFailure::Contract(error.to_string()))?,
        enabled: model.enabled,
        sound: model.sound,
        volume: model
            .volume
            .to_f32()
            .ok_or_else(|| ApiFailure::Contract("volume is not f32".into()))?,
        gradual_seconds: u32::try_from(model.gradual_seconds)
            .map_err(|_| ApiFailure::Contract("negative gradual_seconds".into()))?,
        tags: serde_json::from_value(model.tags)
            .map_err(|error| ApiFailure::Contract(error.to_string()))?,
        generation: u64::try_from(model.generation)
            .map_err(|_| ApiFailure::Contract("negative alarm generation".into()))?,
        created_at: model.created_at.to_rfc3339(),
        updated_at: model.updated_at.to_rfc3339(),
    })
}

fn occurrence_contract(model: occurrence::Model) -> Result<AlarmOccurrence, ApiFailure> {
    Ok(AlarmOccurrence {
        id: model.id.to_string(),
        alarm_id: model.alarm_id.to_string(),
        scheduled_for: model.scheduled_for.to_rfc3339(),
        state: parse_state(&model.state)?,
        generation: u64::try_from(model.generation)
            .map_err(|_| ApiFailure::Contract("negative occurrence generation".into()))?,
        active_transition_id: model.active_transition_id.map(|id| id.to_string()),
        snooze_until: model.snooze_until.map(|value| value.to_rfc3339()),
        acknowledged_at: model.acknowledged_at.map(|value| value.to_rfc3339()),
        completed_at: model.completed_at.map(|value| value.to_rfc3339()),
        updated_at: model.updated_at.to_rfc3339(),
    })
}

fn change_contract(model: sync_change::Model) -> Result<SyncChange, ApiFailure> {
    Ok(SyncChange {
        change_id: model.change_id.to_string(),
        scope: model.scope,
        collection: model.collection,
        entity_id: model.entity_id.to_string(),
        operation: match model.operation.as_str() {
            "upsert" => ChangeOperation::Upsert,
            "delete" => ChangeOperation::Delete,
            other => return Err(ApiFailure::Contract(format!("unknown operation {other}"))),
        },
        generation: u64::try_from(model.generation)
            .map_err(|_| ApiFailure::Contract("negative sync generation".into()))?,
        actor_id: model.actor_id,
        document: model.document,
        occurred_at: model.occurred_at.to_rfc3339(),
    })
}

fn parse_state(value: &str) -> Result<AlarmOccurrenceState, ApiFailure> {
    match value {
        "scheduled" => Ok(AlarmOccurrenceState::Scheduled),
        "firing" => Ok(AlarmOccurrenceState::Firing),
        "acknowledged" => Ok(AlarmOccurrenceState::Acknowledged),
        "snoozed" => Ok(AlarmOccurrenceState::Snoozed),
        "completed" => Ok(AlarmOccurrenceState::Completed),
        "missed" => Ok(AlarmOccurrenceState::Missed),
        "canceled" => Ok(AlarmOccurrenceState::Canceled),
        other => Err(ApiFailure::Contract(format!(
            "unknown occurrence state {other}"
        ))),
    }
}

fn state_name(value: AlarmOccurrenceState) -> &'static str {
    match value {
        AlarmOccurrenceState::Scheduled => "scheduled",
        AlarmOccurrenceState::Firing => "firing",
        AlarmOccurrenceState::Acknowledged => "acknowledged",
        AlarmOccurrenceState::Snoozed => "snoozed",
        AlarmOccurrenceState::Completed => "completed",
        AlarmOccurrenceState::Missed => "missed",
        AlarmOccurrenceState::Canceled => "canceled",
    }
}

fn disposition_name(value: TransitionDisposition) -> &'static str {
    match value {
        TransitionDisposition::Applied => "applied",
        TransitionDisposition::Stale => "stale",
        TransitionDisposition::Rejected => "rejected",
    }
}

fn operation_name(value: ChangeOperation) -> &'static str {
    match value {
        ChangeOperation::Upsert => "upsert",
        ChangeOperation::Delete => "delete",
    }
}

fn parse_uuid(name: &str, value: &str) -> Result<Uuid, ApiFailure> {
    Uuid::parse_str(value).map_err(|_| ApiFailure::Invalid(format!("{name} must be a UUID")))
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, ApiFailure> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| ApiFailure::Invalid("timestamp must be RFC 3339".into()))
}

fn hash<T: Serialize>(value: &T) -> Result<String, ApiFailure> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| ApiFailure::Invalid(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn emit(state: &AppState, operation: &str, status: StatusCode, failed: bool) {
    let mut fields = Map::new();
    fields.insert("operation".into(), json!(operation));
    fields.insert("status".into(), json!(status.as_u16()));
    let event = if failed {
        state
            .telemetry
            .error(vec![json!("happy_wakey.api.request")])
    } else {
        state.telemetry.info(vec![json!("happy_wakey.api.request")])
    };
    let _ = event
        .add_fields(fields)
        .add_tags(["happy-wakey", "api"])
        .send();
}
