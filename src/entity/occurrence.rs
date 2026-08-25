use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "happy_wakey_alarm_occurrences")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub alarm_id: Uuid,
    pub owner_id: String,
    pub scheduled_for: DateTimeUtc,
    pub state: String,
    pub generation: i64,
    pub active_transition_id: Option<Uuid>,
    pub snooze_until: Option<DateTimeUtc>,
    pub acknowledged_at: Option<DateTimeUtc>,
    pub completed_at: Option<DateTimeUtc>,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
