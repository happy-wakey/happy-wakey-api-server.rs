use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "happy_wakey_alarms")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub owner_id: String,
    pub label: String,
    pub local_time: Time,
    pub time_zone: String,
    pub weekdays: Json,
    pub enabled: bool,
    pub sound: String,
    pub volume: Decimal,
    pub gradual_seconds: i32,
    pub tags: Json,
    pub generation: i64,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
