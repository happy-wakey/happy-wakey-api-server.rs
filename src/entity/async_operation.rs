use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "happy_wakey_async_operations")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub operation_id: Uuid,
    pub owner_id: String,
    pub operation: String,
    pub status: String,
    pub response: Option<Json>,
    pub expires_at: DateTimeUtc,
    pub created_at: DateTimeUtc,
    pub completed_at: Option<DateTimeUtc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
