use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "happy_wakey_transition_receipts")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub transition_id: Uuid,
    pub owner_id: String,
    pub occurrence_id: Option<Uuid>,
    pub request_hash: String,
    pub disposition: String,
    pub response: Json,
    pub created_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
