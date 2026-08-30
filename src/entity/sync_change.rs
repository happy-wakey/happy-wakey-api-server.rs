use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "happy_wakey_sync_changes")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub sequence: i64,
    pub change_id: Uuid,
    pub scope: String,
    pub collection: String,
    pub entity_id: Uuid,
    pub operation: String,
    pub generation: i64,
    pub actor_id: String,
    pub document: Option<Json>,
    pub occurred_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
