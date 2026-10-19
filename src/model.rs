//! File metadata: one row per uploaded object, owned by one user.
//!
//! The entity is a `viewset::Entity`, so it gets `viewset::DefaultRepo` for free:
//! typed CRUD, filtering, sorting, pagination and caching through ferrumec's cache.

use chrono::{DateTime, Utc};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;
use viewset::Entity;

/// Characters left unescaped when a filename is stored as S3 metadata or sent in
/// `Content-Disposition` (everything else is percent-encoded, keeping it ASCII).
pub const FILENAME_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'.').remove(b'-').remove(b'_');

#[derive(Entity, FromRow, Serialize, Clone, Deserialize)]
#[entity(table = "uploaded_files", create = "NewFile")]
pub struct FileRecord {
    #[entity(skip_create)]
    pub id: Uuid,
    #[entity(sortable)]
    #[entity(skip_create)]
    pub created_at: DateTime<Utc>,
    #[entity(filterable)]
    pub owner_id: Uuid,
    pub object_key: String,
    #[entity(sortable)]
    pub filename: String,
    #[entity(filterable)]
    pub content_type: String,
    #[entity(sortable)]
    pub size_bytes: i64,
}

/// Columns supplied on insert (`id` and `created_at` come from database defaults).
#[derive(Clone, Serialize, Deserialize)]
pub struct NewFile {
    pub owner_id: Uuid,
    pub object_key: String,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
}

/// What clients see. The owner and the internal object key are not exposed.
#[derive(Serialize)]
pub struct FileView {
    pub id: Uuid,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub created_at: DateTime<Utc>,
}

impl From<&FileRecord> for FileView {
    fn from(r: &FileRecord) -> Self {
        Self {
            id: r.id,
            filename: r.filename.clone(),
            content_type: r.content_type.clone(),
            size_bytes: r.size_bytes,
            created_at: r.created_at,
        }
    }
}

const CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS uploaded_files (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    owner_id     UUID NOT NULL,
    object_key   TEXT NOT NULL UNIQUE,
    filename     TEXT NOT NULL,
    content_type TEXT NOT NULL,
    size_bytes   BIGINT NOT NULL CHECK (size_bytes >= 0)
)";

const CREATE_INDEX: &str = "CREATE INDEX IF NOT EXISTS uploaded_files_owner_created_idx
    ON uploaded_files (owner_id, created_at DESC)";

/// Creates the table if it does not exist (idempotent; safe to run on every start).
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(CREATE_TABLE).execute(pool).await?;
    sqlx::query(CREATE_INDEX).execute(pool).await?;
    Ok(())
}

/// Total bytes currently stored by `owner` (used for the persistent storage quota).
pub async fn storage_used(pool: &PgPool, owner: Uuid) -> Result<u64, sqlx::Error> {
    let used: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(size_bytes), 0)::BIGINT FROM uploaded_files WHERE owner_id = $1",
    )
    .bind(owner)
    .fetch_one(pool)
    .await?;
    Ok(used.max(0) as u64)
}
