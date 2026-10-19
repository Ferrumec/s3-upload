use std::sync::Arc;

use aws_sdk_s3::Client as S3Client;
use ferrumec::EventStream;
use sqlx::PgPool;
use tokio::sync::Semaphore;
use viewset::DefaultRepo;

use crate::{config::Config, limits::UserLimits, model::FileRecord};

/// Everything the handlers share. Built once in `FilesModule::new`.
pub struct AppState {
    pub s3: S3Client,
    pub bucket: Arc<str>,
    pub cfg: Config,
    pub db: PgPool,
    /// File metadata repository (viewset): typed queries plus ferrumec-cache-backed reads.
    pub repo: DefaultRepo<FileRecord>,
    pub events: Arc<dyn EventStream>,
    pub upload_permits: Arc<Semaphore>,
    pub download_permits: Arc<Semaphore>,
    pub limits: Arc<UserLimits>,
}
