//! Configuration, read from environment variables (defaults in brackets).
//!
//! Storage
//!   S3_BUCKET                    required
//!   S3_ENDPOINT                  e.g. http://127.0.0.1:9000 for MinIO (enables path-style); unset = AWS
//!   S3_CREATE_BUCKET             "1" = create the bucket at startup if it is missing
//!   AWS_REGION [us-east-1], AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
//!   S3_CONNECT_TIMEOUT_SECS [5]  S3_OP_TIMEOUT_SECS [120] (per attempt)
//!
//! Infrastructure (read by ferrumec)
//!   DATABASE_URL                 Postgres, [postgres://postgres:postgres@localhost:5432/postgres]
//!   OTEL_LOGS_ENDPOINT / OTEL_METRICS_ENDPOINT / OTEL_TRACES_ENDPOINT   OTLP/HTTP collectors
//!
//! Authentication
//!   SESSION_COOKIE [session]     cookie holding the session id (see viewset::web::SessionMiddleware)
//!
//! Limits
//!   MAX_UPLOAD_BYTES [100 MiB]       largest accepted object
//!   PART_SIZE_BYTES [8 MiB]          multipart part size (min 5 MiB); memory per upload
//!   MAX_CONCURRENT_UPLOADS [8]       global cap; excess requests get 503
//!   MAX_CONCURRENT_DOWNLOADS [32]    global cap; excess requests get 503
//!   MAX_UPLOADS_PER_USER [2]         per user; excess requests get 429
//!   UPLOAD_IDLE_TIMEOUT_SECS [30]    max gap between body chunks
//!   UPLOAD_TOTAL_TIMEOUT_SECS [600]  max duration of one upload
//!   DOWNLOAD_IDLE_TIMEOUT_SECS [30]  max stall while streaming a download
//!
//! Quotas (per authenticated user)
//!   USER_STORAGE_QUOTA_BYTES [5 GiB]     total stored bytes (persistent, from the database)
//!   USER_UPLOAD_QUOTA_BYTES [1 GiB]      bytes uploaded per window (in memory)
//!   USER_DOWNLOAD_QUOTA_BYTES [5 GiB]    bytes downloaded per window (in memory)
//!   QUOTA_WINDOW_SECS [3600]

use std::{env, fmt::Display, str::FromStr, time::Duration};

pub const MIB: u64 = 1024 * 1024;
const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // S3 minimum for all but the last part
const MAX_PARTS: u64 = 10_000; // S3 maximum number of parts

#[derive(Clone)]
pub struct Config {
    pub max_upload_bytes: u64,
    pub part_size: usize,
    pub max_concurrent_uploads: usize,
    pub max_concurrent_downloads: usize,
    pub max_uploads_per_user: u32,
    pub upload_idle_timeout: Duration,
    pub upload_total_timeout: Duration,
    pub download_idle_timeout: Duration,
    pub user_storage_quota: u64,
    pub user_upload_quota: u64,
    pub user_download_quota: u64,
    pub quota_window: Duration,
}

pub fn env_parse<T>(name: &str, default: T) -> Result<T, String>
where
    T: FromStr,
    T::Err: Display,
{
    match env::var(name) {
        Ok(v) => v.trim().parse().map_err(|e| format!("{name}: {e}")),
        Err(_) => Ok(default),
    }
}

pub fn env_secs(name: &str, default: u64) -> Result<Duration, String> {
    env_parse(name, default).map(Duration::from_secs)
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let cfg = Config {
            max_upload_bytes: env_parse("MAX_UPLOAD_BYTES", 100 * MIB)?,
            part_size: env_parse("PART_SIZE_BYTES", 8 * 1024 * 1024_usize)?,
            max_concurrent_uploads: env_parse("MAX_CONCURRENT_UPLOADS", 8)?,
            max_concurrent_downloads: env_parse("MAX_CONCURRENT_DOWNLOADS", 32)?,
            max_uploads_per_user: env_parse("MAX_UPLOADS_PER_USER", 2)?,
            upload_idle_timeout: env_secs("UPLOAD_IDLE_TIMEOUT_SECS", 30)?,
            upload_total_timeout: env_secs("UPLOAD_TOTAL_TIMEOUT_SECS", 600)?,
            download_idle_timeout: env_secs("DOWNLOAD_IDLE_TIMEOUT_SECS", 30)?,
            user_storage_quota: env_parse("USER_STORAGE_QUOTA_BYTES", 5 * 1024 * MIB)?,
            user_upload_quota: env_parse("USER_UPLOAD_QUOTA_BYTES", 1024 * MIB)?,
            user_download_quota: env_parse("USER_DOWNLOAD_QUOTA_BYTES", 5 * 1024 * MIB)?,
            quota_window: env_secs("QUOTA_WINDOW_SECS", 3600)?,
        };
        if cfg.part_size < MIN_PART_SIZE {
            return Err(format!("PART_SIZE_BYTES must be at least {MIN_PART_SIZE}"));
        }
        if cfg.max_upload_bytes.div_ceil(cfg.part_size as u64) > MAX_PARTS {
            return Err(format!(
                "MAX_UPLOAD_BYTES / PART_SIZE_BYTES exceeds the S3 limit of {MAX_PARTS} parts; \
                 raise PART_SIZE_BYTES"
            ));
        }
        Ok(cfg)
    }
}
