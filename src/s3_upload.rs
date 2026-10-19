//! S3 access: client setup, and streaming an upload into S3 one part at a time.

use std::env;

use actix_web::{HttpResponse, http::StatusCode, web};
use aws_sdk_s3::{
    Client as S3Client,
    config::{
        Builder as S3ConfigBuilder, RequestChecksumCalculation, ResponseChecksumValidation,
        timeout::TimeoutConfig,
    },
    error::DisplayErrorContext,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use percent_encoding::utf8_percent_encode;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    config::env_secs,
    model::FILENAME_SET,
    responses::{json_error, retry_later},
    state::AppState,
};

// ---------------------------------------------------------------------------
// Client setup
// ---------------------------------------------------------------------------

pub async fn build_client() -> Result<S3Client, String> {
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region))
        .load()
        .await;

    let mut builder = S3ConfigBuilder::from(&aws_config)
        // Some S3-compatible servers reject the SDK's default trailing checksums.
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(env_secs("S3_CONNECT_TIMEOUT_SECS", 5)?)
                .operation_attempt_timeout(env_secs("S3_OP_TIMEOUT_SECS", 120)?)
                .build(),
        );
    if let Ok(endpoint) = env::var("S3_ENDPOINT") {
        builder = builder.endpoint_url(endpoint).force_path_style(true);
    }
    Ok(S3Client::from_conf(builder.build()))
}

pub async fn ensure_bucket(s3: &S3Client, bucket: &str) {
    if s3.head_bucket().bucket(bucket).send().await.is_ok() {
        return;
    }
    if env::var("S3_CREATE_BUCKET").is_ok_and(|v| v == "1") {
        match s3.create_bucket().bucket(bucket).send().await {
            Ok(_) => info!(%bucket, "created bucket"),
            Err(e) => warn!(%bucket, error = %DisplayErrorContext(&e), "could not create bucket"),
        }
    } else {
        warn!(%bucket, "bucket not reachable at startup; uploads fail until it exists (S3_CREATE_BUCKET=1 creates it)");
    }
}

/// Best-effort removal of an object (used to undo an upload whose metadata insert failed).
pub async fn delete_object(state: &AppState, key: &str) -> bool {
    match state
        .s3
        .delete_object()
        .bucket(&*state.bucket)
        .key(key)
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            error!(%key, error = %DisplayErrorContext(&e), "failed to delete S3 object");
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Upload errors
// ---------------------------------------------------------------------------

pub enum UploadError {
    TooLarge,
    StorageQuota,
    RateQuota { retry_after: u64 },
    IdleTimeout,
    TotalTimeout,
    ClientBody(String),
    Storage,
}

impl UploadError {
    pub fn into_response(self) -> HttpResponse {
        match self {
            UploadError::TooLarge => json_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "upload exceeds configured size limit",
            ),
            UploadError::StorageQuota => json_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "upload would exceed your storage quota",
            ),
            UploadError::RateQuota { retry_after } => retry_later(
                StatusCode::TOO_MANY_REQUESTS,
                "upload quota exceeded",
                retry_after,
            ),
            UploadError::IdleTimeout => json_error(
                StatusCode::REQUEST_TIMEOUT,
                "timed out waiting for upload data",
            ),
            UploadError::TotalTimeout => {
                json_error(StatusCode::REQUEST_TIMEOUT, "upload took too long")
            }
            UploadError::ClientBody(msg) => {
                warn!(error = %msg, "failed to read upload body");
                json_error(StatusCode::BAD_REQUEST, "failed to read upload body")
            }
            UploadError::Storage => json_error(StatusCode::BAD_GATEWAY, "storage upload failed"),
        }
    }
}

fn storage_err(op: &'static str, e: impl std::error::Error) -> UploadError {
    error!(op, error = %DisplayErrorContext(&e), "S3 operation failed");
    UploadError::Storage
}

// ---------------------------------------------------------------------------
// Streaming upload
// ---------------------------------------------------------------------------

/// Aborts an in-progress multipart upload when dropped while still armed.
/// This runs on every early exit, including the client disconnecting (actix drops
/// the handler future), so half-uploaded parts are not left behind in the bucket.
struct AbortGuard {
    s3: S3Client,
    bucket: String,
    key: String,
    upload_id: String,
    armed: bool,
}

impl Drop for AbortGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!(key = %self.key, "no runtime to abort multipart upload; relies on bucket lifecycle cleanup");
            return;
        };
        let (s3, bucket, key, upload_id) = (
            self.s3.clone(),
            self.bucket.clone(),
            self.key.clone(),
            self.upload_id.clone(),
        );
        handle.spawn(async move {
            if let Err(e) = s3
                .abort_multipart_upload()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .send()
                .await
            {
                warn!(%key, error = %DisplayErrorContext(&e), "failed to abort multipart upload");
            }
        });
    }
}

struct Multipart {
    guard: AbortGuard,
    parts: Vec<CompletedPart>,
}

pub struct ObjectMeta<'a> {
    pub key: &'a str,
    pub content_type: &'a str,
    pub filename: &'a str,
}

impl ObjectMeta<'_> {
    fn encoded_filename(&self) -> String {
        utf8_percent_encode(self.filename, FILENAME_SET).to_string()
    }
}

/// Upload one full part, starting the multipart upload first if this is part 1.
async fn upload_next_part(
    state: &AppState,
    mp: &mut Option<Multipart>,
    meta: &ObjectMeta<'_>,
    data: Bytes,
) -> Result<(), UploadError> {
    if mp.is_none() {
        let out = state
            .s3
            .create_multipart_upload()
            .bucket(&*state.bucket)
            .key(meta.key)
            .content_type(meta.content_type)
            .metadata("original-filename", meta.encoded_filename())
            .send()
            .await
            .map_err(|e| storage_err("create_multipart_upload", e))?;
        let Some(upload_id) = out.upload_id() else {
            error!("S3 returned no upload id");
            return Err(UploadError::Storage);
        };
        *mp = Some(Multipart {
            guard: AbortGuard {
                s3: state.s3.clone(),
                bucket: state.bucket.to_string(),
                key: meta.key.to_string(),
                upload_id: upload_id.to_string(),
                armed: true,
            },
            parts: Vec::new(),
        });
    }
    let m = mp.as_mut().expect("multipart initialised above");
    let part_number = (m.parts.len() + 1) as i32;
    let out = state
        .s3
        .upload_part()
        .bucket(&*state.bucket)
        .key(meta.key)
        .upload_id(&m.guard.upload_id)
        .part_number(part_number)
        .body(ByteStream::from(data))
        .send()
        .await
        .map_err(|e| storage_err("upload_part", e))?;
    m.parts.push(
        CompletedPart::builder()
            .part_number(part_number)
            .set_e_tag(out.e_tag().map(str::to_owned))
            .build(),
    );
    Ok(())
}

/// Stream the request body into S3 and return the exact number of bytes stored.
///
/// Memory use is at most one part (`cfg.part_size`) plus one network chunk.
/// `remaining_storage` is what is left of the user's storage quota; the upload is cut
/// off as soon as it would pass either that or `cfg.max_upload_bytes`.
pub async fn stream_to_s3(
    state: &AppState,
    user: Uuid,
    meta: &ObjectMeta<'_>,
    payload: &mut web::Payload,
    remaining_storage: u64,
) -> Result<u64, UploadError> {
    let cfg = &state.cfg;
    let part_size = cfg.part_size;
    let hard_limit = cfg.max_upload_bytes.min(remaining_storage);
    let mut buf = BytesMut::with_capacity(part_size);
    let mut mp: Option<Multipart> = None;
    let mut total: u64 = 0;

    loop {
        let next = tokio::time::timeout(cfg.upload_idle_timeout, payload.next())
            .await
            .map_err(|_| UploadError::IdleTimeout)?;
        let Some(chunk) = next else { break };
        let mut chunk = chunk.map_err(|e| UploadError::ClientBody(e.to_string()))?;

        let len = chunk.len() as u64;
        total += len;
        if total > hard_limit {
            return Err(if remaining_storage < cfg.max_upload_bytes {
                UploadError::StorageQuota
            } else {
                UploadError::TooLarge
            });
        }
        state
            .limits
            .charge_upload(user, len)
            .map_err(|retry_after| UploadError::RateQuota { retry_after })?;

        while !chunk.is_empty() {
            // A full buffer is flushed only once more data arrives, so a body that is
            // exactly one part long still takes the single-PutObject path below.
            if buf.len() == part_size {
                let full = std::mem::take(&mut buf).freeze();
                upload_next_part(state, &mut mp, meta, full).await?;
                buf = BytesMut::with_capacity(part_size);
            }
            let take = (part_size - buf.len()).min(chunk.len());
            buf.extend_from_slice(&chunk.split_to(take));
        }
    }

    if mp.is_none() {
        // Small object: one request.
        state
            .s3
            .put_object()
            .bucket(&*state.bucket)
            .key(meta.key)
            .content_type(meta.content_type)
            .metadata("original-filename", meta.encoded_filename())
            .body(ByteStream::from(buf.freeze()))
            .send()
            .await
            .map_err(|e| storage_err("put_object", e))?;
    } else {
        if !buf.is_empty() {
            upload_next_part(state, &mut mp, meta, buf.freeze()).await?;
        }
        let mut m = mp.take().expect("checked above");
        state
            .s3
            .complete_multipart_upload()
            .bucket(&*state.bucket)
            .key(meta.key)
            .upload_id(&m.guard.upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(std::mem::take(&mut m.parts)))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| storage_err("complete_multipart_upload", e))?;
        m.guard.armed = false; // finished: nothing to abort
    }

    Ok(total)
}
