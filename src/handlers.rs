//! HTTP handlers. Every handler takes a [`Principal`], so none of them can run for an
//! unauthenticated or unauthorized caller; files are always scoped to their owner.

use std::{collections::HashMap, io, sync::Arc, time::Duration};

use actix_web::{
    HttpRequest, HttpResponse, ResponseError,
    http::{
        StatusCode,
        header::{self, HeaderValue},
    },
    web,
};
use aws_sdk_s3::{
    error::DisplayErrorContext, operation::get_object::GetObjectError, primitives::ByteStream,
};
use percent_encoding::utf8_percent_encode;
use serde::Deserialize;
use tokio::sync::OwnedSemaphorePermit;
use tracing::{error, info, warn};
use uuid::Uuid;
use viewset::{ApiError, Page, PaginationParams, Repository};

use crate::{
    events::{self, FileDeleted, FileUploaded},
    limits::{Denied, UserLimits},
    metrics, model,
    model::{FILENAME_SET, FileRecord, FileView, NewFile},
    responses::{json_error, retry_later, server_busy},
    s3_upload::{self, ObjectMeta, UploadError, stream_to_s3},
    state::AppState,
};

fn safe_filename(input: Option<&str>) -> String {
    let raw = input.unwrap_or("upload.bin");
    let leaf = raw.rsplit(['/', '\\']).next().unwrap_or("upload.bin");
    let cleaned: String = leaf
        .chars()
        .filter(|c| !c.is_control() && *c != '\0')
        .take(180)
        .collect();
    if cleaned.trim().is_empty() {
        "upload.bin".to_string()
    } else {
        cleaned
    }
}

/// Load a file the caller owns. Someone else's file looks exactly like a missing one,
/// so ids cannot be probed.
async fn load_owned(state: &AppState, user: Uuid, id: Uuid) -> Result<FileRecord, HttpResponse> {
    match state.repo.retrieve(&id).await {
        Ok(record) if record.owner_id == user => Ok(record),
        Ok(_) | Err(sqlx::Error::RowNotFound) => Err(ApiError::NotFound.error_response()),
        Err(e) => Err(ApiError::from(e).error_response()),
    }
}

// ---------------------------------------------------------------------------
// PUT /v1/files?filename=...
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct UploadQuery {
    /// Optional display name. Never used as the S3 object key.
    filename: Option<String>,
}

pub async fn upload(
    req: HttpRequest,
    principal: viewset::web::Identity,
    query: web::Query<UploadQuery>,
    mut payload: web::Payload,
    state: web::Data<AppState>,
) -> HttpResponse {
    let user = principal.sub;
    let metrics = metrics::get();

    // Cheap rejections first: they cost nothing and read no body.
    let declared_len = match req.headers().get(header::CONTENT_LENGTH) {
        None => 0,
        Some(v) => match v.to_str().ok().and_then(|v| v.parse::<u64>().ok()) {
            Some(n) if n > state.cfg.max_upload_bytes => {
                metrics.upload("rejected");
                return UploadError::TooLarge.into_response();
            }
            Some(n) => n,
            None => {
                metrics.upload("rejected");
                return json_error(StatusCode::BAD_REQUEST, "invalid Content-Length");
            }
        },
    };

    let Ok(_permit) = Arc::clone(&state.upload_permits).try_acquire_owned() else {
        metrics.upload("rejected");
        return server_busy();
    };
    let _slot = match state.limits.begin_upload(user, declared_len) {
        Ok(slot) => slot,
        Err(denied) => {
            metrics.upload("rejected");
            return match denied {
                Denied::Busy => retry_later(
                    StatusCode::TOO_MANY_REQUESTS,
                    "too many concurrent uploads",
                    5,
                ),
                Denied::Quota { retry_after } => retry_later(
                    StatusCode::TOO_MANY_REQUESTS,
                    "upload quota exceeded",
                    retry_after,
                ),
            };
        }
    };

    // Persistent per-user storage quota, from the database.
    let used = match model::storage_used(&state.db, user).await {
        Ok(used) => used,
        Err(e) => {
            metrics.upload("failed");
            return ApiError::from(e).error_response();
        }
    };
    let remaining = state.cfg.user_storage_quota.saturating_sub(used);
    if remaining == 0 || declared_len > remaining {
        metrics.upload("rejected");
        return UploadError::StorageQuota.into_response();
    }

    let filename = safe_filename(query.filename.as_deref());
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 255)
        .unwrap_or("application/octet-stream")
        .to_string();
    let key = format!("uploads/{}", Uuid::new_v4());
    let meta = ObjectMeta {
        key: &key,
        content_type: &content_type,
        filename: &filename,
    };

    let result = tokio::time::timeout(
        state.cfg.upload_total_timeout,
        stream_to_s3(&state, user, &meta, &mut payload, remaining),
    )
    .await
    .unwrap_or(Err(UploadError::TotalTimeout));

    let size = match result {
        Ok(size) => size,
        Err(e) => {
            metrics.upload("failed");
            return e.into_response();
        }
    };

    // Record who owns the object. If this fails, remove the object again so nothing is
    // stored that no one can see or delete.
    let record = match state
        .repo
        .create(NewFile {
            owner_id: user,
            object_key: key.clone(),
            filename,
            content_type,
            size_bytes: size as i64,
        })
        .await
    {
        Ok(record) => record,
        Err(e) => {
            error!(%key, error = %e, "failed to record upload; removing object");
            s3_upload::delete_object(&state, &key).await;
            metrics.upload("failed");
            return ApiError::from(e).error_response();
        }
    };

    metrics.upload("ok");
    metrics.upload_bytes(size);
    info!(%user, file = %record.id, size_bytes = size, "upload complete");
    events::publish(
        &state.events,
        FileUploaded {
            id: record.id,
            owner_id: user,
            filename: record.filename.clone(),
            content_type: record.content_type.clone(),
            size_bytes: record.size_bytes,
        },
        user,
    )
    .await;

    HttpResponse::Created().json(FileView::from(&record))
}

// ---------------------------------------------------------------------------
// GET /v1/files            (the caller's files, paginated)
// GET /v1/files/{id}       (metadata)
// ---------------------------------------------------------------------------

fn query_u32(q: &HashMap<String, String>, key: &str, default: u32) -> u32 {
    q.get(key).and_then(|v| v.parse().ok()).unwrap_or(default)
}

pub async fn list(
    principal: viewset::web::Identity,
    query: web::Query<HashMap<String, String>>,
    state: web::Data<AppState>,
) -> HttpResponse {
    // Build the filter set ourselves: only known keys are passed on, paging values are
    // clamped, and the owner filter is forced to the caller.
    let mut filters = viewset::Filters::new();
    filters.insert(
        "page".into(),
        query_u32(&query, "page", 1).clamp(1, 1_000_000).to_string(),
    );
    filters.insert(
        "page_size".into(),
        query_u32(&query, "page_size", 25).clamp(1, 200).to_string(),
    );
    for key in ["sort", "content_type"] {
        if let Some(value) = query.get(key) {
            filters.insert(key.into(), value.clone());
        }
    }
    filters.insert("owner_id".into(), principal.sub.to_string());

    match state.repo.list(&filters).await {
        Ok((records, total)) => {
            let pagination = PaginationParams::from_query(&filters);
            let items: Vec<FileView> = records.iter().map(FileView::from).collect();
            HttpResponse::Ok().json(Page::new(items, &pagination, total))
        }
        Err(e) => ApiError::from(e).error_response(),
    }
}

pub async fn metadata(
    principal: viewset::web::Identity,
    path: web::Path<Uuid>,
    state: web::Data<AppState>,
) -> HttpResponse {
    match load_owned(&state, principal.sub, path.into_inner()).await {
        Ok(record) => HttpResponse::Ok().json(FileView::from(&record)),
        Err(resp) => resp,
    }
}

// ---------------------------------------------------------------------------
// GET /v1/files/{id}/download
// ---------------------------------------------------------------------------

struct DownloadState {
    body: ByteStream,
    limits: Arc<UserLimits>,
    user: Uuid,
    idle: Duration,
    _permit: OwnedSemaphorePermit, // held until the response body is dropped
}

pub async fn download(
    principal: viewset::web::Identity,
    path: web::Path<Uuid>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let user = principal.sub;
    let metrics = metrics::get();

    let record = match load_owned(&state, user, path.into_inner()).await {
        Ok(record) => record,
        Err(resp) => {
            metrics.download("not_found");
            return resp;
        }
    };

    let Ok(permit) = Arc::clone(&state.download_permits).try_acquire_owned() else {
        metrics.download("rejected");
        return server_busy();
    };

    let output = match state
        .s3
        .get_object()
        .bucket(&*state.bucket)
        .key(&record.object_key)
        .send()
        .await
    {
        Ok(output) => output,
        Err(err) => {
            if err
                .as_service_error()
                .is_some_and(GetObjectError::is_no_such_key)
            {
                error!(file = %record.id, key = %record.object_key, "metadata row exists but the object is missing");
                metrics.download("not_found");
                return json_error(StatusCode::NOT_FOUND, "file not found");
            }
            error!(error = %DisplayErrorContext(&err), "S3 download failed");
            metrics.download("failed");
            return json_error(StatusCode::BAD_GATEWAY, "storage download failed");
        }
    };

    if let Err(retry_after) = state
        .limits
        .download_allowed(user, record.size_bytes.max(0) as u64)
    {
        metrics.download("rejected");
        return retry_later(
            StatusCode::TOO_MANY_REQUESTS,
            "download quota exceeded",
            retry_after,
        );
    }

    let stream = futures_util::stream::try_unfold(
        DownloadState {
            body: output.body,
            limits: Arc::clone(&state.limits),
            user,
            idle: state.cfg.download_idle_timeout,
            _permit: permit,
        },
        |mut st| async move {
            match tokio::time::timeout(st.idle, st.body.try_next()).await {
                Ok(Ok(Some(chunk))) => {
                    if st
                        .limits
                        .charge_download(st.user, chunk.len() as u64)
                        .is_err()
                    {
                        return Err(io::Error::other("download quota exceeded"));
                    }
                    Ok(Some((chunk, st)))
                }
                Ok(Ok(None)) => Ok(None),
                Ok(Err(e)) => Err(io::Error::other(e)),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "storage stalled while streaming download",
                )),
            }
        },
    );

    let content_type = HeaderValue::from_str(&record.content_type)
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
    let disposition = HeaderValue::from_str(&format!(
        "attachment; filename*=UTF-8''{}",
        utf8_percent_encode(&record.filename, FILENAME_SET)
    ))
    .unwrap_or_else(|_| HeaderValue::from_static("attachment"));

    metrics.download("ok");
    let mut resp = HttpResponse::Ok();
    resp.insert_header((header::CONTENT_TYPE, content_type))
        .insert_header((header::CONTENT_DISPOSITION, disposition))
        .insert_header((header::X_CONTENT_TYPE_OPTIONS, "nosniff"))
        .no_chunking(record.size_bytes.max(0) as u64);
    resp.streaming(stream)
}

// ---------------------------------------------------------------------------
// DELETE /v1/files/{id}
// ---------------------------------------------------------------------------

pub async fn delete(
    principal: viewset::web::Identity,
    path: web::Path<Uuid>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let user = principal.sub;
    let record = match load_owned(&state, user, path.into_inner()).await {
        Ok(record) => record,
        Err(resp) => return resp,
    };

    // Object first, row second: if the second step fails the request can simply be
    // retried (deleting a missing object succeeds), whereas the other order would
    // leave an object nobody can reach.
    if !s3_upload::delete_object(&state, &record.object_key).await {
        return json_error(StatusCode::BAD_GATEWAY, "storage delete failed");
    }
    match state.repo.delete(&record.id).await {
        Ok(()) | Err(sqlx::Error::RowNotFound) => {}
        Err(e) => return ApiError::from(e).error_response(),
    }
    // viewset's `delete` clears the item cache but not cached list pages.
    if let Err(e) = state.repo.list_cache().clear().await {
        warn!(error = %e, "failed to clear the list cache after delete");
    }

    events::publish(
        &state.events,
        FileDeleted {
            id: record.id,
            owner_id: user,
        },
        user,
    )
    .await;
    HttpResponse::NoContent().finish()
}

#[cfg(test)]
mod tests {
    use super::safe_filename;

    #[test]
    fn filenames_are_reduced_to_a_safe_leaf() {
        assert_eq!(safe_filename(Some("../../etc/passwd")), "passwd");
        assert_eq!(safe_filename(Some("C:\\temp\\a.txt")), "a.txt");
        assert_eq!(safe_filename(Some("  ")), "upload.bin");
        assert_eq!(safe_filename(None), "upload.bin");
    }
}
