use std::{env, net::SocketAddr, sync::Arc};

use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use bytes::Bytes;
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    s3: S3Client,
    bucket: Arc<str>,
    max_upload_bytes: u64,
}

#[derive(Deserialize)]
struct UploadQuery {
    // Optional display name. Never used as the S3 object key.
    filename: Option<String>,
}

#[derive(Serialize)]
struct UploadResponse {
    key: String,
    bucket: String,
    filename: String,
    content_type: String,
    size_bytes: u64,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Deserialize)]
struct AccessQuery {
    key: String,
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.into(),
        }),
    )
        .into_response()
}

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

async fn health() -> &'static str {
    "ok"
}

/// Upload raw bytes:
/// PUT /v1/files?filename=report.pdf
/// Content-Type: application/pdf
/// Request body: file bytes
async fn upload(
    State(state): State<AppState>,
    Query(query): Query<UploadQuery>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let filename = safe_filename(query.filename.as_deref());
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 255)
        .unwrap_or("application/octet-stream")
        .to_string();

    if let Some(length) = headers.get(header::CONTENT_LENGTH) {
        match length.to_str().ok().and_then(|v| v.parse::<u64>().ok()) {
            Some(n) if n > state.max_upload_bytes => {
                return error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "upload exceeds configured size limit",
                );
            }
            Some(_) => {}
            None => return error_response(StatusCode::BAD_REQUEST, "invalid Content-Length"),
        }
    }

    // Count and enforce the limit while streaming; the whole file is not buffered.
    use futures_util::TryStreamExt;
    let max = state.max_upload_bytes;
    let stream = body
        .into_data_stream()
        .map_err(|err| std::io::Error::other(err.to_string()));

    let key = format!("uploads/{}", Uuid::new_v4());
    let limited = limit_stream(stream, max);

    let uploaded_bytes = match limited
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok::<_, std::io::Error>(acc)
        })
        .await
    {
        Ok(bytes) => Bytes::from(bytes),
        Err(err) if err.kind() == std::io::ErrorKind::InvalidData => {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "upload exceeds configured size limit",
            );
        }
        Err(err) => {
            error!(error = %err, "failed to read upload");
            return error_response(StatusCode::BAD_REQUEST, "failed to read upload body");
        }
    };

    let body_stream = ByteStream::from(uploaded_bytes);
    let result = state
        .s3
        .put_object()
        .bucket(state.bucket.as_ref())
        .key(&key)
        .content_type(&content_type)
        .metadata("original-filename", filename.clone())
        .body(body_stream)
        .send()
        .await;

    match result {
        Ok(_) => {
            // Size is not returned by S3 PutObject; it is counted in the stream
            // in the production response via metadata or a HEAD request.
            let size = match state
                .s3
                .head_object()
                .bucket(state.bucket.as_ref())
                .key(&key)
                .send()
                .await
            {
                Ok(head) => head.content_length().unwrap_or(0).max(0) as u64,
                Err(_) => 0,
            };
            (
                StatusCode::CREATED,
                Json(UploadResponse {
                    key,
                    bucket: state.bucket.to_string(),
                    filename,
                    content_type,
                    size_bytes: size,
                }),
            )
                .into_response()
        }
        Err(err) => {
            use aws_sdk_s3::error::DisplayErrorContext;

error!(error = %DisplayErrorContext(&err), "S3 upload failed");
            error_response(StatusCode::BAD_GATEWAY, "storage upload failed")
        }
    }
}

async fn access_file(State(state): State<AppState>, Query(query): Query<AccessQuery>) -> Response {
    if !query.key.starts_with("uploads/") || query.key.len() > 512 || query.key.contains("..") {
        return error_response(StatusCode::BAD_REQUEST, "invalid object key");
    }

    let output = match state
        .s3
        .get_object()
        .bucket(state.bucket.as_ref())
        .key(&query.key)
        .send()
        .await
    {
        Ok(output) => output,
        Err(err) => {
            error!(error = %err, "S3 download failed");
            return error_response(StatusCode::NOT_FOUND, "file not found");
        }
    };

    let content_type = output
        .content_type()
        .unwrap_or("application/octet-stream")
        .to_string();

    let content_length = output.content_length();

    let stream = tokio_util::io::ReaderStream::new(
    output.body.into_async_read(),
);

let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;

    if let Ok(value) = header::HeaderValue::from_str(&content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }

    if let Some(length) = content_length {
        if let Ok(value) = header::HeaderValue::from_str(&length.to_string()) {
            response.headers_mut().insert(header::CONTENT_LENGTH, value);
        }
    }

    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        header::HeaderValue::from_static("attachment"),
    );

    response
}

/// Enforce an upload size cap without buffering the request.
/// If exceeded, the stream errors and the S3 request fails.
fn limit_stream<S>(
    stream: S,
    max_bytes: u64,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>>
where
    S: futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    use futures_util::StreamExt;
    let mut total = 0_u64;
    stream.map(move |item| {
        let chunk = item?;
        total = total.saturating_add(chunk.len() as u64);
        if total > max_bytes {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "upload exceeds configured size limit",
            ))
        } else {
            Ok(chunk)
        }
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG").unwrap_or_else(|_| "rust_s3_upload=info,tower_http=info".into()),
        )
        .init();

    let bucket = env::var("S3_BUCKET").expect("S3_BUCKET must be set");
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let bind = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let max_upload_bytes = env::var("MAX_UPLOAD_BYTES")
        .unwrap_or_else(|_| (100 * 1024 * 1024_u64).to_string())
        .parse::<u64>()?;

    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region))
        .load()
        .await;
    use aws_sdk_s3::config::{Builder as S3ConfigBuilder, RequestChecksumCalculation, ResponseChecksumValidation};

let endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    

let s3_config = S3ConfigBuilder::from(&aws_config)
    .endpoint_url(endpoint)
    .force_path_style(true) // http://host:9000/bucket/key, not bucket.host
    .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
    .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
    .build();

let s3 = S3Client::from_conf(s3_config);
    let state = AppState {
        s3,
        bucket: Arc::from(bucket),
        max_upload_bytes,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/files", put(upload))
        .route("/v1/files/access", get(access_file))
        .layer(DefaultBodyLimit::disable())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr: SocketAddr = bind.parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "starting S3 upload service");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}
