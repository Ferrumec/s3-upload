//! S3 upload/download service (actix-web).
//!
//! Endpoints
//!   PUT /v1/files?filename=report.pdf          raw body -> 201 {key, bucket, filename, content_type, size_bytes}
//!   GET /v1/files/access?key=uploads/<uuid>    streams the object back
//!   GET /health
//!
//! Uploads are streamed into S3 one part at a time (multipart upload), so memory
//! per upload is bounded by PART_SIZE_BYTES no matter how large the file is.
//! Files smaller than one part use a single PutObject.
//!
//! Configuration (environment variables, defaults in brackets)
//!   S3_BUCKET                    required
//!   S3_ENDPOINT                  custom endpoint, e.g. http://127.0.0.1:9000 (enables path-style); unset = AWS
//!   S3_CREATE_BUCKET             "1" to create the bucket at startup if it is missing
//!   AWS_REGION [us-east-1], AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
//!   BIND_ADDR [0.0.0.0:3000]     WORKERS [cpu count]     MAX_CONNECTIONS [1024]
//!
//!   MAX_UPLOAD_BYTES [100 MiB]   largest accepted object
//!   PART_SIZE_BYTES [8 MiB]      multipart part size (min 5 MiB); memory per upload
//!   MAX_CONCURRENT_UPLOADS [8]   global cap; excess requests get 503
//!   MAX_CONCURRENT_DOWNLOADS [32]
//!   MAX_UPLOADS_PER_CLIENT [2]   per client IP; excess requests get 429
//!   UPLOAD_IDLE_TIMEOUT_SECS [30]    max gap between body chunks
//!   UPLOAD_TOTAL_TIMEOUT_SECS [600]  max duration of one upload
//!   DOWNLOAD_IDLE_TIMEOUT_SECS [30]  max stall while streaming a download
//!   S3_CONNECT_TIMEOUT_SECS [5]  S3_OP_TIMEOUT_SECS [120] (per attempt)
//!   CLIENT_UPLOAD_QUOTA_BYTES [1 GiB]    per client IP per window
//!   CLIENT_DOWNLOAD_QUOTA_BYTES [5 GiB]  per client IP per window
//!   QUOTA_WINDOW_SECS [3600]

use std::{
    collections::HashMap,
    env,
    fmt::Display,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use actix_web::{
    App, HttpRequest, HttpResponse, HttpServer,
    http::{
        StatusCode,
        header::{self, HeaderValue},
    },
    web,
};
use aws_sdk_s3::{
    Client as S3Client,
    config::{
        Builder as S3ConfigBuilder, RequestChecksumCalculation, ResponseChecksumValidation,
        timeout::TimeoutConfig,
    },
    error::DisplayErrorContext,
    operation::get_object::GetObjectError,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{error, info, warn};
use tracing_actix_web::TracingLogger;
use uuid::Uuid;

const MIB: u64 = 1024 * 1024;
const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // S3 minimum for all but the last part
const MAX_PARTS: u64 = 10_000; // S3 maximum number of parts

/// Characters left unescaped when storing the original filename as S3 metadata
/// (metadata must be plain ASCII, so everything else is percent-encoded).
const META_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'.').remove(b'-').remove(b'_');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Config {
    max_upload_bytes: u64,
    part_size: usize,
    upload_idle_timeout: Duration,
    upload_total_timeout: Duration,
    download_idle_timeout: Duration,
    max_uploads_per_client: u32,
    client_upload_quota: u64,
    client_download_quota: u64,
    quota_window: Duration,
}

fn env_parse<T>(name: &str, default: T) -> Result<T, String>
where
    T: FromStr,
    T::Err: Display,
{
    match env::var(name) {
        Ok(v) => v.trim().parse().map_err(|e| format!("{name}: {e}")),
        Err(_) => Ok(default),
    }
}

fn env_secs(name: &str, default: u64) -> Result<Duration, String> {
    env_parse(name, default).map(Duration::from_secs)
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let cfg = Config {
            max_upload_bytes: env_parse("MAX_UPLOAD_BYTES", 100 * MIB)?,
            part_size: env_parse("PART_SIZE_BYTES", 8 * 1024 * 1024_usize)?,
            upload_idle_timeout: env_secs("UPLOAD_IDLE_TIMEOUT_SECS", 30)?,
            upload_total_timeout: env_secs("UPLOAD_TOTAL_TIMEOUT_SECS", 600)?,
            download_idle_timeout: env_secs("DOWNLOAD_IDLE_TIMEOUT_SECS", 30)?,
            max_uploads_per_client: env_parse("MAX_UPLOADS_PER_CLIENT", 2)?,
            client_upload_quota: env_parse("CLIENT_UPLOAD_QUOTA_BYTES", 1024 * MIB)?,
            client_download_quota: env_parse("CLIENT_DOWNLOAD_QUOTA_BYTES", 5 * 1024 * MIB)?,
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

// ---------------------------------------------------------------------------
// Per-client concurrency + quota tracking
// ---------------------------------------------------------------------------

struct ClientEntry {
    window_start: Instant,
    upload_bytes: u64,
    download_bytes: u64,
    active_uploads: u32,
}

#[derive(Debug, PartialEq, Eq)]
enum Denied {
    /// Too many simultaneous uploads from this client.
    Busy,
    /// Byte quota for the current window is used up; retry after N seconds.
    Quota { retry_after: u64 },
}

/// Tracks, per client IP, how many uploads are running and how many bytes were
/// moved in the current fixed window. All state is in memory: it resets when the
/// process restarts and is not shared between instances.
struct Clients {
    inner: Mutex<HashMap<IpAddr, ClientEntry>>,
    window: Duration,
    max_active_uploads: u32,
    upload_quota: u64,
    download_quota: u64,
}

impl Clients {
    fn new(cfg: &Config) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            window: cfg.quota_window,
            max_active_uploads: cfg.max_uploads_per_client,
            upload_quota: cfg.client_upload_quota,
            download_quota: cfg.client_download_quota,
        })
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<IpAddr, ClientEntry>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Fetch (creating if needed) the entry for `ip`, rolling its window if expired.
    /// Also returns the seconds left in the window.
    fn entry<'a>(
        &self,
        map: &'a mut HashMap<IpAddr, ClientEntry>,
        ip: IpAddr,
    ) -> (&'a mut ClientEntry, u64) {
        let now = Instant::now();
        let e = map.entry(ip).or_insert(ClientEntry {
            window_start: now,
            upload_bytes: 0,
            download_bytes: 0,
            active_uploads: 0,
        });
        if now.duration_since(e.window_start) >= self.window {
            e.window_start = now;
            e.upload_bytes = 0;
            e.download_bytes = 0;
        }
        let left = self
            .window
            .saturating_sub(now.duration_since(e.window_start))
            .as_secs()
            .max(1);
        (e, left)
    }

    /// Reserve an upload slot. `declared_len` is the Content-Length (0 if unknown):
    /// a request that cannot possibly fit in the remaining quota is refused early.
    fn begin_upload(self: &Arc<Self>, ip: IpAddr, declared_len: u64) -> Result<UploadSlot, Denied> {
        let mut map = self.lock();
        let (e, left) = self.entry(&mut map, ip);
        if e.active_uploads >= self.max_active_uploads {
            return Err(Denied::Busy);
        }
        if e.upload_bytes.saturating_add(declared_len) > self.upload_quota
            || e.upload_bytes >= self.upload_quota
        {
            return Err(Denied::Quota { retry_after: left });
        }
        e.active_uploads += 1;
        Ok(UploadSlot {
            clients: Arc::clone(self),
            ip,
        })
    }

    /// Count `n` more uploaded bytes against the client's quota.
    fn charge_upload(&self, ip: IpAddr, n: u64) -> Result<(), u64> {
        let mut map = self.lock();
        let (e, left) = self.entry(&mut map, ip);
        if e.upload_bytes.saturating_add(n) > self.upload_quota {
            return Err(left);
        }
        e.upload_bytes += n;
        Ok(())
    }

    /// Check (without charging) that a download of `len` bytes fits the quota.
    fn download_allowed(&self, ip: IpAddr, len: u64) -> Result<(), u64> {
        let mut map = self.lock();
        let (e, left) = self.entry(&mut map, ip);
        if e.download_bytes.saturating_add(len) > self.download_quota {
            return Err(left);
        }
        Ok(())
    }

    /// Count `n` more downloaded bytes against the client's quota.
    fn charge_download(&self, ip: IpAddr, n: u64) -> Result<(), u64> {
        let mut map = self.lock();
        let (e, left) = self.entry(&mut map, ip);
        if e.download_bytes.saturating_add(n) > self.download_quota {
            return Err(left);
        }
        e.download_bytes += n;
        Ok(())
    }

    /// Drop idle entries whose window has expired so the map cannot grow forever.
    fn sweep(&self) {
        let now = Instant::now();
        let window = self.window;
        self.lock()
            .retain(|_, e| e.active_uploads > 0 || now.duration_since(e.window_start) < window);
    }
}

/// Releases the client's upload slot when dropped (success, error or disconnect).
struct UploadSlot {
    clients: Arc<Clients>,
    ip: IpAddr,
}

impl Drop for UploadSlot {
    fn drop(&mut self) {
        if let Some(e) = self.clients.lock().get_mut(&self.ip) {
            e.active_uploads = e.active_uploads.saturating_sub(1);
        }
    }
}

// ---------------------------------------------------------------------------
// App state, responses
// ---------------------------------------------------------------------------

struct AppState {
    s3: S3Client,
    bucket: Arc<str>,
    cfg: Config,
    upload_permits: Arc<Semaphore>,
    download_permits: Arc<Semaphore>,
    clients: Arc<Clients>,
}

#[derive(Deserialize)]
struct UploadQuery {
    /// Optional display name. Never used as the S3 object key.
    filename: Option<String>,
}

#[derive(Deserialize)]
struct AccessQuery {
    key: String,
}

#[derive(Serialize)]
struct UploadResponse {
    key: String,
    bucket: String,
    filename: String,
    content_type: String,
    /// Number of bytes actually received and stored.
    size_bytes: u64,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

fn json_error(status: StatusCode, message: impl Into<String>) -> HttpResponse {
    HttpResponse::build(status).json(ErrorResponse {
        error: message.into(),
    })
}

fn retry_later(status: StatusCode, message: &str, retry_after: u64) -> HttpResponse {
    let mut resp = json_error(status, message);
    resp.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(retry_after));
    resp
}

fn denied_response(d: Denied) -> HttpResponse {
    match d {
        Denied::Busy => retry_later(
            StatusCode::TOO_MANY_REQUESTS,
            "too many concurrent uploads from this client",
            5,
        ),
        Denied::Quota { retry_after } => retry_later(
            StatusCode::TOO_MANY_REQUESTS,
            "upload quota exceeded",
            retry_after,
        ),
    }
}

fn server_busy() -> HttpResponse {
    retry_later(
        StatusCode::SERVICE_UNAVAILABLE,
        "server is at capacity, try again shortly",
        5,
    )
}

fn client_ip(req: &HttpRequest) -> IpAddr {
    // The direct peer address. Behind a reverse proxy every client shares the
    // proxy's address, so per-client limits then act as one global limit.
    req.peer_addr()
        .map(|a| a.ip().to_canonical())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
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

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

enum UploadError {
    TooLarge,
    Quota { retry_after: u64 },
    IdleTimeout,
    TotalTimeout,
    ClientBody(String),
    Storage,
}

impl UploadError {
    fn into_response(self) -> HttpResponse {
        match self {
            UploadError::TooLarge => json_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "upload exceeds configured size limit",
            ),
            UploadError::Quota { retry_after } => retry_later(
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

struct ObjectMeta<'a> {
    key: &'a str,
    content_type: &'a str,
    filename: &'a str,
}

impl ObjectMeta<'_> {
    fn encoded_filename(&self) -> String {
        utf8_percent_encode(self.filename, META_SET).to_string()
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
async fn stream_to_s3(
    state: &AppState,
    ip: IpAddr,
    meta: &ObjectMeta<'_>,
    payload: &mut web::Payload,
) -> Result<u64, UploadError> {
    let cfg = &state.cfg;
    let part_size = cfg.part_size;
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
        if total > cfg.max_upload_bytes {
            return Err(UploadError::TooLarge);
        }
        state
            .clients
            .charge_upload(ip, len)
            .map_err(|retry_after| UploadError::Quota { retry_after })?;

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

async fn upload(
    req: HttpRequest,
    query: web::Query<UploadQuery>,
    mut payload: web::Payload,
    state: web::Data<AppState>,
) -> HttpResponse {
    let ip = client_ip(&req);

    // Cheap rejections first: they cost nothing and read no body.
    let declared_len = match req.headers().get(header::CONTENT_LENGTH) {
        None => 0,
        Some(v) => match v.to_str().ok().and_then(|v| v.parse::<u64>().ok()) {
            Some(n) if n > state.cfg.max_upload_bytes => {
                return UploadError::TooLarge.into_response();
            }
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "invalid Content-Length"),
        },
    };

    let Ok(_permit) = Arc::clone(&state.upload_permits).try_acquire_owned() else {
        return server_busy();
    };
    let _slot = match state.clients.begin_upload(ip, declared_len) {
        Ok(slot) => slot,
        Err(d) => return denied_response(d),
    };

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
        stream_to_s3(&state, ip, &meta, &mut payload),
    )
    .await
    .unwrap_or(Err(UploadError::TotalTimeout));

    match result {
        Ok(size_bytes) => {
            info!(%ip, %key, size_bytes, "upload complete");
            HttpResponse::Created().json(UploadResponse {
                key,
                bucket: state.bucket.to_string(),
                filename,
                content_type,
                size_bytes,
            })
        }
        Err(e) => e.into_response(),
    }
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

struct DownloadState {
    body: ByteStream,
    clients: Arc<Clients>,
    ip: IpAddr,
    idle: Duration,
    _permit: OwnedSemaphorePermit, // held until the response body is dropped
}

async fn access_file(
    req: HttpRequest,
    query: web::Query<AccessQuery>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let key = &query.key;
    if !key.starts_with("uploads/") || key.len() > 512 || key.contains("..") {
        return json_error(StatusCode::BAD_REQUEST, "invalid object key");
    }
    let ip = client_ip(&req);

    let Ok(permit) = Arc::clone(&state.download_permits).try_acquire_owned() else {
        return server_busy();
    };

    let output = match state
        .s3
        .get_object()
        .bucket(&*state.bucket)
        .key(key)
        .send()
        .await
    {
        Ok(output) => output,
        Err(err) => {
            if err
                .as_service_error()
                .is_some_and(GetObjectError::is_no_such_key)
            {
                return json_error(StatusCode::NOT_FOUND, "file not found");
            }
            error!(error = %DisplayErrorContext(&err), "S3 download failed");
            return json_error(StatusCode::BAD_GATEWAY, "storage download failed");
        }
    };

    let length = output.content_length().and_then(|n| u64::try_from(n).ok());
    if let Some(len) = length {
        if let Err(retry_after) = state.clients.download_allowed(ip, len) {
            return retry_later(
                StatusCode::TOO_MANY_REQUESTS,
                "download quota exceeded",
                retry_after,
            );
        }
    }

    let content_type = output
        .content_type()
        .and_then(|ct| HeaderValue::from_str(ct).ok())
        .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"));

    let stream = futures_util::stream::try_unfold(
        DownloadState {
            body: output.body,
            clients: Arc::clone(&state.clients),
            ip,
            idle: state.cfg.download_idle_timeout,
            _permit: permit,
        },
        |mut st| async move {
            match tokio::time::timeout(st.idle, st.body.try_next()).await {
                Ok(Ok(Some(chunk))) => {
                    if st
                        .clients
                        .charge_download(st.ip, chunk.len() as u64)
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

    let mut resp = HttpResponse::Ok();
    resp.insert_header((header::CONTENT_TYPE, content_type))
        .insert_header((header::CONTENT_DISPOSITION, "attachment"))
        .insert_header((header::X_CONTENT_TYPE_OPTIONS, "nosniff"));
    if let Some(len) = length {
        resp.no_chunking(len);
    }
    resp.streaming(stream)
}

async fn health() -> &'static str {
    "ok"
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

async fn ensure_bucket(s3: &S3Client, bucket: &str, create: bool) {
    if s3.head_bucket().bucket(bucket).send().await.is_ok() {
        return;
    }
    if create {
        match s3.create_bucket().bucket(bucket).send().await {
            Ok(_) => info!(%bucket, "created bucket"),
            Err(e) => warn!(%bucket, error = %DisplayErrorContext(&e), "could not create bucket"),
        }
    } else {
        warn!(%bucket, "bucket not reachable at startup; uploads will fail until it exists (set S3_CREATE_BUCKET=1 to create it)");
    }
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG").unwrap_or_else(|_| {
                "rust_s3_upload=info,tracing_actix_web=info,actix_web=info".into()
            }),
        )
        .init();

    let bucket = env::var("S3_BUCKET").map_err(|_| "S3_BUCKET must be set")?;
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let bind: SocketAddr = env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3000".into())
        .parse()?;
    let cfg = Config::from_env()?;
    let max_uploads: usize = env_parse("MAX_CONCURRENT_UPLOADS", 8)?;
    let max_downloads: usize = env_parse("MAX_CONCURRENT_DOWNLOADS", 32)?;
    let max_connections: usize = env_parse("MAX_CONNECTIONS", 1024)?;
    let workers: usize = env_parse(
        "WORKERS",
        std::thread::available_parallelism().map_or(2, |n| n.get()),
    )?;

    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region))
        .load()
        .await;

    let mut s3_config = S3ConfigBuilder::from(&aws_config)
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
        s3_config = s3_config.endpoint_url(endpoint).force_path_style(true);
    }
    let s3 = S3Client::from_conf(s3_config.build());

    ensure_bucket(
        &s3,
        &bucket,
        env::var("S3_CREATE_BUCKET").is_ok_and(|v| v == "1"),
    )
    .await;

    let clients = Clients::new(&cfg);
    {
        let clients = Arc::clone(&clients);
        actix_web::rt::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                clients.sweep();
            }
        });
    }

    let state = web::Data::new(AppState {
        s3,
        bucket: Arc::from(bucket),
        cfg,
        upload_permits: Arc::new(Semaphore::new(max_uploads)),
        download_permits: Arc::new(Semaphore::new(max_downloads)),
        clients,
    });

    info!(
        %bind, workers, max_uploads, max_downloads,
        max_upload_bytes = state.cfg.max_upload_bytes,
        part_size = state.cfg.part_size,
        "starting S3 upload service"
    );

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .app_data(web::QueryConfig::default().error_handler(|err, _| {
                actix_web::error::InternalError::from_response(
                    err,
                    json_error(StatusCode::BAD_REQUEST, "invalid query string"),
                )
                .into()
            }))
            .wrap(TracingLogger::default())
            .route("/health", web::get().to(health))
            .route("/v1/files", web::put().to(upload))
            .route("/v1/files/access", web::get().to(access_file))
    })
    .workers(workers)
    .max_connections(max_connections)
    // Time allowed for a client to send the request head (slow-loris protection).
    // Body stalls are handled per upload by UPLOAD_IDLE_TIMEOUT_SECS.
    .client_request_timeout(Duration::from_secs(10))
    .client_disconnect_timeout(Duration::from_secs(5))
    .keep_alive(Duration::from_secs(30))
    .shutdown_timeout(30)
    .bind(bind)?
    .run()
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const IP: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

    fn clients(max_active: u32, up: u64, down: u64, window_ms: u64) -> Arc<Clients> {
        Arc::new(Clients {
            inner: Mutex::new(HashMap::new()),
            window: Duration::from_millis(window_ms),
            max_active_uploads: max_active,
            upload_quota: up,
            download_quota: down,
        })
    }

    #[test]
    fn upload_slots_are_limited_per_client_and_released_on_drop() {
        let c = clients(2, 1000, 1000, 60_000);
        let a = c.begin_upload(IP, 0).unwrap();
        let _b = c.begin_upload(IP, 0).unwrap();
        assert_eq!(c.begin_upload(IP, 0).err(), Some(Denied::Busy));
        // a different client is unaffected
        assert!(
            c.begin_upload(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 0)
                .is_ok()
        );
        drop(a);
        assert!(c.begin_upload(IP, 0).is_ok());
    }

    #[test]
    fn declared_length_beyond_remaining_quota_is_refused_early() {
        let c = clients(5, 100, 100, 60_000);
        c.charge_upload(IP, 60).unwrap();
        assert!(matches!(
            c.begin_upload(IP, 50).err(),
            Some(Denied::Quota { .. })
        ));
        assert!(c.begin_upload(IP, 40).is_ok());
    }

    #[test]
    fn upload_quota_is_enforced_and_resets_after_the_window() {
        let c = clients(5, 100, 100, 50);
        assert!(c.charge_upload(IP, 100).is_ok());
        assert!(c.charge_upload(IP, 1).is_err());
        std::thread::sleep(Duration::from_millis(80));
        assert!(c.charge_upload(IP, 100).is_ok());
    }

    #[test]
    fn download_quota_is_checked_and_charged() {
        let c = clients(5, 100, 100, 60_000);
        assert!(c.download_allowed(IP, 100).is_ok());
        assert!(c.download_allowed(IP, 101).is_err());
        c.charge_download(IP, 70).unwrap();
        assert!(c.charge_download(IP, 31).is_err());
        assert!(c.download_allowed(IP, 30).is_ok());
    }

    #[test]
    fn sweep_removes_expired_idle_clients_but_keeps_active_ones() {
        let c = clients(5, 100, 100, 30);
        let idle = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9));
        c.charge_upload(idle, 1).unwrap();
        let _active = c.begin_upload(IP, 0).unwrap();
        std::thread::sleep(Duration::from_millis(60));
        c.sweep();
        let map = c.lock();
        assert!(!map.contains_key(&idle));
        assert!(map.contains_key(&IP));
    }

    #[test]
    fn filenames_are_reduced_to_a_safe_leaf() {
        assert_eq!(safe_filename(Some("../../etc/passwd")), "passwd");
        assert_eq!(safe_filename(Some("C:\\temp\\a.txt")), "a.txt");
        assert_eq!(safe_filename(Some("  ")), "upload.bin");
        assert_eq!(safe_filename(None), "upload.bin");
    }
}
