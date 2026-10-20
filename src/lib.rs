//! Files service: upload/download to S3-compatible storage, wrapped in the Ferrumec
//! ecosystem.
//!
//! * `ferrumec`: `Module` + `launch` (HTTP server, OpenTelemetry logs/metrics/traces,
//!   permission middleware), `Infra` (Postgres, cache, event stream), events, `Authority`.
//! * `viewset`: sessions (identity), the file-metadata entity/repository with caching,
//!   pagination and filtering, and the shared `ApiError` JSON errors.
//!
//! Routes (all require a session and a matching entry in `permissions.json`):
//!   PUT    /api/files/v1/files?filename=report.pdf    upload (raw body)
//!   GET    /api/files/v1/files                         list your files (page, page_size, sort, content_type)
//!   GET    /api/files/v1/files/{id}                    metadata
//!   GET    /api/files/v1/files/{id}/download           download
//!   DELETE /api/files/v1/files/{id}                    delete
//!   GET    /api/health                                 Postgres/cache/bus health (from ferrumec)
//!
//! `ferrumec::launch` listens on 127.0.0.1:8080. See `config.rs` for every setting.

mod config;
mod events;
mod handlers;
mod limits;
mod metrics;
mod model;
mod module;
mod responses;
mod s3_upload;
mod state;

pub use module::FilesModule as Module;
