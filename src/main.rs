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

mod auth;
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

use std::{io, time::Duration};

use chrono::Utc;
use ferrumec::{Authority, CacheFactory, Infra, LocalInfra};
use uuid::Uuid;
use viewset::web::Session;

use module::FilesModule;

fn to_io(e: impl ToString) -> io::Error {
    io::Error::other(e.to_string())
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    let infra = LocalInfra::new().await.map_err(to_io)?;

    let modules = ferrumec::modules!(
        infra.clone();
        ("files", FilesModule),
    )
    .map_err(to_io)?;

    if std::env::var("DEV_SEED_SESSION").is_ok_and(|v| v == "1") {
        seed_dev_session(&infra);
    }

    ferrumec::launch::launch(modules, infra).await
}

/// DEVELOPMENT ONLY. Normally an auth service creates sessions at login. For local
/// testing without one, this writes a session and a permission session straight into
/// the in-process caches and prints the cookies to use. It only works with the
/// in-process (Moka) cache, because the caches are not shared with other processes, and
/// it re-seeds every 20 minutes because cache entries expire after 30.
fn seed_dev_session(infra: &LocalInfra) {
    const TTL: Duration = Duration::from_secs(30 * 60);
    // Bits 40-44, matching permissions.files.json.
    const DEV_ROLE: u128 = 0b11111 << 40;

    let user = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let permission_id = Uuid::new_v4();

    let sessions = infra
        .cache_factory()
        .new_cache::<Uuid, Session>("sessions_items", TTL);
    let permissions = infra
        .cache_factory()
        .new_cache::<Uuid, Authority>("permission_sessions", TTL);

    let seed = move || {
        let sessions = sessions.clone();
        let permissions = permissions.clone();
        async move {
            let session: Session = serde_json::from_value(serde_json::json!({
                "id": session_id,
                "created_at": Utc::now(),
                "sub": user,
                "username": "dev",
                "email": "dev@localhost",
                "expires_at": Utc::now() + chrono::Duration::hours(24),
                "ip_address": "127.0.0.1/32",
            }))
            .expect("valid dev session");
            let authority = Authority {
                user,
                namespace: Uuid::nil(),
                role: DEV_ROLE,
            };
            let _ = sessions.set(&session_id, session).await;
            let _ = permissions.set(&permission_id, authority).await;
        }
    };

    // Seeded before `launch` installs the tracing subscriber, so print instead of log.
    println!("DEV_SEED_SESSION=1: development session created (do not use in production)");
    println!("  Cookie: session={session_id}; permission={permission_id}");

    actix_web::rt::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(20 * 60));
        loop {
            tick.tick().await; // first tick fires immediately
            seed().await;
        }
    });
}
