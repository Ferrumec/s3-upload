//! The files service as a `ferrumec::Module`: built from any `Infra`, mounted under
//! a namespace (`/api/files/...` with `ferrumec::launch`).

use std::{env, sync::Arc, time::Duration};

use actix_web::web;
use ferrumec::{Infra, Module};
use tokio::sync::Semaphore;
use viewset::DefaultRepo;

use crate::{
    config::Config,
    handlers,
    limits::UserLimits,
    model::{self, FileRecord},
    s3_upload,
    state::AppState,
};

pub struct FilesModule {
    state: Arc<AppState>,
    auth: Arc<viewset::web::SessionMiddleware>,
}

impl Module for FilesModule {
    async fn new(infra: impl Infra) -> Result<Self, Box<dyn std::error::Error>> {
        let cfg = Config::from_env()?;
        let bucket = env::var("S3_BUCKET").map_err(|_| "S3_BUCKET must be set")?;

        let s3 = s3_upload::build_client().await?;
        s3_upload::ensure_bucket(&s3, &bucket).await;

        // Infrastructure from ferrumec: database pool, cache factory, event stream.
        let db = infra.database();
        model::migrate(&db).await?;
        let repo = DefaultRepo::<FileRecord>::new(db.clone(), infra.cache_factory());
        
        let limits = UserLimits::new(&cfg);
        {
            let limits = Arc::clone(&limits);
            actix_web::rt::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(60));
                loop {
                    tick.tick().await;
                    limits.sweep();
                }
            });
        }

        let auth = Arc::new(viewset::web::SessionMiddleware::new(infra.cache_factory()));
        let state = AppState {
            s3,
            bucket: Arc::from(bucket),
            db,
            repo,
            events: infra.event_stream(),
            upload_permits: Arc::new(Semaphore::new(cfg.max_concurrent_uploads)),
            download_permits: Arc::new(Semaphore::new(cfg.max_concurrent_downloads)),
            limits,
            cfg,
        };
        Ok(Self {
            state: Arc::new(state),
            auth,
        })
    }

    fn configure(self: Arc<Self>, service_conf: &mut web::ServiceConfig, namespace: &str) {
        service_conf.service(
            web::scope(&format!("/{}", namespace.trim_matches('/')))
                .app_data(web::Data::from(Arc::clone(&self.state)))
                .wrap(self.auth.clone())
                .service(
                    web::resource("/v1/files")
                        .route(web::put().to(handlers::upload))
                        .route(web::get().to(handlers::list)),
                )
                .service(
                    web::resource("/v1/files/{id}")
                        .route(web::get().to(handlers::metadata))
                        .route(web::delete().to(handlers::delete)),
                )
                .service(
                    web::resource("/v1/files/{id}/download")
                        .route(web::get().to(handlers::download)),
                ),
        );
    }
}
