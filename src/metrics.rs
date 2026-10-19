//! Business metrics, exported through the OpenTelemetry pipeline that
//! `ferrumec::Observability` installs (alongside its per-request count/duration).
//!
//! Instruments are created lazily on first use: `ferrumec::launch` sets the global
//! meter provider only after the modules are built, and an instrument created
//! before that would stay attached to the no-op provider.

use std::sync::OnceLock;

use opentelemetry::{KeyValue, global, metrics::Counter};

pub struct Metrics {
    uploads: Counter<u64>,
    upload_bytes: Counter<u64>,
    downloads: Counter<u64>,
}

pub fn get() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = global::meter("files");
        Metrics {
            uploads: meter
                .u64_counter("files.uploads")
                .with_description("Upload attempts by outcome (ok, rejected, failed)")
                .build(),
            upload_bytes: meter
                .u64_counter("files.upload.bytes")
                .with_unit("By")
                .with_description("Bytes stored by successful uploads")
                .build(),
            downloads: meter
                .u64_counter("files.downloads")
                .with_description("Download attempts by outcome (ok, not_found, rejected, failed)")
                .build(),
        }
    })
}

impl Metrics {
    pub fn upload(&self, outcome: &'static str) {
        self.uploads.add(1, &[KeyValue::new("outcome", outcome)]);
    }

    pub fn upload_bytes(&self, bytes: u64) {
        self.upload_bytes.add(bytes, &[]);
    }

    pub fn download(&self, outcome: &'static str) {
        self.downloads.add(1, &[KeyValue::new("outcome", outcome)]);
    }
}
