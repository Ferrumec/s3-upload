//! Domain events published on ferrumec's event stream (in-process by default,
//! NATS when the service runs on a distributed `Infra`). Other services can
//! subscribe, e.g. to scan uploads, update usage billing or notify the owner.

use std::sync::Arc;

use ferrumec::{Event, EventStream, EventType};
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct FileUploaded {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
}

impl EventType for FileUploaded {
    const SUBJECT: &'static str = "files.uploaded";
}

#[derive(Serialize, Deserialize)]
pub struct FileDeleted {
    pub id: Uuid,
    pub owner_id: Uuid,
}

impl EventType for FileDeleted {
    const SUBJECT: &'static str = "files.deleted";
}

/// Fire-and-forget: a failed publish is logged but never fails the request, because
/// the upload or delete itself has already succeeded.
pub async fn publish<T: EventType + Sync>(stream: &Arc<dyn EventStream>, payload: T, user: Uuid) {
    let event = Event::new(payload)
        .with_producer("files")
        .with_user_id(user);
    if let Err(e) = event.publish(Arc::clone(stream)).await {
        warn!(subject = T::SUBJECT, error = %e, "failed to publish event");
    }
}
