//! Small JSON error helpers. The body shape `{"error": "..."}` matches
//! `viewset::ApiError`, so clients see one error format everywhere.

use actix_web::{
    HttpResponse,
    http::{
        StatusCode,
        header::{self, HeaderValue},
    },
};
use serde::Serialize;

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

pub fn json_error(status: StatusCode, message: impl Into<String>) -> HttpResponse {
    HttpResponse::build(status).json(ErrorBody {
        error: message.into(),
    })
}

pub fn retry_later(status: StatusCode, message: &str, retry_after: u64) -> HttpResponse {
    let mut resp = json_error(status, message);
    resp.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(retry_after));
    resp
}

pub fn server_busy() -> HttpResponse {
    retry_later(
        StatusCode::SERVICE_UNAVAILABLE,
        "server is at capacity, try again shortly",
        5,
    )
}
