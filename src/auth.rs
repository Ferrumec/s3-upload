//! Authentication and authorization, built from the two crates:
//!
//! * **Who is calling** (authentication): the `session` cookie holds a session id; the
//!   session itself (`viewset::web::Session`) is looked up in the shared cache store
//!   `sessions_items`, the same store `viewset::web::SessionMiddleware` uses. Some auth
//!   service must create it at login.
//! * **May they call this route** (authorization): `ferrumec::launch` wraps the app in
//!   the permission middleware, which checks the `permission` cookie's role bitset
//!   against `permissions.json` and leaves a `ferrumec::Authority` in the request.
//!
//! `Principal` is an extractor that requires both and rejects the request otherwise,
//! so a handler that takes a `Principal` can never run for an anonymous caller.
//!
//! Why the extra check instead of relying on the middleware alone: the permission
//! middleware lets a route through when `permissions.json` has no entry for it. Here a
//! route missing from the file ends up with no `Authority`, and `Principal` answers
//! 403, i.e. default deny.

use std::sync::Arc;

use actix_web::{FromRequest, HttpMessage, HttpRequest, dev::Payload, web};
use chrono::Utc;
use ferrumec::{Authority, Store};
use futures_util::future::LocalBoxFuture;
use uuid::Uuid;
use viewset::{ApiError, web::Session};

/// Shared by every worker: where sessions live and which cookie carries the id.
#[derive(Clone)]
pub struct Auth {
    sessions: Arc<dyn Store<Uuid, Session>>,
    cookie_name: String,
}

impl Auth {
    pub fn new(sessions: Arc<dyn Store<Uuid, Session>>, cookie_name: String) -> Self {
        Self {
            sessions,
            cookie_name,
        }
    }
}

/// The authenticated and authorized caller.
#[allow(dead_code)]
pub struct Principal {
    pub user_id: Uuid,
    pub username: String,
    pub email: String,
    pub session_id: Uuid,
    /// Role bitset from the permission session (see `permissions.json`).
    pub role: u128,
}

impl FromRequest for Principal {
    type Error = ApiError;
    type Future = LocalBoxFuture<'static, Result<Self, ApiError>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let auth = req.app_data::<web::Data<Auth>>().cloned();
        let session_cookie = auth.as_ref().and_then(|a| req.cookie(&a.cookie_name));
        let authority = req.extensions().get::<Authority>().cloned();

        Box::pin(async move {
            let auth = auth.ok_or_else(|| ApiError::Internal("auth is not configured".into()))?;

            let cookie = session_cookie.ok_or(ApiError::Unauthorized)?;
            let session_id = Uuid::parse_str(cookie.value()).map_err(|_| ApiError::Unauthorized)?;

            let session = match auth.sessions.get(&session_id).await {
                Ok(Some(session)) => session,
                Ok(None) => return Err(ApiError::Unauthorized),
                Err(e) => return Err(ApiError::Internal(format!("session store: {e}"))),
            };
            if Utc::now() > session.expires_at {
                return Err(ApiError::Unauthorized);
            }

            // No Authority = the route is not listed in permissions.json (default deny).
            let authority = authority.ok_or(ApiError::Forbidden)?;
            // Two independent cookies: make sure they belong to the same user.
            if authority.user != session.sub {
                return Err(ApiError::Forbidden);
            }

            Ok(Principal {
                user_id: session.sub,
                username: session.username,
                email: session.email,
                session_id,
                role: authority.role,
            })
        })
    }
}
