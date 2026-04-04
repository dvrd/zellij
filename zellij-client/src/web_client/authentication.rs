use crate::web_client::utils::parse_cookies;
use axum::body::Body;
use axum::http::header::SET_COOKIE;
use axum::{extract::Request, http::StatusCode, middleware::Next, response::Response};
use axum_extra::extract::cookie::{Cookie, SameSite};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use zellij_utils::web_authentication_tokens::{
    hash_token, is_session_token_read_only, validate_session_token,
};

#[derive(Clone)]
pub struct SessionTokenHash(pub String);

#[derive(Clone, Copy)]
pub struct IsReadOnly(pub bool);

/// Cached result of a successful token validation.  Avoids repeated
/// SHA-256 hashing + SQLite queries for the same session token.
struct CachedAuth {
    is_read_only: bool,
    session_token_hash: String,
    validated_at: Instant,
}

/// How long a cached validation result is considered fresh.
const AUTH_CACHE_TTL_SECS: u64 = 60;

static AUTH_CACHE: OnceLock<Mutex<HashMap<String, CachedAuth>>> = OnceLock::new();

fn get_auth_cache() -> &'static Mutex<HashMap<String, CachedAuth>> {
    AUTH_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub async fn auth_middleware(request: Request, next: Next) -> Result<Response, StatusCode> {
    let cookies = parse_cookies(&request);

    let session_token = match cookies.get("session_token") {
        Some(token) => token.clone(),
        None => return Err(StatusCode::UNAUTHORIZED),
    };

    // Fast path: serve from cache if the token was recently validated.
    // Extract cached values while holding the lock, then drop the guard
    // before the .await so the MutexGuard is not held across a suspend
    // point (required for Send).
    let cached_hit = {
        let cache = get_auth_cache().lock().unwrap();
        cache.get(&session_token).and_then(|cached| {
            if cached.validated_at.elapsed().as_secs() < AUTH_CACHE_TTL_SECS {
                Some((cached.is_read_only, cached.session_token_hash.clone()))
            } else {
                None
            }
        })
    };
    if let Some((is_read_only, session_token_hash)) = cached_hit {
        let mut request = request;
        request.extensions_mut().insert(IsReadOnly(is_read_only));
        request
            .extensions_mut()
            .insert(SessionTokenHash(session_token_hash));
        let response = next.run(request).await;
        return Ok(response);
    }

    match validate_session_token(&session_token) {
        Ok(true) => {
            // Check if this is a read-only token
            let is_read_only = is_session_token_read_only(&session_token).unwrap_or(true);

            // Compute session token hash for client ownership verification
            let session_token_hash = hash_token(&session_token);

            // Populate cache
            {
                let mut cache = get_auth_cache().lock().unwrap();
                cache.insert(
                    session_token.clone(),
                    CachedAuth {
                        is_read_only,
                        session_token_hash: session_token_hash.clone(),
                        validated_at: Instant::now(),
                    },
                );
            }

            // Store in request extensions for downstream handlers
            let mut request = request;
            request.extensions_mut().insert(IsReadOnly(is_read_only));
            request
                .extensions_mut()
                .insert(SessionTokenHash(session_token_hash));

            let response = next.run(request).await;
            Ok(response)
        },
        Ok(false) | Err(_) => {
            // Evict from cache so stale entries don't linger.
            {
                let mut cache = get_auth_cache().lock().unwrap();
                cache.remove(&session_token);
            }
            // revoke session_token as if it exists it's no longer valid
            let mut response = Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body(Body::empty())
                .unwrap();

            // Clear both secure and non-secure versions
            // in case the user was on http before and is now on https
            // or vice versa
            let clear_cookies = [
                Cookie::build(("session_token", ""))
                    .http_only(true)
                    .secure(false)
                    .same_site(SameSite::Strict)
                    .path("/")
                    .max_age(time::Duration::seconds(0))
                    .build(),
                Cookie::build(("session_token", ""))
                    .http_only(true)
                    .secure(true)
                    .same_site(SameSite::Strict)
                    .path("/")
                    .max_age(time::Duration::seconds(0))
                    .build(),
            ];

            for cookie in clear_cookies {
                response
                    .headers_mut()
                    .append(SET_COOKIE, cookie.to_string().parse().unwrap());
            }

            Ok(response)
        },
    }
}
