use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json, 
    RequestPartsExt, 
};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use jsonwebtoken::{decode, DecodingKey, EncodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::OnceLock;

// ---- Keys (JWT) ----
pub struct Keys {
    pub encoding: EncodingKey,
    pub decoding: DecodingKey,
}

impl Keys {
    fn new(secret: &[u8]) -> Self {
        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
        }
    }
}

static KEYS: OnceLock<Keys> = OnceLock::new();

#[inline]
pub fn keys() -> &'static Keys {
    KEYS.get_or_init(|| {
        let secret = std::env::var("JWT_SECRET").expect("JWT_SECRET must be set");
        Keys::new(secret.as_bytes())
    })
}

#[derive(Debug, Deserialize)]
pub struct AuthPayload {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct AuthBody {
    pub access_token: String,
    pub token_type: String,
}

// implement a method to create a response type containing the JWT
impl AuthBody {
    pub fn new(access_token: String) -> Self {
        Self {
            access_token,
            token_type: "Bearer".to_string(),
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
}

#[derive(Debug)]
pub enum AuthError {
    Forbidden,
    WrongCredentials,
    MissingCredentials,
    TokenCreation,
    InvalidToken,
}

// implement IntoResponse for AuthError so we can use it as an Axum response type
impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AuthError::Forbidden => (StatusCode::FORBIDDEN, "No access to this resource"),
            AuthError::WrongCredentials => (StatusCode::UNAUTHORIZED, "Wrong credentials"),
            AuthError::MissingCredentials => (StatusCode::BAD_REQUEST, "Missing credentials"),
            AuthError::TokenCreation => (StatusCode::INTERNAL_SERVER_ERROR, "Token creation error"),
            AuthError::InvalidToken => (StatusCode::BAD_REQUEST, "Invalid token"),
        };
        let body = Json(ErrorBody { error: error_message });
        (status, body).into_response()
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub is_admin: bool,
    pub exp: usize,
}

// allow us to print the claim details for the private route
impl Display for Claims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "username: {}\nis_admin: {}", self.sub, self.is_admin)
    }
}

// implement FromRequestParts for Claims (the JWT struct)
// FromRequestParts allows us to use Claims without consuming the request
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract the token from the authorization header
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| AuthError::InvalidToken)?;
        // Decode the user data
        let token_data = decode::<Claims>(bearer.token(), &keys().decoding, &Validation::default())
            .map_err(|_| AuthError::InvalidToken)?;

        Ok(token_data.claims)
    }
}

// #[cfg(unix)]
// pub mod platform {
//     use pam::Authenticator;

//     pub fn verify_user(username: &str, password: &str) -> bool {
//         let mut auth = match Authenticator::with_password("login") {
//             Ok(a) => a,
//             Err(_) => return false,
//         };
//         auth.get_handler().set_credentials(username, password);
//         auth.authenticate().is_ok()
//     }
// }

#[cfg(unix)]
pub mod platform {
    use std::process::{Command, Stdio};
    use std::io::Write;

    // Verify user credentials using the su command
    /// Returns true if authentication succeeds, false otherwise
    pub fn verify_user(username: &str, password: &str) -> bool {
        let mut child = match Command::new("su")
            .arg(username)
            .arg("-c")
            .arg("true") // Just run the 'true' command if auth succeeds
            .stdin(Stdio::piped())
            .stdout(Stdio::null()) // Suppress output
            .stderr(Stdio::null()) // Suppress error messages
            .spawn()
        {
            Ok(child) => child,
            Err(_) => return false, // Failed to spawn su command
        };

        // Send the password to su's stdin
        if let Some(mut stdin) = child.stdin.take() {
            if writeln!(stdin, "{}", password).is_err() {
                return false;
            }
        }

        // Wait for su to complete and check exit status
        match child.wait() {
            Ok(status) => status.success(),
            Err(_) => false,
        }
    }

}

#[cfg(windows)]
pub mod platform {
    use std::env;

    // Fake auth: defaults admin/admin, override with env vars.
    pub fn verify_user(username: &str, password: &str) -> bool {
        let expected_user = env::var("FAKE_USER").unwrap_or_else(|_| "admin".to_string());
        let expected_pass = env::var("FAKE_PASSWORD").unwrap_or_else(|_| "admin".to_string());
        username == expected_user && password == expected_pass
    }
}

