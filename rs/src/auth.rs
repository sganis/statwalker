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
    use libc::{c_char, c_int, c_void};
    use std::{ffi::CString, ptr};

    // ---- PAM FFI (as you already have) ----
    #[repr(C)] struct PamHandle { _private: [u8;0] }
    #[repr(C)] struct PamMessage { msg_style: c_int, msg: *const c_char }
    #[repr(C)] struct PamResponse { resp: *mut c_char, resp_retcode: c_int }
    type PamConvFunc = extern "C" fn(c_int, *mut *const PamMessage, *mut *mut PamResponse, *mut c_void) -> c_int;
    #[repr(C)] struct PamConv { conv: Option<PamConvFunc>, appdata_ptr: *mut c_void }

    const PAM_SUCCESS: c_int = 0;
    const PAM_PROMPT_ECHO_OFF: c_int = 1;
    const PAM_ERROR_MSG: c_int = 3;
    const PAM_TEXT_INFO: c_int = 4;

    #[link(name = "pam")]
    unsafe extern "C" {
        fn pam_start(service_name: *const c_char, user: *const c_char, conv: *const PamConv, pamh: *mut *mut PamHandle) -> c_int;
        fn pam_end(pamh: *mut PamHandle, pam_status: c_int) -> c_int;
        fn pam_authenticate(pamh: *mut PamHandle, flags: c_int) -> c_int;
        // fn pam_acct_mgmt(pamh: *mut PamHandle, flags: c_int) -> c_int; // optional
    }

    unsafe extern "C" {
        fn calloc(nmemb: usize, size: usize) -> *mut c_void;
        fn strdup(s: *const c_char) -> *mut c_char;
    }

    // Conversation callback: feed password for "hidden" prompt
    extern "C" fn pam_conv(n: c_int, msg: *mut *const PamMessage, resp: *mut *mut PamResponse, appdata: *mut c_void) -> c_int {
        unsafe {
            if n <= 0 || msg.is_null() || resp.is_null() { return 1; }
            let replies = calloc(n as usize, std::mem::size_of::<PamResponse>()) as *mut PamResponse;
            if replies.is_null() { return 1; }
            *resp = replies;

            let pw = appdata as *const c_char;

            for i in 0..n {
                let m = *msg.add(i as usize);
                let r = replies.add(i as usize);
                (*r).resp = std::ptr::null_mut();
                (*r).resp_retcode = 0;
                match (*m).msg_style {
                    PAM_PROMPT_ECHO_OFF => { // password
                        if !pw.is_null() {
                            (*r).resp = strdup(pw);
                            if (*r).resp.is_null() { return 1; }
                        }
                    }
                    PAM_ERROR_MSG | PAM_TEXT_INFO => { /* no answer needed */ }
                    _ => { /* ignore */ }
                }
            }
            PAM_SUCCESS
        }
    }

    /// Authenticate a user by password via PAM without needing root.
    /// Defaults to PAM service "su" (present by default; no admin changes).
    pub fn verify_user(username: &str, password: &str) -> bool {
        let service = std::env::var("PAM_SERVICE").unwrap_or_else(|_| "su".to_string());
        verify_user_with_service(username, password, &service).unwrap_or(false)
    }

    pub fn verify_user_with_service(username: &str, password: &str, service: &str) -> Result<bool, String> {
        let service_c = CString::new(service).map_err(|_| "bad service")?;
        let user_c    = CString::new(username).map_err(|_| "bad username")?;
        let pw_c      = CString::new(password).map_err(|_| "bad password")?;

        unsafe {
            let mut pamh: *mut PamHandle = std::ptr::null_mut();
            let conv = PamConv { conv: Some(pam_conv), appdata_ptr: pw_c.as_ptr() as *mut c_void };

            let rc = pam_start(service_c.as_ptr(), user_c.as_ptr(), &conv, &mut pamh);
            if rc != PAM_SUCCESS || pamh.is_null() {
                if !pamh.is_null() { pam_end(pamh, rc); }
                return Err(format!("pam_start rc={}", rc));
            }

            let ok = pam_authenticate(pamh, 0) == PAM_SUCCESS;
            // By default, skip pam_acct_mgmt to avoid needing an "account" stack in the service file.
            // If you want it, set PAM_ACCOUNT_CHECK=1
            // if ok && std::env::var("PAM_ACCOUNT_CHECK").ok().is_some() {
            //     ok = pam_acct_mgmt(pamh, 0) == PAM_SUCCESS;
            // }

            pam_end(pamh, 0);
            Ok(ok)
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

