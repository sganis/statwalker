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

    // ---- Minimal PAM FFI ----
    #[repr(C)]
    struct PamHandle { _private: [u8; 0] }

    #[repr(C)]
    struct PamMessage {
        msg_style: c_int,
        msg: *const c_char,
    }

    #[repr(C)]
    struct PamResponse {
        resp: *mut c_char,
        resp_retcode: c_int,
    }

    #[repr(C)]
    struct PamConv {
        conv: Option<
            extern "C" fn(
                num_msg: c_int,
                msg: *mut *mut PamMessage,
                resp: *mut *mut PamResponse,
                appdata_ptr: *mut c_void,
            ) -> c_int,
        >,
        appdata_ptr: *mut c_void,
    }

    const PAM_SUCCESS: c_int = 0;
    const PAM_PROMPT_ECHO_OFF: c_int = 1;
    const PAM_PROMPT_ECHO_ON: c_int  = 2;
    const PAM_ERROR_MSG: c_int       = 3;
    const PAM_TEXT_INFO: c_int       = 4;

    #[link(name = "pam")]
    extern "C" {
        fn pam_start(
            service_name: *const c_char,
            user: *const c_char,
            pam_conversation: *const PamConv,
            pamh: *mut *mut PamHandle,
        ) -> c_int;

        fn pam_end(pamh: *mut PamHandle, pam_status: c_int) -> c_int;
        fn pam_authenticate(pamh: *mut PamHandle, flags: c_int) -> c_int;
        fn pam_acct_mgmt(pamh: *mut PamHandle, flags: c_int) -> c_int;

        // glibc
        fn calloc(nmemb: usize, size: usize) -> *mut c_void;
        fn free(ptr: *mut c_void);
        fn strdup(s: *const c_char) -> *mut c_char;
    }

    // Conversation callback: supply password for PROMPT_ECHO_OFF
    extern "C" fn conv(
        num_msg: c_int,
        msg: *mut *mut PamMessage,
        resp: *mut *mut PamResponse,
        appdata_ptr: *mut c_void,
    ) -> c_int {
        unsafe {
            if num_msg <= 0 || msg.is_null() || resp.is_null() {
                return 1; // error
            }

            // Allocate response array (PAM will free it)
            let size = std::mem::size_of::<PamResponse>();
            let replies = calloc(num_msg as usize, size) as *mut PamResponse;
            if replies.is_null() {
                return 1;
            }
            *resp = replies;

            let pw_cstr = appdata_ptr as *const c_char;

            for i in 0..num_msg {
                let m = *msg.add(i as usize);
                let r = replies.add(i as usize);

                match (*m).msg_style {
                    PAM_PROMPT_ECHO_OFF => {
                        // Duplicate password for PAM
                        (*r).resp = strdup(pw_cstr);
                        (*r).resp_retcode = 0;
                        if (*r).resp.is_null() {
                            return 1;
                        }
                    }
                    PAM_PROMPT_ECHO_ON => {
                        // Not used here; respond empty
                        (*r).resp = ptr::null_mut();
                        (*r).resp_retcode = 0;
                    }
                    PAM_ERROR_MSG | PAM_TEXT_INFO => {
                        // Informational; no response needed
                        (*r).resp = ptr::null_mut();
                        (*r).resp_retcode = 0;
                    }
                    _ => {
                        return 1; // unknown style
                    }
                }
            }

            PAM_SUCCESS
        }
    }

    /// Verify a user/password against the system PAM stack.
    /// Service can be overridden via `PAM_SERVICE`, default "login".
    pub fn verify_user(username: &str, password: &str) -> bool {
        unsafe {
            let service = std::env::var("PAM_SERVICE").unwrap_or_else(|_| "login".to_string());
            let service_c = match CString::new(service) { Ok(s) => s, Err(_) => return false };
            let user_c    = match CString::new(username) { Ok(s) => s, Err(_) => return false };
            let pw_c      = match CString::new(password) { Ok(s) => s, Err(_) => return false };

            let mut pamh: *mut PamHandle = ptr::null_mut();
            let mut conv = PamConv {
                conv: Some(conv),
                appdata_ptr: pw_c.as_ptr() as *mut c_void, // keep pw_c alive
            };

            let rc = pam_start(service_c.as_ptr(), user_c.as_ptr(), &conv as *const PamConv, &mut pamh);
            if rc != PAM_SUCCESS || pamh.is_null() {
                return false;
            }

            let mut ok = false;

            // Authenticate
            if pam_authenticate(pamh, 0) == PAM_SUCCESS {
                // Account management (e.g., expired, locked)
                let acct = pam_acct_mgmt(pamh, 0);
                ok = acct == PAM_SUCCESS;
            }

            pam_end(pamh, 0);
            ok
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

