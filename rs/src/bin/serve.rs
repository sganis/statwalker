use anyhow::{Context, Result as AResult};
use axum::{
    extract::Query,
    http::{Method, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    net::SocketAddr,
    path::{Path, Path as FsPath},
    sync::OnceLock,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use chrono::{NaiveDateTime, Utc};

#[cfg(unix)]
use std::ffi::CStr;

// ===================== Helpers =====================

fn unquote_csv_field(field: &str) -> String {
    let trimmed = field.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        let inner = &trimmed[1..trimmed.len() - 1];
        inner.replace("\"\"", "\"")
    } else {
        trimmed.to_string()
    }
}

fn epoch_secs_to_iso_date(secs: i64) -> String {
    let ndt = NaiveDateTime::from_timestamp_opt(secs, 0)
        .unwrap_or_else(|| NaiveDateTime::from_timestamp_opt(0, 0).unwrap());
    chrono::DateTime::<Utc>::from_utc(ndt, Utc)
        .date_naive()
        .to_string() // "YYYY-MM-DD"
}

#[cfg(unix)]
fn username_from_uid(uid: u32) -> String {
    unsafe {
        let pw = libc::getpwuid(uid);
        if pw.is_null() {
            return "UNK".to_string();
        }
        let name_ptr = (*pw).pw_name;
        if name_ptr.is_null() {
            return "UNK".to_string();
        }
        match CStr::from_ptr(name_ptr).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => "UNK".to_string(),
        }
    }
}

#[cfg(not(unix))]
fn username_from_uid(_uid: u32) -> String {
    "UNK".to_string()
}

// ===================== File scan output (for /api/files) =====================

#[derive(Debug, Clone, Serialize)]
pub struct FsItemOut {
    pub path: String,
    pub owner: String,   // username
    pub size: u64,       // bytes
    pub modified: String // "YYYY-MM-DD" (UTC)
}

/// List regular files in `folder`. If `usernames` is empty → all users.
/// Otherwise, include only files owned by a username in the provided list.
/// Non-recursive.
#[cfg(unix)]
pub fn get_items<P: AsRef<Path>>(folder: P, usernames: &[String]) -> AResult<Vec<FsItemOut>> {
    let filter: Option<HashSet<String>> = if usernames.is_empty() {
        None
    } else {
        Some(usernames.iter().cloned().collect())
    };

    let mut out = Vec::new();

    let dir = fs::read_dir(&folder)
        .with_context(|| format!("read_dir({}) failed", folder.as_ref().display()))?;

    for entry_res in dir {
        let entry = match entry_res {
            Ok(e) => e,
            Err(_) => continue, // skip unreadable entries
        };
        let path = entry.path();

        let md = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        if !md.file_type().is_file() {
            continue;
        }

        let owner = username_from_uid(md.uid());

        if let Some(ref allow) = filter {
            if !allow.contains(&owner) {
                continue;
            }
        }

        out.push(FsItemOut {
            path: path.to_string_lossy().into_owned(),
            owner,
            size: md.size(),
            modified: epoch_secs_to_iso_date(md.mtime()),
        });
    }

    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

#[cfg(not(unix))]
pub fn get_items<P: AsRef<Path>>(_folder: P, _usernames: &[String]) -> AResult<Vec<FsItemOut>> {
    anyhow::bail!("get_items(folder, usernames) is only implemented on Unix-like systems.");
}

// ===================== Aggregated index (folders) =====================
// NOTE: This matches your current aggregated CSV shape:
//   path,user,file_count,file_size,disk_usage,latest_modified
// where sizes are stored as GiB (f64) and dates are ISO strings.

#[derive(Serialize, Debug, Clone)]
struct AggRowBin {
    file_count: u64,
    file_size: f64,       // GiB
    disk_usage: f64,      // GiB
    latest_mtime: String, // ISO date string
    users: HashSet<String>, // usernames
}

#[derive(Default, Debug, Clone)]
struct UserStats {
    file_count: u64,
    file_size: f64,  // GiB
    disk_usage: f64, // GiB
    latest_mtime: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct UserStatsJson {
    pub username: String,
    pub count: u64,
    pub size: f64,  // GiB
    pub disk: f64,  // GiB
}

#[derive(Serialize, Debug, Clone)]
pub struct FileItem {
    pub path: String,
    pub total_count: u64,
    pub total_size: f64,   // GiB
    pub total_disk: f64,   // GiB
    pub modified: String,  // ISO date string
    pub users: HashMap<String, UserStatsJson>, // keyed by username
}

#[derive(Debug, Clone)]
struct TrieNode {
    children: HashMap<String, Box<TrieNode>>,
    data: Option<AggRowBin>,
}
impl TrieNode {
    fn new() -> Self { Self { children: HashMap::new(), data: None } }
}

#[derive(Debug, Clone)]
pub struct InMemoryFSIndex {
    root: TrieNode,
    total_entries: usize,
    per_user: HashMap<(String, String), UserStats>, // (path, username)
}

impl InMemoryFSIndex {
    pub fn new() -> Self {
        Self { root: TrieNode::new(), total_entries: 0, per_user: HashMap::new() }
    }

    /// CSV columns (with header): path,user,file_count,file_size,disk_usage,latest_modified
    pub fn load_from_csv(&mut self, path: &FsPath) -> AResult<Vec<String>> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_path(path)
            .with_context(|| format!("Failed to open CSV file: {}", path.display()))?;

        let mut all_users: HashSet<String> = HashSet::new();
        let mut loaded_count = 0usize;

        for (line_no, record) in rdr.records().enumerate() {
            let record = record.with_context(|| format!("Failed to read CSV line {}", line_no + 2))?;
            if record.len() < 6 {
                continue;
            }

            let path_str = unquote_csv_field(record.get(0).unwrap_or(""));
            let username = record.get(1).unwrap_or("").trim().to_string();
            let file_count: u64 = record.get(2).unwrap_or("0").parse().unwrap_or(0);
            let file_size: f64 = record.get(3).unwrap_or("0").parse().unwrap_or(0.0);
            let disk_usage: f64 = record.get(4).unwrap_or("0").parse().unwrap_or(0.0);
            let latest_mtime: String = record.get(5).unwrap_or("").trim().to_string();

            if path_str.is_empty() || username.is_empty() {
                continue;
            }

            all_users.insert(username.clone());

            self.insert_merge(&path_str, file_count, file_size, disk_usage, &latest_mtime, &username);

            let key = (Self::canonical_key(&path_str), username.clone());
            let entry = self.per_user.entry(key).or_insert(UserStats::default());
            entry.file_count = entry.file_count.saturating_add(file_count);
            entry.file_size  += file_size;
            entry.disk_usage += disk_usage;
            if entry.latest_mtime.is_empty() || entry.latest_mtime < latest_mtime {
                entry.latest_mtime = latest_mtime.clone();
            }

            loaded_count += 1;
        }

        self.total_entries = loaded_count;

        let mut users: Vec<String> = all_users.into_iter().collect();
        users.sort();
        Ok(users)
    }

    fn insert_merge(
        &mut self,
        path: &str,
        file_count: u64,
        file_size: f64,
        disk_usage: f64,
        latest_mtime: &str,
        username: &str,
    ) {
        let components = Self::path_to_components(path);
        let mut current = &mut self.root;
        for component in components {
            current = current.children
                .entry(component)
                .or_insert_with(|| Box::new(TrieNode::new()));
        }

        match &mut current.data {
            Some(data) => {
                data.file_count = data.file_count.saturating_add(file_count);
                data.file_size  += file_size;
                data.disk_usage += disk_usage;
                if data.latest_mtime.is_empty() || data.latest_mtime.as_str() < latest_mtime {
                    data.latest_mtime = latest_mtime.to_string();
                }
                data.users.insert(username.to_string());
            }
            None => {
                let mut users = HashSet::new();
                users.insert(username.to_string());
                current.data = Some(AggRowBin {
                    file_count,
                    file_size,
                    disk_usage,
                    latest_mtime: latest_mtime.to_string(),
                    users,
                });
            }
        }
    }

    pub fn list_children(
        &self,
        dir_path: &str,
        user_filter: &Vec<String>, // [] => all users (by username)
    ) -> AResult<Vec<FileItem>> {
        let components = Self::path_to_components(dir_path);
        let mut current = &self.root;
        for component in components {
            current = current.children.get(&component)
                .ok_or_else(|| anyhow::anyhow!("Directory not found: {}", dir_path))?
                .as_ref();
        }

        let mut items = Vec::new();
        let base_path = Self::normalize_path(dir_path);

        for (child_name, child_node) in &current.children {
            if let Some(data) = &child_node.data {
                let full_path = if base_path.is_empty() || base_path == "/" {
                    format!("/{}", child_name)
                } else {
                    format!("{}/{}", base_path.trim_end_matches('/'), child_name)
                };

                let users_to_show: Vec<String> = if user_filter.is_empty() {
                    data.users.iter().cloned().collect()
                } else {
                    data.users
                        .iter()
                        .filter(|u| user_filter.contains(*u))
                        .cloned()
                        .collect()
                };

                if !user_filter.is_empty() && users_to_show.is_empty() {
                    continue;
                }

                let mut user_stats: HashMap<String, UserStatsJson> = HashMap::new();
                let mut total_count: u64;
                let mut total_size:  f64;
                let mut total_disk:  f64;
                let mut modified:    String;

                let pkey = Self::canonical_key(&full_path);

                if user_filter.is_empty() {
                    total_count = data.file_count;
                    total_size  = data.file_size;
                    total_disk  = data.disk_usage;
                    modified    = data.latest_mtime.clone();

                    for uname in users_to_show {
                        if let Some(stats) = self.per_user.get(&(pkey.clone(), uname.clone())) {
                            user_stats.insert(uname.clone(), UserStatsJson {
                                username: uname,
                                count: stats.file_count,
                                size:  stats.file_size,
                                disk:  stats.disk_usage,
                            });
                        }
                    }
                } else {
                    total_count = 0;
                    total_size  = 0.0;
                    total_disk  = 0.0;
                    modified    = String::new();

                    for uname in users_to_show {
                        if let Some(stats) = self.per_user.get(&(pkey.clone(), uname.clone())) {
                            user_stats.insert(uname.clone(), UserStatsJson {
                                username: uname.clone(),
                                count: stats.file_count,
                                size:  stats.file_size,
                                disk:  stats.disk_usage,
                            });

                            total_count = total_count.saturating_add(stats.file_count);
                            total_size  += stats.file_size;
                            total_disk  += stats.disk_usage;
                            if modified.is_empty() || modified.as_str() < stats.latest_mtime.as_str() {
                                modified = stats.latest_mtime.clone();
                            }
                        }
                    }

                    if user_stats.is_empty() {
                        continue;
                    }
                }

                items.push(FileItem {
                    path: full_path,
                    total_count,
                    total_size,
                    total_disk,
                    modified,
                    users: user_stats,
                });
            }
        }

        items.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(items)
    }

    fn path_to_components(path: &str) -> Vec<String> {
        let normalized = Self::normalize_path(path);
        normalized.split('/').filter(|s| !s.is_empty()).map(|s| s.to_string()).collect()
    }

    fn normalize_path(path: &str) -> String {
        let mut normalized = path.replace('\\', "/");
        if cfg!(windows) && normalized.len() >= 2 && normalized.chars().nth(1) == Some(':') {
            if !normalized.starts_with('/') {
                normalized = format!("/{}", normalized);
            }
        } else if !normalized.starts_with('/') && !normalized.is_empty() {
            normalized = format!("/{}", normalized);
        }
        normalized
    }

    /// Canonical key for per_user: normalized and without trailing slash (except root "/").
    fn canonical_key(path: &str) -> String {
        let mut n = Self::normalize_path(path);
        if n.len() > 1 {
            n = n.trim_end_matches('/').to_string();
        }
        n
    }
}

// ===================== Globals =====================

static FS_INDEX: OnceLock<InMemoryFSIndex> = OnceLock::new();
static USERS: OnceLock<Vec<String>> = OnceLock::new();

fn get_users() -> &'static Vec<String> {
    USERS.get().expect("User list not initialized")
}

// ===================== Web layer =====================

#[derive(Deserialize)]
struct FolderQuery {
    /// path inside the indexed tree. If omitted/empty -> "/"
    path: Option<String>,
    /// Comma-separated usernames. If omitted/empty -> all users
    users: Option<String>,
    /// Legacy alias (also usernames): ?uids=support,san
    uids: Option<String>,
}

#[derive(Deserialize)]
struct FilesQuery {
    path: Option<String>,
    /// Username filter: comma-separated "users=support,san"
    users: Option<String>,
    /// Legacy alias for usernames: "uids=support,san"
    uids: Option<String>,
}

fn parse_users_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map(|p| p.to_string())
        .collect()
}

async fn users_handler() -> impl IntoResponse {
    Json(get_users().clone())
}

/// /api/folders?path=/Users/foo&users=alice,bob  (reads from index)
/// also supports legacy ?uids=alice,bob (treated as usernames)
async fn get_folders_handler(Query(q): Query<FolderQuery>) -> impl IntoResponse {
    // normalize path
    let mut path = q.path.unwrap_or_else(|| "/".to_string());
    if path.is_empty() { path = "/".to_string(); }
    if !path.starts_with('/') { path = format!("/{}", path); }

    // Merge users + legacy uids into a single username list
    let mut merged = String::new();
    if let Some(s) = q.users.as_deref() {
        if !s.trim().is_empty() { merged.push_str(s); }
    }
    if let Some(s) = q.uids.as_deref() {
        if !s.trim().is_empty() {
            if !merged.is_empty() { merged.push(','); }
            merged.push_str(s);
        }
    }

    let user_filter: Vec<String> = if merged.trim().is_empty() {
        Vec::new() // empty => all users
    } else {
        parse_users_csv(&merged)
    };

    let index = FS_INDEX.get().expect("FS index not initialized");

    let items = match index.list_children(&path, &user_filter) {
        Ok(v) => v,
        Err(_) => Vec::new(),
    };

    Json(items)
}

/// /api/files?path=/some/dir&users=alice,bob
/// also supports legacy alias: &uids=alice,bob
/// Returns: [{ path, owner: <username>, size: <bytes>, modified: "YYYY-MM-DD" }, ...]
async fn get_files_handler(Query(q): Query<FilesQuery>) -> impl IntoResponse {
    // validate path
    let folder = match q.path.as_deref() {
        Some(p) if !p.is_empty() => p.to_string(),
        _ => return (StatusCode::BAD_REQUEST, "missing 'path' query parameter").into_response(),
    };

    // Merge users + legacy uids into a single username list
    let mut merged = String::new();
    if let Some(s) = q.users.as_deref() {
        if !s.trim().is_empty() { merged.push_str(s); }
    }
    if let Some(s) = q.uids.as_deref() {
        if !s.trim().is_empty() {
            if !merged.is_empty() { merged.push(','); }
            merged.push_str(s);
        }
    }
    let usernames: Vec<String> = if merged.trim().is_empty() {
        Vec::new() // empty => all users
    } else {
        parse_users_csv(&merged)
    };

    // run blocking scan
    let fut = tokio::task::spawn_blocking(move || get_items(folder, &usernames));

    match fut.await {
        Err(join_err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("task error: {join_err}")).into_response(),
        Ok(Err(e)) => {
            #[cfg(not(unix))]
            { (StatusCode::NOT_IMPLEMENTED, e.to_string()).into_response() }
            #[cfg(unix)]
            { (StatusCode::BAD_REQUEST, e.to_string()).into_response() }
        }
        Ok(Ok(items)) => Json(items).into_response(),
    }
}

// ===================== Bootstrap =====================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // CSV path: first CLI arg, or env CSV_PATH
    let csv_path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("CSV_PATH").ok())
        .expect("Provide CSV path as first arg or set CSV_PATH env var");

    let mut idx = InMemoryFSIndex::new();
    let users = idx.load_from_csv(FsPath::new(&csv_path))?;
    FS_INDEX.set(idx).expect("FS_INDEX already set");
    USERS.set(users).expect("USERS already set");

    // static dir (frontend build)
    let static_dir = std::env::var("STATIC_DIR").unwrap_or_else(|_| "public".to_string());

    // ServeDir directly; SPA fallback to index.html
    let frontend = ServeDir::new(&static_dir)
        .not_found_service(ServeFile::new(format!("{}/index.html", static_dir)));

    // CORS (dev)
    let cors = CorsLayer::new()
        .allow_origin([
            "http://localhost:8080".parse().unwrap(),
            "http://localhost:5173".parse().unwrap(),
            "http://127.0.0.1:5173".parse().unwrap(),
        ])
        .allow_methods([Method::GET, Method::OPTIONS])
        .allow_headers(Any);

    // API
    let api = Router::new()
        .route("/users", get(users_handler))         // Vec<String> usernames
        .route("/folders", get(get_folders_handler)) // query: users=alice,bob or uids=alice,bob
        .route("/files", get(get_files_handler));    // path + optional users/uids

    // App
    let app = Router::new()
        .nest("/api", api)        // frontend calls /api/...
        .fallback_service(frontend)
        .layer(cors);

    // Bind
    let port: u16 = std::env::var("PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(8080);
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    println!("Serving on http://{addr}  (static dir: {static_dir})");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}
