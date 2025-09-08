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
use std::ffi::CStr;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

// ===================== File/Dir scanner (Unix) =====================

#[derive(Debug, Clone, Serialize)]
pub struct FsItem {
    pub path: String,
    pub size: u64,
    pub modified: i64, // seconds since epoch
    pub mode: u32,     // unix perms
    pub uid: u32,
    pub gid: u32,
}

/// List only regular files in `folder` owned by `uid` (non-recursive).
#[cfg(unix)]
pub fn get_items<P: AsRef<Path>>(folder: P, uid: u32) -> AResult<Vec<FsItem>> {
    let mut out = Vec::new();

    let dir = fs::read_dir(&folder)
        .with_context(|| format!("read_dir({}) failed", folder.as_ref().display()))?;

    for entry_res in dir {
        let entry = match entry_res {
            Ok(e) => e,
            Err(_) => continue, // skip unreadable entries
        };
        let path = entry.path();

        // Don't follow symlinks; we only want regular files.
        let md = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        if !md.file_type().is_file() {
            continue;
        }

        if md.uid() != uid {
            continue;
        }

        out.push(FsItem {
            path: path.to_string_lossy().into_owned(),
            size: md.size(),
            modified: md.mtime(),
            mode: md.mode(),
            uid: md.uid(),
            gid: md.gid(),
        });
    }

    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

#[cfg(not(unix))]
pub fn get_items<P: AsRef<Path>>(_folder: P, _uid: u32) -> AResult<Vec<FsItem>> {
    anyhow::bail!("get_items(folder, uid) is only implemented on Unix-like systems.");
}

/// List all regular files (no owner filtering), non-recursive.
#[cfg(unix)]
pub fn get_items_all<P: AsRef<Path>>(folder: P) -> AResult<Vec<FsItem>> {
    let mut out = Vec::new();

    let dir = fs::read_dir(&folder)
        .with_context(|| format!("read_dir({}) failed", folder.as_ref().display()))?;

    for entry_res in dir {
        let entry = match entry_res {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = entry.path();

        let md = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        if !md.file_type().is_file() {
            continue;
        }

        out.push(FsItem {
            path: path.to_string_lossy().into_owned(),
            size: md.size(),
            modified: md.mtime(),
            mode: md.mode(),
            uid: md.uid(),
            gid: md.gid(),
        });
    }

    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

#[cfg(not(unix))]
pub fn get_items_all<P: AsRef<Path>>(_folder: P) -> AResult<Vec<FsItem>> {
    anyhow::bail!("get_items_all(folder) is only implemented on Unix-like systems.");
}

// ===================== Username resolution (for CSV index) =====================

#[cfg(unix)]
fn get_username_from_uid(uid: u32) -> String {
    unsafe {
        let passwd = libc::getpwuid(uid);
        if passwd.is_null() {
            return "UNK".to_string();
        }
        let name_ptr = (*passwd).pw_name;
        if name_ptr.is_null() {
            return "UNK".to_string();
        }
        match CStr::from_ptr(name_ptr).to_str() {
            Ok(name) => name.to_string(),
            Err(_) => "UNK".to_string(),
        }
    }
}

#[cfg(unix)]
fn resolve_usernames(all_users: &HashSet<i32>) -> HashMap<i32, String> {
    let mut uid_name_map = HashMap::new();
    for &uid in all_users {
        if uid < 0 {
            continue;
        }
        let uname = get_username_from_uid(uid as u32);
        uid_name_map.insert(uid, uname);
    }
    uid_name_map
}

#[cfg(not(unix))]
fn resolve_usernames(all_users: &HashSet<i32>) -> HashMap<i32, String> {
    let mut uid_name_map = HashMap::new();
    for &uid in all_users {
        uid_name_map.insert(uid, "UNK".to_string());
    }
    uid_name_map
}

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

// ===================== Core data types (index) =====================

#[derive(Serialize, Debug, Clone)]
struct AggRowBin {
    file_count: u64,
    file_size: u128,
    disk_usage: u128,
    latest_mtime: i64,
    users: HashSet<i32>,
}

#[derive(Default, Debug, Clone, Copy)]
struct UserStats {
    file_count: u64,
    file_size: u128,
    disk_usage: u128,
    latest_mtime: i64,
}

#[derive(Serialize, Debug, Clone)]
struct UserStatsJson {
    username: String,
    count: u64,
    size: u128,
    disk: u128,
}

#[derive(Serialize, Debug, Clone)]
struct FileItemStacked {
    path: String,
    total_count: u64,
    total_size: u128,
    total_disk: u128,
    modified: i64,
    users: HashMap<i32, UserStatsJson>,
}

// ===================== In-memory trie index (still used for /folders & /users) =====================

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
    per_user: HashMap<(String, i32), UserStats>, // per-(path, uid)
}

impl InMemoryFSIndex {
    pub fn new() -> Self {
        Self { root: TrieNode::new(), total_entries: 0, per_user: HashMap::new() }
    }

    /// CSV columns: path, uid, file_count, file_size, disk_usage, latest_mtime
    pub fn load_from_csv(&mut self, path: &FsPath) -> AResult<HashMap<i32, String>> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .from_path(path)
            .with_context(|| format!("Failed to open CSV file: {}", path.display()))?;

        let mut all_users: HashSet<i32> = HashSet::new();
        let mut loaded_count = 0usize;

        for (line_no, record) in rdr.records().enumerate() {
            let record = record.with_context(|| format!("Failed to read CSV line {}", line_no + 1))?;
            if record.len() < 6 {
                continue;
            }

            let path_str = unquote_csv_field(record.get(0).unwrap_or(""));
            let uid: i32 = record.get(1).unwrap_or("0").parse().unwrap_or(0);
            let file_count: u64 = record.get(2).unwrap_or("0").parse().unwrap_or(0);
            let file_size: u128 = record.get(3).unwrap_or("0").parse().unwrap_or(0);
            let disk_usage: u128 = record.get(4).unwrap_or("0").parse().unwrap_or(0);
            let latest_mtime: i64 = record.get(5).unwrap_or("0").parse().unwrap_or(0);

            all_users.insert(uid);

            self.insert_merge(&path_str, file_count, file_size, disk_usage, latest_mtime, uid);

            // per-user stats with canonical key
            let key = (Self::canonical_key(&path_str), uid);
            let entry = self.per_user.entry(key).or_insert(UserStats::default());
            entry.file_count = entry.file_count.saturating_add(file_count);
            entry.file_size  = entry.file_size.saturating_add(file_size);
            entry.disk_usage = entry.disk_usage.saturating_add(disk_usage);
            if latest_mtime > entry.latest_mtime { entry.latest_mtime = latest_mtime; }

            loaded_count += 1;
        }

        self.total_entries = loaded_count;
        Ok(resolve_usernames(&all_users))
    }

    fn insert_merge(
        &mut self,
        path: &str,
        file_count: u64,
        file_size: u128,
        disk_usage: u128,
        latest_mtime: i64,
        uid: i32,
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
                data.file_size  = data.file_size.saturating_add(file_size);
                data.disk_usage = data.disk_usage.saturating_add(disk_usage);
                if latest_mtime > data.latest_mtime { data.latest_mtime = latest_mtime; }
                data.users.insert(uid);
            }
            None => {
                let mut users = HashSet::new();
                users.insert(uid);
                current.data = Some(AggRowBin { file_count, file_size, disk_usage, latest_mtime, users });
            }
        }
    }

    pub fn list_children(
        &self,
        dir_path: &str,
        uid_name_map: &HashMap<i32, String>,
        user_filter: &Vec<i32>, // [] => all users
    ) -> AResult<Vec<FileItemStacked>> {
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

                let users_to_show: Vec<i32> = if user_filter.is_empty() {
                    data.users.iter().copied().collect()
                } else {
                    data.users
                        .iter()
                        .copied()
                        .filter(|uid| user_filter.contains(uid))
                        .collect()
                };

                if !user_filter.is_empty() && users_to_show.is_empty() {
                    continue;
                }

                let mut user_stats: HashMap<i32, UserStatsJson> = HashMap::new();
                let mut total_count: u64;
                let mut total_size:  u128;
                let mut total_disk:  u128;
                let mut modified:    i64;

                let pkey = Self::canonical_key(&full_path);

                if user_filter.is_empty() {
                    total_count = data.file_count;
                    total_size  = data.file_size;
                    total_disk  = data.disk_usage;
                    modified    = data.latest_mtime;

                    for uid in users_to_show {
                        if let Some(stats) = self.per_user.get(&(pkey.clone(), uid)) {
                            let username = uid_name_map.get(&uid).map(|s| s.as_str()).unwrap_or("UNK");
                            user_stats.insert(uid, UserStatsJson {
                                username: username.to_string(),
                                count: stats.file_count,
                                size:  stats.file_size,
                                disk:  stats.disk_usage,
                            });
                        }
                    }
                } else {
                    total_count = 0;
                    total_size  = 0;
                    total_disk  = 0;
                    modified    = 0;

                    for uid in users_to_show {
                        if let Some(stats) = self.per_user.get(&(pkey.clone(), uid)) {
                            let username = uid_name_map.get(&uid).map(|s| s.as_str()).unwrap_or("UNK");
                            user_stats.insert(uid, UserStatsJson {
                                username: username.to_string(),
                                count: stats.file_count,
                                size:  stats.file_size,
                                disk:  stats.disk_usage,
                            });

                            total_count = total_count.saturating_add(stats.file_count);
                            total_size  = total_size.saturating_add(stats.file_size);
                            total_disk  = total_disk.saturating_add(stats.disk_usage);
                            if stats.latest_mtime > modified { modified = stats.latest_mtime; }
                        }
                    }

                    if user_stats.is_empty() {
                        continue;
                    }
                }

                items.push(FileItemStacked {
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
static USER_NAME_MAP: OnceLock<HashMap<i32, String>> = OnceLock::new();

fn get_users() -> &'static HashMap<i32, String> {
    USER_NAME_MAP.get().expect("User map not initialized")
}

// ===================== Web layer =====================

#[derive(Deserialize)]
struct FolderQuery {
    /// path inside the indexed tree. If omitted/empty -> "/"
    path: Option<String>,
    /// Comma-separated UIDs. If omitted/empty -> all users
    uids: Option<String>,
}

#[derive(Deserialize)]
struct FilesQuery {
    path: Option<String>,
    /// Accept both "uid" and "uids" for convenience (CSV list).
    uid: Option<String>,
    uids: Option<String>,
}

fn parse_uids_i32(s: &str) -> Vec<i32> {
    s.split(',').filter_map(|p| p.trim().parse::<i32>().ok()).collect()
}
fn parse_uids_u32(s: &str) -> Vec<u32> {
    s.split(',').filter_map(|p| p.trim().parse::<u32>().ok()).collect()
}

async fn users_handler() -> impl IntoResponse {
    Json(get_users().clone())
}

/// NEW: /api/folders?path=/Users/foo&uids=0,43  (reads from index)
async fn get_folders_handler(Query(q): Query<FolderQuery>) -> impl IntoResponse {
    // normalize path
    let mut path = q.path.unwrap_or_else(|| "/".to_string());
    if path.is_empty() { path = "/".to_string(); }
    if !path.starts_with('/') { path = format!("/{}", path); }

    // user filter (i32 for index)
    let user_filter: Vec<i32> = q
        .uids
        .as_deref()
        .map(parse_uids_i32)
        .unwrap_or_default(); // empty => all users

    let index = FS_INDEX.get().expect("FS index not initialized");
    let user_map = USER_NAME_MAP.get().expect("User map not initialized");

    let items = match index.list_children(&path, user_map, &user_filter) {
        Ok(v) => v,
        Err(_) => Vec::new(),
    };

    Json(items)
}

/// /api/files?path=/some/dir&uid=0,1000 (or &uids=...)
async fn get_files_handler(Query(q): Query<FilesQuery>) -> impl IntoResponse {
    // validate path
    let folder = match q.path.as_deref() {
        Some(p) if !p.is_empty() => p.to_string(),
        _ => return (StatusCode::BAD_REQUEST, "missing 'path' query parameter").into_response(),
    };

    // accept uid or uids (CSV)
    let uids_csv = q.uid.or(q.uids).unwrap_or_default();
    let uids: Vec<u32> = if uids_csv.trim().is_empty() {
        Vec::new()
    } else {
        parse_uids_u32(&uids_csv)
    };

    // run blocking scan(s)
    let fut = if uids.is_empty() {
        tokio::task::spawn_blocking(move || get_items_all(folder))
    } else {
        tokio::task::spawn_blocking(move || {
            let mut acc: Vec<FsItem> = Vec::new();
            for uid in uids {
                match get_items(&folder, uid) {
                    Ok(mut v) => acc.append(&mut v),
                    Err(_) => continue,
                }
            }
            acc.sort_by(|a, b| a.path.cmp(&b.path));
            Ok::<_, anyhow::Error>(acc)
        })
    };

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
    USER_NAME_MAP.set(users).expect("USER_NAME_MAP already set");

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
        .route("/users", get(users_handler))
        .route("/folders", get(get_folders_handler)) // ← querystring version, reads index
        .route("/files", get(get_files_handler));    // ← filesystem scan

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
