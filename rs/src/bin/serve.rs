// serve.rs
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
    fs::{File},
    io::{Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::OnceLock,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use memchr::memchr_iter;
use clap::{Parser, ColorChoice};

#[cfg(unix)]
use std::ffi::CStr;

#[derive(Parser, Debug)]
#[command(author, version, color = ColorChoice::Always,
    about = "Statwalker web server and UI")]
struct Args {
    /// Input CSV file path
    input: PathBuf,
    /// UI folder (defaults to STATIC_DIR env var or local public directory)
    #[arg(long, value_name="DIR", env="STATIC_DIR")]
    static_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FsItemOut {
    pub path: String,
    pub owner: String,   // username
    pub size: u64,       // bytes
    pub modified: i64    // unix
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    
    // CSV path
    let csv_path = args.input.clone();

    let static_dir: String = args
        .static_dir
        .or_else(|| std::env::var("STATIC_DIR").ok())
        .unwrap_or_else(default_static_dir);

    // ServeDir directly; SPA fallback to index.html
    let frontend = ServeDir::new(&static_dir)
        .not_found_service(ServeFile::new(format!("{}/index.html", static_dir)));

    let mut idx = InMemoryFSIndex::new();
    let users = idx.load_from_csv(Path::new(&csv_path))?;
    FS_INDEX.set(idx).expect("FS_INDEX already set");
    USERS.set(users).expect("USERS already set");

    

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
        .route("/folders", get(get_folders_handler)) // query: path, optional users/uids, optional age
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


// ===================== Helpers =====================
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

#[cfg(unix)]
pub fn get_items<P: AsRef<std::path::Path>>(
    folder: P,
    usernames: &[String],
    age_filter: Option<u8>, // 0 recent (≤60d), 1 mid (≤730d), 2 old (>730d)
) -> AResult<Vec<FsItemOut>> {
    use std::{collections::HashSet, fs};
    use std::os::unix::fs::MetadataExt;
    use chrono::{Duration, Utc};

    let filter: Option<HashSet<String>> = if usernames.is_empty() {
        None
    } else {
        Some(usernames.iter().cloned().collect())
    };

    // Precompute cutoffs once in epoch seconds (fast)
    let now = Utc::now();
    let cutoff_recent = (now - Duration::days(60)).timestamp();
    let cutoff_old    = (now - Duration::days(730)).timestamp();

    let mut out = Vec::new();

    let dir = fs::read_dir(&folder)
        .with_context(|| format!("read_dir({}) failed", folder.as_ref().display()))?;

    for entry_res in dir {
        let entry = match entry_res { Ok(e) => e, Err(_) => continue };
        let path = entry.path();

        let md = match fs::symlink_metadata(&path) { Ok(m) => m, Err(_) => continue };
        if !md.file_type().is_file() { continue; }

        let owner = username_from_uid(md.uid());
        if let Some(ref allow) = filter {
            if !allow.contains(&owner) { continue; }
        }

        let mtime = md.mtime();

        if let Some(af) = age_filter {
            let age = if mtime >= cutoff_recent { 0 }
                      else if mtime <  cutoff_old { 2 }
                      else                         { 1 };
            if age != af { continue; }
        }

        out.push(FsItemOut {
            path: path.to_string_lossy().into_owned(),
            owner,
            size: md.size(),
            modified: mtime,
        });
    }

    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

#[cfg(not(unix))]
pub fn get_items<P: AsRef<std::path::Path>>(
    _folder: P,
    _usernames: &[String],
    _age_filter: Option<u8>,
) -> AResult<Vec<FsItemOut>> {
    anyhow::bail!("get_items(folder, usernames, age_filter) is only implemented on Unix-like systems.");
}

// ===================== Aggregated index (folders) =====================
pub fn count_lines(path: &Path) -> std::io::Result<usize> {
    let mut file = File::open(path)?;
    let mut buf = [0u8; 128 * 1024]; // 128 KiB is plenty; adjust if you like
    let mut count = 0usize;
    let mut last: Option<u8> = None;

    loop {
        let n = file.read(&mut buf)?;
        if n == 0 { break; }
        count += memchr_iter(b'\n', &buf[..n]).count();
        last = Some(buf[n - 1]);
    }

    if let Some(b) = last {
        if b != b'\n' {
            count += 1; // account for final line without trailing newline
        }
    }
    Ok(count)
}

#[derive(Default, Debug, Clone)]
struct Stats {
    file_count: u64,
    size_bytes: u64,   // mirrored from disk
    disk_bytes: u64,   // from CSV
    latest_mtime: i64, // Unix seconds
}

#[derive(Debug, Clone)]
struct TrieNode {
    children: HashMap<String, Box<TrieNode>>,
    // only used to quickly discover which users exist under a path
    users: HashSet<String>,
}
impl TrieNode {
    fn new() -> Self { Self { children: HashMap::new(), users: HashSet::new() } }
}

#[derive(Debug, Clone)]
pub struct InMemoryFSIndex {
    root: TrieNode,
    total_entries: usize,
    // Single authoritative index
    per_user_age: HashMap<(String, String, u8), Stats>, // (path, username, age)
    // To know which users exist under a given path quickly
    users_by_path: HashMap<String, HashSet<String>>,
}

impl InMemoryFSIndex {
    pub fn new() -> Self {
        Self {
            root: TrieNode::new(),
            total_entries: 0,
            per_user_age: HashMap::new(),
            users_by_path: HashMap::new(),
        }
    }

    /// CSV columns (with header): path,user,age,files,disk,modified
    pub fn load_from_csv(&mut self, path: &Path) -> AResult<Vec<String>> {

        // Count total lines for progress tracking
        print!("Counting lines in {}... ", path.display());
        std::io::stdout().flush().unwrap(); 
        let total_lines = count_lines(&path)?;
        let data_lines = total_lines.saturating_sub(1);
        let progress_interval = if data_lines >= 10 { data_lines / 10 } else { 0 };
        println!("done:\nTotal lines: {}", total_lines);
        println!("Loading and building index...");

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

            let path_str           = record.get(0).unwrap_or("");
            let username           = record.get(1).unwrap_or("").trim().to_string();
            let age: u8            = record.get(2).unwrap_or("0").parse().unwrap_or(0);
            let file_count: u64    = record.get(3).unwrap_or("0").parse().unwrap_or(0);
            let disk_bytes: u64    = record.get(4).unwrap_or("0").parse().unwrap_or(0);
            let latest_mtime: i64  = record.get(5).unwrap_or("0").parse().unwrap_or(0);

            if path_str.is_empty() || username.is_empty() {
                continue;
            }

            all_users.insert(username.clone());

            // Maintain user set under this path
            let pkey = Self::canonical_key(&path_str);
            self.users_by_path.entry(pkey.clone()).or_default().insert(username.clone());

            // Insert into trie for structural navigation
            self.insert_path(&path_str, &username);

            // Single index: (path, user, age)
            let entry = self.per_user_age.entry((pkey, username, age)).or_insert_with(Stats::default);
            entry.file_count = entry.file_count.saturating_add(file_count);
            entry.size_bytes = entry.size_bytes.saturating_add(disk_bytes); // mirror size=disk
            entry.disk_bytes = entry.disk_bytes.saturating_add(disk_bytes);
            if latest_mtime > entry.latest_mtime {
                entry.latest_mtime = latest_mtime;
            }

            loaded_count += 1;
            // Show progress (approx 10% steps)
            if progress_interval > 0 && (line_no + 1) % progress_interval == 0 {
                let percent = ((line_no + 1) as f64 * 100.0 / data_lines.max(1) as f64).ceil() as u32;
                println!("{}%", percent.min(100));
            }
        }

        self.total_entries = loaded_count;

        let mut users: Vec<String> = all_users.into_iter().collect();
        users.sort();
        Ok(users)
    }

    fn insert_path(&mut self, path: &str, username: &str) {
        let components = Self::path_to_components(path);
        let mut current = &mut self.root;
        for component in components {
            current = current.children
                .entry(component)
                .or_insert_with(|| Box::new(TrieNode::new()));
            current.users.insert(username.to_string());
        }
    }

    pub fn list_children(
        &self,
        dir_path: &str,
        user_filter: &Vec<String>, // [] => all users
        age_filter: Option<u8>,     // Some(0|1|2) or None
    ) -> AResult<Vec<FolderOut>> {
        // descend to the directory node
        let components = Self::path_to_components(dir_path);
        let mut current = &self.root;
        for component in components {
            current = current.children.get(&component)
                .ok_or_else(|| anyhow::anyhow!("Directory not found: {}", dir_path))?
                .as_ref();
        }

        let mut items = Vec::new();
        let base_path = Self::normalize_path(dir_path);

        for (child_name, _child_node) in &current.children {
            let full_path = if base_path.is_empty() || base_path == "/" {
                format!("/{}", child_name)
            } else {
                format!("{}/{}", base_path.trim_end_matches('/'), child_name)
            };

            let pkey = Self::canonical_key(&full_path);

            // Which users exist under this child?
            let available_users = self.users_by_path.get(&pkey);
            if available_users.is_none() {
                continue;
            }
            let available_users = available_users.unwrap();

            // Apply user filter
            let mut users_to_show: Vec<String> = if user_filter.is_empty() {
                available_users.iter().cloned().collect()
            } else {
                available_users
                    .iter()
                    .filter(|u| user_filter.contains(*u))
                    .cloned()
                    .collect()
            };
            users_to_show.sort();

            if !user_filter.is_empty() && users_to_show.is_empty() {
                continue;
            }

            // Build users -> username -> ages map and compute totals for this folder
            let mut users_map: HashMap<String, HashMap<String, AgeMini>> = HashMap::new();
            let mut total_count: u64 = 0;
            let mut total_size:  u64 = 0;
            let mut total_disk:  u64 = 0;
            let mut modified:    i64 = 0;

            let ages_to_consider: Vec<u8> = if let Some(a) = age_filter { vec![a] } else { vec![0,1,2] };

            for uname in &users_to_show {
                let mut age_map: HashMap<String, AgeMini> = HashMap::new();

                for a in &ages_to_consider {
                    if let Some(s) = self.per_user_age.get(&(pkey.clone(), uname.clone(), *a)) {
                        age_map.insert(a.to_string(), AgeMini {
                            count: s.file_count,
                            size:  s.size_bytes,
                            disk:  s.disk_bytes,
                            mtime: s.latest_mtime,
                        });

                        total_count = total_count.saturating_add(s.file_count);
                        total_size  = total_size.saturating_add(s.size_bytes);
                        total_disk  = total_disk.saturating_add(s.disk_bytes);
                        if s.latest_mtime > modified {
                            modified = s.latest_mtime;
                        }
                    }
                }

                if !age_map.is_empty() {
                    users_map.insert(uname.clone(), age_map);
                }
            }

            // Skip folders with no matching user/age after filters
            if users_map.is_empty() {
                continue;
            }

            items.push(FolderOut {
                path: full_path,
                users: users_map,
            });
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

    fn canonical_key(path: &str) -> String {
        let mut n = Self::normalize_path(path);
        if n.len() > 1 {
            n = n.trim_end_matches('/').to_string();
        }
        n
    }
}

// ----------- Output shapes for /api/folders -----------

#[derive(Serialize, Debug, Clone)]
pub struct AgeMini {
    pub count: u64,
    pub size:  u64,  // bytes (mirrored from disk)
    pub disk:  u64,  // bytes
    pub mtime: i64,  // Unix seconds
}

#[derive(Serialize, Debug, Clone)]
pub struct FolderOut {
    pub path: String,
    pub users: HashMap<String, HashMap<String, AgeMini>>, // username -> age_string -> stats
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
    path: Option<String>,
    users: Option<String>, // "alice,bob"
    age:   Option<u8>,     // 0|1|2
}

#[derive(Deserialize)]
struct FilesQuery {
    path: Option<String>,
    users: Option<String>,
    age:   Option<u8>,     // 0|1|2
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

async fn get_folders_handler(Query(q): Query<FolderQuery>) -> impl IntoResponse {
    // normalize path
    let mut path = q.path.unwrap_or_else(|| "/".to_string());
    if path.is_empty() { path = "/".to_string(); }
    if !path.starts_with('/') { path = format!("/{}", path); }

    let usernames: Vec<String> = match q.users.as_deref() {
        Some(s) if !s.trim().is_empty() => parse_users_csv(s),
        _ => Vec::new(), // empty means "all users"
    };

    let index = FS_INDEX.get().expect("FS index not initialized");

    let items = match index.list_children(&path, &usernames, q.age) {
        Ok(v) => v,
        Err(_) => Vec::new(),
    };

    Json(items)
}

/// /api/files?path=/some/dir&users=alice,bob
async fn get_files_handler(Query(q): Query<FilesQuery>) -> impl IntoResponse {
    // validate path
    let folder = match q.path.as_deref() {
        Some(p) if !p.is_empty() => p.to_string(),
        _ => return (StatusCode::BAD_REQUEST, "missing 'path' query parameter").into_response(),
    };

    let usernames: Vec<String> = match q.users.as_deref() {
        Some(s) if !s.trim().is_empty() => parse_users_csv(s),
        _ => Vec::new(), // empty means "all users"
    };

    // run blocking scan
    let fut = tokio::task::spawn_blocking(move || get_items(folder, &usernames, q.age));

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

fn default_static_dir() -> String {
    let mut exe_dir = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
    exe_dir.pop(); // remove the binary name
    let static_dir = exe_dir.join("public");
    static_dir.to_string_lossy().into_owned()
}

