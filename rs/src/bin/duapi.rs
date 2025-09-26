// duapi.rs
use anyhow::{Context, Result};
use axum::{
    extract::Query,
    http::{Method, StatusCode},
    response::IntoResponse,
    routing::{get, post},
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
    time::{SystemTime,Duration},
    net::TcpStream,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use memchr::memchr_iter;
use clap::{Parser, ColorChoice};
use colored::Colorize;
use jsonwebtoken::{encode, Header};
use dutopia::{
    auth::{platform, AuthError, AuthPayload, AuthBody, Claims, keys},
    util::print_about,
};

#[cfg(unix)]
use std::ffi::CStr;

#[derive(Parser, Debug)]
#[command(version, color = ColorChoice::Auto,
    about="Disk usage API server with web UI"
)]
struct Args {
    /// Input CSV file path
    input: PathBuf,
    /// UI folder (defaults to STATIC_DIR env var or local public directory)
    #[arg(short, long, value_name="DIR", env="STATIC_DIR")]
    static_dir: Option<String>,
    /// Port number (defaults to PORT env var or 8080)
    #[arg(short, long, env="PORT")]
    port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsItemOut {
    pub path: String,
    pub owner: String,   // username
    pub size: u64,       // bytes
    pub accessed: i64,    // unix
    pub modified: i64,    // unix
}

// ----------- Output shapes for /api/folders -----------
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Age {
    pub count: u64,
    pub disk:  u64,  // bytes
    pub atime: i64,  // Unix seconds
    pub mtime: i64,  // Unix seconds
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FolderOut {
    pub path: String,
    pub users: HashMap<String, HashMap<String, Age>>, // username -> age_string -> stats
}

static FS_INDEX: OnceLock<InMemoryFSIndex> = OnceLock::new();
static USERS: OnceLock<Vec<String>> = OnceLock::new();

#[tokio::main]
async fn main() -> Result<()> {
    
    print_about();

    dotenvy::dotenv().ok();
    if std::env::var("JWT_SECRET").is_err() {
        eprintln!("{}","Warning: JWT_SECRET env var is not set, using default (unsafe)".yellow());
        unsafe {
            std::env::set_var("JWT_SECRET", "1234567890abcdef");
        }
    }  

    let args = Args::parse();
    let csv_path = args.input.clone();
    let static_dir: String = args
        .static_dir
        .or_else(|| std::env::var("STATIC_DIR").ok())
        .unwrap_or_else(default_static_dir);    
    let port = args.port.or_else(|| std::env::var("PORT").ok().and_then(|s| s.parse().ok())).unwrap_or(8080);
    if is_port_taken(port) {
        eprintln!("{}",format!("Error: Port {port} is already in use. Try another port with --port or PORT env var.").red());
        std::process::exit(1);
    }
    
    match std::env::var("ADMIN_GROUP") {
        Ok(g) => {
            println!("ADMIN_GROUP={g}");
        }
        Err(_) => {
            eprintln!("{}","Warning: ADMIN_GROUP env var is not set.".yellow());
        }
    }
        
    let mut idx = InMemoryFSIndex::new();
    let users = idx.load_from_csv(Path::new(&csv_path))?;

    FS_INDEX.set(idx).expect("FS_INDEX already set");
    USERS.set(users).expect("USERS already set");

    // CORS (dev)
    let cors = CorsLayer::new()
        .allow_origin([
            "http://localhost:5173".parse().unwrap(),
        ])
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any);

    // API
    let api = Router::new()
        .route("/login", post(login_handler))
        .route("/users", get(users_handler))         // Vec<String> usernames
        .route("/folders", get(get_folders_handler)) // query: path, optional users/uids, optional age
        .route("/files", get(get_files_handler));    // path + optional users/uids

    let frontend = ServeDir::new(&static_dir)
        .not_found_service(ServeFile::new(format!("{}/index.html", static_dir)));

    // App
    let app = Router::new()
        .nest("/api", api)        // frontend calls /api/...
        .fallback_service(frontend)
        .layer(cors);

    // Bind
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    println!("Serving on http://{addr}  (static dir: {static_dir})");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}


pub fn is_port_taken(port: u16) -> bool {
    let addrs = [
        format!("127.0.0.1:{port}"),
        format!("[::1]:{port}"),
    ];
    for a in addrs {
        if TcpStream::connect_timeout(&a.parse().unwrap(), Duration::from_millis(120)).is_ok() {
            return true;
        }
    }
    false
}


/// PSOT /api/login
async fn login_handler(Json(payload): Json<AuthPayload>) -> Result<Json<AuthBody>, AuthError> {
    // Check if the user sent the credentials
    if payload.username.is_empty() || payload.password.is_empty() {
        return Err(AuthError::MissingCredentials);
    }
    
    if !platform::verify_user(&payload.username, &payload.password) {
        return Err(AuthError::WrongCredentials);
    }

    const TTL_SECONDS: u64 = 24 * 60 * 60; // 1 day
    let exp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + TTL_SECONDS;
    
    let admins: HashSet<String> = std::env::var("ADMIN_GROUP")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect();

    let claims = Claims {
        sub: payload.username.to_owned(),
        is_admin: admins.contains(&payload.username.trim().to_ascii_lowercase()),
        exp: exp.try_into().unwrap(),
    };
    println!("login success: {:?}", &claims);

    // Create the authorization token
    let token = encode(&Header::default(), &claims, &keys().encoding)
        .map_err(|_| AuthError::TokenCreation)?;

    // Send the authorized token
    Ok(Json(AuthBody::new(token)))
}

/// GET /api/users
async fn users_handler(claims: Claims) -> impl IntoResponse {
    // println!("GET /api/users as={} admin={}", claims.sub, claims.is_admin);

    if claims.is_admin {
        let users = get_users().clone();
        println!("200 OK /api/users count={}", users.len());
        Json(users)
    } else {
        let me = vec![claims.sub.clone()];
        println!("200 OK /api/users self={:?}", me);
        Json(me)
    }
}


/// GET /api/folders?path=/some/dir&users=alice,bob&age=1
async fn get_folders_handler(claims: Claims, Query(q): Query<FolderQuery>) -> impl IntoResponse {
    // println!(
    //     "GET /api/folders raw path={:?} users={:?} age={:?} as={} admin={}",
    //     q.path, q.users, q.age, claims.sub, claims.is_admin
    // );

    // normalize path
    let mut path = q.path.unwrap_or_else(|| "/".to_string());
    if path.is_empty() { path = "/".to_string(); }
    if !path.starts_with('/') { path = format!("/{}", path); }

    // parse users (empty => "all users")
    let requested: Vec<String> = match q.users.as_deref() {
        Some(s) if !s.trim().is_empty() => parse_users_csv(s),
        _ => Vec::new(),
    };
    // println!(
    //     "GET /api/folders normalized path={} requested_users={:?} age={:?}",
    //     path, requested, q.age
    // );

    // authorization
    if !claims.is_admin {
        if requested.is_empty() || requested.len() != 1 || requested[0] != claims.sub {
            println!("403 Forbidden /api/folders path={} requested_users={:?}", path, requested);
            return AuthError::Forbidden.into_response();
        }
    }

    let index = FS_INDEX.get().expect("FS index not initialized");
    let items = match index.list_children(&path, &requested, q.age) {
        Ok(v) => {
            println!("200 OK /api/folders path={} items={}", path, v.len());
            v
        }
        Err(e) => {
            println!("list_children ERROR /api/folders path={} err={}", path, e);
            Vec::new()
        }
    };

    Json(items).into_response()
}

/// GET /api/files?path=/some/dir&users=alice,bob&age=1
async fn get_files_handler(claims: Claims, Query(q): Query<FilesQuery>) -> impl IntoResponse {
    // println!(
    //     "GET /api/files raw path={:?} users={:?} age={:?} as={} admin={}",
    //     q.path, q.users, q.age, claims.sub, claims.is_admin
    // );

    // validate path
    let folder = match q.path.as_deref() {
        Some(p) if !p.is_empty() => p.to_string(),
        _ => {
            println!("400 Bad Request /api/files missing 'path'");
            return (StatusCode::BAD_REQUEST, "missing 'path' query parameter").into_response();
        }
    };

    // parse users (empty => "all users")
    let requested: Vec<String> = match q.users.as_deref() {
        Some(s) if !s.trim().is_empty() => parse_users_csv(s),
        _ => Vec::new(),
    };

    // authorization
    if !claims.is_admin {
        if requested.is_empty() || requested.len() != 1 || requested[0] != claims.sub {
            println!("403 Forbidden /api/files path={} requested_users={:?}", folder, requested);
            return AuthError::Forbidden.into_response();
        }
    }

    let age = q.age;
    //println!("GET /api/files scan folder={} requested_users={:?} age={:?}", folder, requested, age);

    // run blocking scan
    let fut = tokio::task::spawn_blocking(move || get_items(folder, &requested, age));

    match fut.await {
        Err(join_err) => {
            println!("500 Task Join Error /api/files err={join_err}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("task error: {join_err}")).into_response()
        }
        Ok(Err(e)) => {
            #[cfg(not(unix))]
            {
                println!("501 Not Implemented /api/files err={}", e);
                (StatusCode::NOT_IMPLEMENTED, e.to_string()).into_response()
            }
            #[cfg(unix)]
            {
                println!("400 Bad Request /api/files err={}", e);
                (StatusCode::BAD_REQUEST, e.to_string()).into_response()
            }
        }
        Ok(Ok(items)) => {
            println!("200 OK /api/files items={}", items.len());
            Json(items).into_response()
        }
    }
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
) -> Result<Vec<FsItemOut>> {
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

        let atime = md.atime();
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
            accessed: atime,
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
) -> Result<Vec<FsItemOut>> {
    anyhow::bail!("get_items(folder, usernames, age_filter) is only implemented on Unix-like systems.");
}

// ===================== Aggregated index (folders) =====================
pub fn count_lines(path: &Path) -> Result<usize> {
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
    disk_bytes: u64,   // from CSV
    latest_atime: i64, // Unix seconds
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
    pub fn load_from_csv(&mut self, path: &Path) -> Result<Vec<String>> {
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
            let latest_atime: i64  = record.get(5).unwrap_or("0").parse().unwrap_or(0);
            let latest_mtime: i64  = record.get(6).unwrap_or("0").parse().unwrap_or(0);

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
            entry.disk_bytes = entry.disk_bytes.saturating_add(disk_bytes);
            if latest_atime > entry.latest_atime {
                entry.latest_atime = latest_atime;
            }
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
    ) -> Result<Vec<FolderOut>> {
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
            let mut users_map: HashMap<String, HashMap<String, Age>> = HashMap::new();
            let mut total_count: u64 = 0;
            let mut total_disk:  u64 = 0;
            let mut accessed:    i64 = 0;
            let mut modified:    i64 = 0;

            let ages_to_consider: Vec<u8> = if let Some(a) = age_filter { vec![a] } else { vec![0,1,2] };

            for uname in &users_to_show {
                let mut age_map: HashMap<String, Age> = HashMap::new();

                for a in &ages_to_consider {
                    if let Some(s) = self.per_user_age.get(&(pkey.clone(), uname.clone(), *a)) {
                        age_map.insert(a.to_string(), Age {
                            count: s.file_count,
                            disk:  s.disk_bytes,
                            atime: s.latest_atime,
                            mtime: s.latest_mtime,
                        });

                        total_count = total_count.saturating_add(s.file_count);
                        total_disk  = total_disk.saturating_add(s.disk_bytes);
                        if s.latest_atime > accessed {
                            accessed = s.latest_atime;
                        }
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

fn default_static_dir() -> String {
    let mut exe_dir = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
    exe_dir.pop(); // remove the binary name
    let static_dir = exe_dir.join("public");
    eprintln!("{}",format!("Using default static dir: {}", static_dir.display() ).yellow());
    static_dir.to_string_lossy().into_owned()
}


#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Query;
    use axum::response::IntoResponse;
    use axum::body::to_bytes;
    use serial_test::serial;
    use serde_json::Value;
    use std::io::Write;
    use tempfile::{tempdir, NamedTempFile};
    
    const TEST_BODY_LIMIT: usize = 2 * 1024 * 1024; // 2 MiB is plenty for these payloads

    /// Build a tiny CSV and initialize FS_INDEX + USERS exactly once.
    fn init_index_once() {
        if FS_INDEX.get().is_some() {
            return;
        }

        // CSV columns: path,user,age,files,disk,atime,mtime
        let mut f = NamedTempFile::new().expect("tmp csv");
        writeln!(
            f,
            "path,user,age,files,disk,atime,mtime\n\
             /,alice,0,2,100,1700000000,1700000100\n\
             /,bob,1,1,50,1600000000,1600000100\n\
             /docs,alice,2,3,300,1500000000,1500000050\n"
        )
        .unwrap();
        let p = f.into_temp_path();

        let mut idx = InMemoryFSIndex::new();
        let users = idx.load_from_csv(p.as_ref()).expect("load_from_csv");
        FS_INDEX.set(idx).expect("FS_INDEX set once");
        USERS.set(users).expect("USERS set once");
    }

    #[test]
    fn test_parse_users_csv() {
        let v = parse_users_csv(" alice, bob ,, carol ");
        assert_eq!(v, vec!["alice", "bob", "carol"]);
        assert!(parse_users_csv(" ,, ,").is_empty());
    }

    #[test]
    fn test_count_lines_variants() {
        let mut f = NamedTempFile::new().unwrap();
        // two lines, trailing newline
        write!(f, "a\nb\n").unwrap();
        assert_eq!(count_lines(f.path()).unwrap(), 2);

        let mut g = NamedTempFile::new().unwrap();
        // two lines, no trailing newline
        write!(g, "a\nb").unwrap();
        assert_eq!(count_lines(g.path()).unwrap(), 2);
    }

    #[test]
    fn test_normalize_and_canonical() {
        assert_eq!(InMemoryFSIndex::normalize_path("foo/bar"), "/foo/bar");
        assert_eq!(InMemoryFSIndex::canonical_key("/foo/bar/"), "/foo/bar");
        assert_eq!(
            InMemoryFSIndex::path_to_components("/a/b/c"),
            vec!["a", "b", "c"]
        );
    }

    #[tokio::test]
    #[serial] // FS_INDEX/USERS are global
    async fn test_users_handler_admin_and_user() {
        init_index_once();
        // Admin should receive full list
        let admin = Claims {
            sub: "root".to_string(),
            is_admin: true,
            exp: 9_999_999_999usize,
        };
        let resp_admin = users_handler(admin).await.into_response();
        assert_eq!(resp_admin.status(), StatusCode::OK);
        let body = to_bytes(resp_admin.into_body(), TEST_BODY_LIMIT).await.unwrap();
        let list: Vec<String> = serde_json::from_slice(&body).unwrap();
        assert!(list.contains(&"alice".to_string()));
        assert!(list.contains(&"bob".to_string()));

        // Non-admin should receive only self
        let user = Claims {
            sub: "alice".to_string(),
            is_admin: false,
            exp: 9_999_999_999usize,
        };
        let resp_user = users_handler(user).await.into_response();
        let body = to_bytes(resp_user.into_body(), TEST_BODY_LIMIT).await.unwrap();
        let list: Vec<String> = serde_json::from_slice(&body).unwrap();
        assert_eq!(list, vec!["alice".to_string()]);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_folders_handler_authz_and_filters() {
        init_index_once();

        // Non-admin asking "all users" -> Forbidden
        let non_admin = Claims {
            sub: "alice".into(),
            is_admin: false,
            exp: 9_999_999_999usize,
        };
        let q_all = FolderQuery {
            path: Some("/".into()),
            users: None, // empty means "all"
            age: None,
        };
        let resp = get_folders_handler(non_admin.clone(), Query(q_all)).await.into_response();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // Non-admin asking for self only -> OK
        let q_self = FolderQuery {
            path: Some("/".into()),
            users: Some("alice".into()),
            age: None,
        };
        let resp = get_folders_handler(non_admin, Query(q_self)).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), TEST_BODY_LIMIT).await.unwrap();
        // Expect JSON array of FolderOut
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v.is_array());

        // Admin asking all -> OK, has children like "/docs"
        let admin = Claims {
            sub: "root".into(),
            is_admin: true,
            exp: 9_999_999_999usize,
        };
        let q_admin_all = FolderQuery {
            path: Some("/".into()),
            users: None,
            age: None,
        };
        let resp = get_folders_handler(admin, Query(q_admin_all)).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), TEST_BODY_LIMIT).await.unwrap();
        let arr: Vec<FolderOut> = serde_json::from_slice(&body).unwrap();
        assert!(arr.iter().any(|it| it.path == "/docs"));
        // Check that at least one user has data in "/docs"
        let docs = arr.into_iter().find(|it| it.path == "/docs").unwrap();
        assert!(!docs.users.is_empty());
    }

    #[tokio::test]
    async fn test_get_files_handler_bad_path() {
        let claims = Claims {
            sub: "any".into(),
            is_admin: true,
            exp: 9_999_999_999usize,
        };
        let q = FilesQuery {
            path: None, // BAD
            users: None,
            age: None,
        };
        let resp = get_files_handler(claims, Query(q)).await.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_get_files_handler_unix_admin_ok() {
        // Make a temp dir with one file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("a.txt");
        std::fs::write(&file_path, b"hi").unwrap();

        let claims = Claims {
            sub: "root".into(),
            is_admin: true,
            exp: 9_999_999_999usize,
        };
        let q = FilesQuery {
            path: Some(dir.path().to_string_lossy().into()),
            users: None, // all users (allowed for admin)
            age: None,
        };

        let resp = get_files_handler(claims, Query(q)).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), TEST_BODY_LIMIT).await.unwrap();
        let items: Vec<FsItemOut> = serde_json::from_slice(&body).unwrap();
        assert_eq!(items.len(), 1);
        assert!(items[0].path.ends_with("a.txt"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_get_files_handler_unix_non_admin_forbidden() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("b.txt");
        std::fs::write(&file_path, b"hi").unwrap();

        let claims = Claims {
            sub: "alice".into(),
            is_admin: false,
            exp: 9_999_999_999usize,
        };
        // Requesting "all users" -> forbidden for non-admin
        let q = FilesQuery {
            path: Some(dir.path().to_string_lossy().into()),
            users: None,
            age: None,
        };
        let resp = get_files_handler(claims, Query(q)).await.into_response();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[cfg(not(unix))]
    #[tokio::test]
    async fn test_get_files_handler_non_unix_not_implemented() {
        let tmp = tempdir().unwrap();
        let claims = Claims {
            sub: "root".into(),
            is_admin: true,
            exp: 9_999_999_999usize,
        };
        let q = FilesQuery {
            path: Some(tmp.path().to_string_lossy().into()),
            users: None,
            age: None,
        };
        let resp = get_files_handler(claims, Query(q)).await.into_response();
        // handler maps get_items() error to 501 when not unix
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

   
    #[tokio::test]
    async fn test_login_missing_credentials() {
        // No need to hit PAM/fake auth to test this branch
        let bad1 = AuthPayload {
            username: "".into(),
            password: "x".into(),
        };
        let err1 = login_handler(Json(bad1)).await.unwrap_err();
        assert!(matches!(err1, AuthError::MissingCredentials));

        let bad2 = AuthPayload {
            username: "x".into(),
            password: "".into(),
        };
        let err2 = login_handler(Json(bad2)).await.unwrap_err();
        assert!(matches!(err2, AuthError::MissingCredentials));
    }

    #[tokio::test]
    #[serial]
    async fn test_list_children_filters_and_ages() {
        init_index_once();
        let idx = FS_INDEX.get().unwrap();

        // all users under "/" (admin-like call)
        let items = idx.list_children("/", &Vec::new(), None).unwrap();
        assert!(items.iter().any(|it| it.path == "/docs"));

        // filter by alice only
        let items_alice = idx
            .list_children("/", &vec!["alice".into()], None)
            .unwrap();
        assert!(items_alice.iter().any(|it| it.path == "/docs"));
        // in "/docs", expect only "alice" present
        let docs = items_alice
            .into_iter()
            .find(|it| it.path == "/docs")
            .unwrap();
        assert!(docs.users.contains_key("alice"));

        // age filter: only age 2 under "/docs" (from fixture CSV)
        let items_age2 = idx
            .list_children("/", &Vec::new(), Some(2))
            .unwrap();
        let docs2 = items_age2.into_iter().find(|it| it.path == "/docs").unwrap();
        // Age map keys are strings "0","1","2"
        let alice_ages = docs2.users.get("alice").unwrap();
        assert!(alice_ages.contains_key("2"));
    }
}
