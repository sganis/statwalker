use anyhow::{Context, Result as AResult};
use axum::{
    extract::Query,
    http::{Method},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::Path as FsPath,
    sync::OnceLock,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};

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

// ===================== Aggregated index (folders) =====================
// CSV shape (header):
//   path,user,age,files,disk,modified
// - 'disk' is integer bytes (u64)
// - 'modified' is Unix epoch seconds (i64)
// - 'size' is not present in CSV; we mirror size=disk.

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
}
impl TrieNode {
    fn new() -> Self { Self { children: HashMap::new() } }
}

#[derive(Debug, Clone)]
pub struct InMemoryFSIndex {
    root: TrieNode,
    // Single authoritative index
    per_user_age: HashMap<(String, String, u8), Stats>, // (path, username, age)
    // For quick user discovery at a path
    users_by_path: HashMap<String, HashSet<String>>,
}

impl InMemoryFSIndex {
    pub fn new() -> Self {
        Self {
            root: TrieNode::new(),
            per_user_age: HashMap::new(),
            users_by_path: HashMap::new(),
        }
    }

    /// CSV columns (with header): path,user,age,files,disk,modified
    pub fn load_from_csv(&mut self, path: &FsPath) -> AResult<Vec<String>> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_path(path)
            .with_context(|| format!("Failed to open CSV file: {}", path.display()))?;

        let mut all_users: HashSet<String> = HashSet::new();

        for (line_no, record) in rdr.records().enumerate() {
            let record = record.with_context(|| format!("Failed to read CSV line {}", line_no + 2))?;
            if record.len() < 6 {
                continue;
            }

            let path_str           = unquote_csv_field(record.get(0).unwrap_or(""));
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
            self.insert_path(&path_str);

            // Single index: (path, user, age)
            let entry = self.per_user_age.entry((pkey, username, age)).or_insert_with(Stats::default);
            entry.file_count = entry.file_count.saturating_add(file_count);
            entry.size_bytes = entry.size_bytes.saturating_add(disk_bytes); // mirror size=disk
            entry.disk_bytes = entry.disk_bytes.saturating_add(disk_bytes);
            if latest_mtime > entry.latest_mtime {
                entry.latest_mtime = latest_mtime;
            }
        }

        let mut users: Vec<String> = all_users.into_iter().collect();
        users.sort();
        Ok(users)
    }

    fn insert_path(&mut self, path: &str) {
        let components = Self::path_to_components(path);
        let mut current = &mut self.root;
        for component in components {
            current = current.children
                .entry(component)
                .or_insert_with(|| Box::new(TrieNode::new()));
        }
    }

    /// Return folders → users → ages.
    /// - `users` filter is applied if provided
    /// - `age` parameter is *accepted* but IGNORED in the output (frontend filters it)
    pub fn list_children(
        &self,
        dir_path: &str,
        user_filter: &Vec<String>, // [] => all users
        _age_filter: Option<u8>,    // accepted but not used to filter ages in the response
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
            let available_users = match self.users_by_path.get(&pkey) {
                Some(u) => u,
                None => continue,
            };

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

            // Build users -> ages map (ALWAYS check ages 0,1,2 if present)
            let mut users_map: HashMap<String, HashMap<String, AgeMini>> = HashMap::new();

            for uname in &users_to_show {
                let mut age_map: HashMap<String, AgeMini> = HashMap::new();

                for a in [0u8, 1u8, 2u8] {
                    if let Some(s) = self.per_user_age.get(&(pkey.clone(), uname.clone(), a)) {
                        age_map.insert(a.to_string(), AgeMini {
                            count: s.file_count,
                            size:  s.size_bytes,
                            disk:  s.disk_bytes,
                            mtime: s.latest_mtime,
                        });
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

            items.push(FolderOut { path: full_path, users: users_map });
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
    pub users: HashMap<String, HashMap<String, AgeMini>>, // username -> "0"/"1"/"2" -> stats
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
    uids:  Option<String>, // legacy alias
    age:   Option<u8>,     // accepted but NOT used to filter ages in response
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

/// /api/folders?path=/&users=user1,user2&age=0
/// Always returns folders -> users -> 0/1/2 ages that exist.
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

    let items = match index.list_children(&path, &user_filter, q.age) {
        Ok(v) => v,
        Err(_) => Vec::new(),
    };

    Json(items)
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
        .route("/folders", get(get_folders_handler)); // folders -> users -> ages

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
