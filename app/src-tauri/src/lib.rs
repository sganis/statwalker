use anyhow::{Context, Result as AResult};
use std::path::{Path, PathBuf, MAIN_SEPARATOR};
use redb::{Database, TableDefinition, ReadableDatabase};
use bincode::{decode_from_slice, Decode, Encode};
use serde::Serialize;
use std::collections::{HashSet, BTreeSet, HashMap};
use std::sync::{Arc, RwLock, OnceLock};
use csv::ReaderBuilder;
use tauri::{AppHandle, Emitter};
use std::fs::File;
use std::io::{self, BufRead, BufReader};

#[cfg(unix)]
use std::ffi::CStr;

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

fn count_lines<P: AsRef<Path>>(path: P) -> io::Result<usize> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut count = 0;
    for line in reader.lines() {
        line?;
        count += 1;
    }
    Ok(count)
}

#[derive(Debug, Clone)]
struct FolderStats {
    file_count: u64,
    file_size: u128,
    disk_usage: u128,
    latest_mtime: i64,
    users: HashSet<i32>,
}

impl FolderStats {
    fn new() -> Self {
        Self {
            file_count: 0,
            file_size: 0,
            disk_usage: 0,
            latest_mtime: 0,
            users: HashSet::new(),
        }
    }
}

#[derive(Encode, Decode, Serialize, Debug, Clone)]
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

// ----- JSON output types -----
#[derive(Serialize, Debug, Clone)]
struct FileItem {
    path: String,
    count: u64,
    size: u128,
    disk: u128,
    modified: i64,
    users: HashSet<i32>,
}

impl From<AggRowBin> for FileItem {
    fn from(row: AggRowBin) -> Self {
        FileItem {
            path: String::new(), // filled later
            count: row.file_count,
            size: row.file_size,
            disk: row.disk_usage,
            modified: row.latest_mtime,
            users: row.users,
        }
    }
}

// ----- IN-MEMORY TRIE IMPLEMENTATION -----
#[derive(Clone, Serialize)]
struct Progress {
  current: usize,
  total: usize,
}

#[derive(Debug, Clone)]
struct TrieNode {
    children: HashMap<String, Box<TrieNode>>,
    data: Option<AggRowBin>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            data: None,
        }
    }
}

#[derive(Debug)]
pub struct InMemoryFSIndex {
    root: TrieNode,
    total_entries: usize,
    // exact per-(path, uid) data for fast user filtering
    per_user: HashMap<(String, i32), UserStats>,
}

impl InMemoryFSIndex {
    pub fn new() -> Self {
        Self {
            root: TrieNode::new(),
            total_entries: 0,
            per_user: HashMap::new(),
        }
    }

    /// Loads normalized CSV with columns:
    /// 0:path, 1:uid, 2:file_count, 3:file_size, 4:disk_usage, 5:latest_mtime
    /// Merges rows per path (sum counts/sizes, max mtime, union users).
    pub fn load_from_csv(&mut self, path: &Path, app: AppHandle) -> AResult<HashMap<i32, String>> {
        let total = count_lines(&path)?;
        let freq = total / 100;
        app.emit("progress", Progress{current: 0, total})?;

        println!("Loading filesystem index from CSV: {}", path.display());
        let start = std::time::Instant::now();

        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .from_path(path)
            .with_context(|| format!("Failed to open CSV file: {}", path.display()))?;

        let mut all_users: HashSet<i32> = HashSet::new();
        let mut loaded_count = 0;

        for (line_no, record) in rdr.records().enumerate() {
            let record = record.with_context(|| format!("Failed to read CSV line {}", line_no + 1))?;

            if record.len() < 6 {
                eprintln!("Warning: Skipping line {} - not enough fields", line_no + 1);
                continue;
            }

            // CSV: path,uid,file_count,file_size,disk_usage,latest_mtime
            let path_str = unquote_csv_field(record.get(0).unwrap_or(""));
            let uid: i32 = record.get(1).unwrap_or("0").parse().unwrap_or(0);
            let file_count: u64 = record.get(2).unwrap_or("0").parse().unwrap_or(0);
            let file_size: u128 = record.get(3).unwrap_or("0").parse().unwrap_or(0);
            let disk_usage: u128 = record.get(4).unwrap_or("0").parse().unwrap_or(0);
            let latest_mtime: i64 = record.get(5).unwrap_or("0").parse().unwrap_or(0);

            all_users.insert(uid);

            // 1) Merge into trie totals for this path (across users)
            self.insert_merge(
                &path_str,
                file_count,
                file_size,
                disk_usage,
                latest_mtime,
                uid,
            );

            // 2) Record exact per-user stats for this path
            let key = (Self::normalize_path(&path_str), uid);
            let entry = self.per_user.entry(key).or_insert(UserStats {
                file_count: 0, file_size: 0, disk_usage: 0, latest_mtime: 0
            });
            entry.file_count = entry.file_count.saturating_add(file_count);
            entry.file_size  = entry.file_size.saturating_add(file_size);
            entry.disk_usage = entry.disk_usage.saturating_add(disk_usage);
            if latest_mtime > entry.latest_mtime { entry.latest_mtime = latest_mtime; }

            loaded_count += 1;
            if loaded_count % freq == 0 {
                app.emit("progress", Progress{current: loaded_count, total})?;
            }
        }

        let uid_name_map = resolve_usernames(&all_users);

        self.total_entries = loaded_count;
        let elapsed = start.elapsed();
        println!(
            "Loaded {} rows in {:.2}s ({:.0} rows/sec)",
            loaded_count,
            elapsed.as_secs_f64(),
            loaded_count as f64 / elapsed.as_secs_f64()
        );
        Ok(uid_name_map)
    }

    /// Insert-or-merge a normalized row into the trie.
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
                if latest_mtime > data.latest_mtime {
                    data.latest_mtime = latest_mtime;
                }
                data.users.insert(uid);
            }
            None => {
                let mut users = HashSet::new();
                users.insert(uid);
                current.data = Some(AggRowBin {
                    file_count,
                    file_size,
                    disk_usage,
                    latest_mtime,
                    users,
                });
            }
        }
    }

    pub fn get(&self, path: &str) -> Option<&AggRowBin> {
        let components = Self::path_to_components(path);
        let mut current = &self.root;
        for component in components {
            current = current.children.get(&component)?.as_ref();
        }
        current.data.as_ref()
    }

    pub fn list_children(&self, dir_path: &str) -> AResult<Vec<FileItem>> {
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

                let mut item: FileItem = data.clone().into();
                item.path = full_path;
                items.push(item);
            }
        }

        items.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(items)
    }

    /// Like list_children, but returns stats **only for a single user** (uid).
    pub fn list_children_for_user(&self, dir_path: &str, uid: i32) -> AResult<Vec<FileItem>> {
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
            // Build full child path
            let full_path = if base_path.is_empty() || base_path == "/" {
                format!("/{}", child_name)
            } else {
                format!("{}/{}", base_path.trim_end_matches('/'), child_name)
            };

            // Lookup exact per-user stats for that path
            let key = (Self::normalize_path(&full_path), uid);
            if let Some(stats) = self.per_user.get(&key) {
                // Fill FileItem using per-user stats; set users = {uid}
                let mut users = HashSet::new();
                users.insert(uid);

                items.push(FileItem {
                    path: full_path,
                    count: stats.file_count,
                    size: stats.file_size,
                    disk: stats.disk_usage,
                    modified: stats.latest_mtime,
                    users,
                });
            }
            // If user has no data for that child path, we skip it (no row).
        }

        items.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(items)
    }

    pub fn get_all_with_prefix(&self, prefix: &str) -> AResult<Vec<FileItem>> {
        let components = Self::path_to_components(prefix);
        let mut current = &self.root;

        for component in components {
            current = current.children.get(&component)
                .ok_or_else(|| anyhow::anyhow!("Path not found: {}", prefix))?
                .as_ref();
        }

        let mut items = Vec::new();
        let base_path = Self::normalize_path(prefix);
        self.collect_all_descendants(current, &base_path, &mut items);

        items.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(items)
    }

    fn collect_all_descendants(&self, node: &TrieNode, current_path: &str, items: &mut Vec<FileItem>) {
        if let Some(data) = &node.data {
            let mut item: FileItem = data.clone().into();
            item.path = current_path.to_string();
            items.push(item);
        }

        for (child_name, child_node) in &node.children {
            let child_path = if current_path.is_empty() || current_path == "/" {
                format!("/{}", child_name)
            } else {
                format!("{}/{}", current_path.trim_end_matches('/'), child_name)
            };
            self.collect_all_descendants(child_node.as_ref(), &child_path, items);
        }
    }

    fn path_to_components(path: &str) -> Vec<String> {
        let normalized = Self::normalize_path(path);
        normalized
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
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

    pub fn stats(&self) -> (usize, usize) {
        let node_count = self.count_nodes(&self.root);
        (self.total_entries, node_count)
    }

    fn count_nodes(&self, node: &TrieNode) -> usize {
        1 + node.children.values().map(|child| self.count_nodes(child)).sum::<usize>()
    }
}

// Global instance - loaded once at startup
static FS_INDEX: OnceLock<Arc<RwLock<InMemoryFSIndex>>> = OnceLock::new();

pub fn get_fs_index() -> AResult<Arc<RwLock<InMemoryFSIndex>>> {
    FS_INDEX.get()
        .ok_or_else(|| anyhow::anyhow!("FS index not initialized"))
        .map(|arc| arc.clone())
}

// ----- MEMORY-BASED TAURI COMMANDS -----

#[tauri::command]
async fn get_files_memory(path: String) -> Result<String, String> {
    let index = get_fs_index().map_err(|e| format!("Index not available: {}", e))?;
    let index = index.read().map_err(|e| format!("Failed to read index: {}", e))?;

    match index.list_children(&path) {
        Ok(items) => serde_json::to_string(&items).map_err(|e| format!("JSON serialization error: {}", e)),
        Err(e) => {
            eprintln!("Error listing children for '{}': {:?}", path, e);
            Ok("[]".to_string())
        }
    }
}

/// Same as get_files_memory, but filtered by a specific user (uid).
#[tauri::command]
async fn get_files_memory_user(path: String, uid: i32) -> Result<String, String> {
    let index = get_fs_index().map_err(|e| format!("Index not available: {}", e))?;
    let index = index.read().map_err(|e| format!("Failed to read index: {}", e))?;

    match index.list_children_for_user(&path, uid) {
        Ok(items) => serde_json::to_string(&items).map_err(|e| format!("JSON serialization error: {}", e)),
        Err(e) => {
            eprintln!("Error listing children for '{}', uid {}: {:?}", path, uid, e);
            Ok("[]".to_string())
        }
    }
}

#[tauri::command]
async fn get_file_info_memory(path: String) -> Result<String, String> {
    let index = get_fs_index().map_err(|e| format!("Index not available: {}", e))?;
    let index = index.read().map_err(|e| format!("Failed to read index: {}", e))?;

    match index.get(&path) {
        Some(data) => {
            let mut item: FileItem = data.clone().into();
            item.path = path;
            serde_json::to_string(&item).map_err(|e| format!("JSON serialization error: {}", e))
        },
        None => Ok("null".to_string())
    }
}

#[tauri::command]
async fn search_prefix_memory(prefix: String) -> Result<String, String> {
    let index = get_fs_index().map_err(|e| format!("Index not available: {}", e))?;
    let index = index.read().map_err(|e| format!("Failed to read index: {}", e))?;

    match index.get_all_with_prefix(&prefix) {
        Ok(items) => serde_json::to_string(&items).map_err(|e| format!("JSON serialization error: {}", e)),
        Err(e) => {
            eprintln!("Error searching prefix '{}': {:?}", prefix, e);
            Ok("[]".to_string())
        }
    }
}

#[tauri::command]
async fn load_db(app: AppHandle, path: String) -> Result<HashMap<i32, String>, String> {
    let mut index = InMemoryFSIndex::new();
    let path = Path::new(&path);
    let users = index.load_from_csv(path, app).map_err(|e| e.to_string())?;
    let (entries, nodes) = index.stats();
    println!("Index initialized: {} entries, {} trie nodes", entries, nodes);

    FS_INDEX
        .set(Arc::new(RwLock::new(index)))
        .map_err(|_| "Failed to initialize global FS index".to_string())?;
    Ok(users)
}

// ----- EXISTING REDB FUNCTIONS -----

const AGG: TableDefinition<&str, &[u8]> = TableDefinition::new("agg");

#[tauri::command]
async fn get_files(path: String) -> Result<String, String> {
    let db_path = PathBuf::from("/Users/san/dev/statwalker/rs/mac.agg.rdb");
    let json = match list_children_db(&db_path, &path) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("Error listing children for '{}': {:?}", path, e);
            "[]".to_string()
        }
    };
    Ok(json)
}

fn unquote_csv_field(field: &str) -> String {
    let trimmed = field.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        let inner = &trimmed[1..trimmed.len() - 1];
        inner.replace("\"\"", "\"")
    } else {
        trimmed.to_string()
    }
}

fn normalize_seps(s: &str) -> String {
    if MAIN_SEPARATOR == '/' { s.replace('\\', "/") } else { s.replace('/', "\\") }
}

fn ensure_trailing_sep(mut s: String) -> String {
    if !s.ends_with(MAIN_SEPARATOR) { s.push(MAIN_SEPARATOR); }
    s
}

fn is_root_dir(p: &str) -> bool {
    use std::path::Component;
    let mut comps = Path::new(p).components();
    match comps.next() {
        Some(Component::Prefix(_)) => matches!(comps.next(), Some(Component::RootDir)) && comps.next().is_none(),
        Some(Component::RootDir) => comps.next().is_none(),
        _ => false,
    }
}

fn first_segment(s: &str) -> &str {
    for (i, ch) in s.char_indices() {
        if ch == '/' || ch == '\\' {
            return &s[..i];
        }
    }
    s
}

fn read_one(db_path: &Path, key: &str) -> AResult<Option<AggRowBin>> {
    let db = Database::open(db_path)?;
    let read = db.begin_read()?;
    let table = read.open_table(AGG)?;
    if let Some(val) = table.get(key)? {
        let cfg = bincode::config::standard().with_fixed_int_encoding();
        let (row, _): (AggRowBin, usize) = decode_from_slice(val.value(), cfg)?;
        Ok(Some(row))
    } else {
        Ok(None)
    }
}

fn list_children_db(db_path: &Path, dir: &str) -> AResult<String> {
    let db = Database::open(db_path)?;
    let read = db.begin_read()?;
    let table = read.open_table(AGG)?;

    let mut dir_norm = normalize_seps(dir.trim());
    if cfg!(windows) && dir_norm.ends_with(':') {
        dir_norm.push(MAIN_SEPARATOR);
    }
    let prefix = if is_root_dir(&dir_norm) {
        ensure_trailing_sep(dir_norm)
    } else {
        ensure_trailing_sep(dir_norm.trim_end_matches(|c| c == '/' || c == '\\').to_string())
    };

    let cmp_prefix = if cfg!(windows) { prefix.to_ascii_lowercase() } else { prefix.clone() };
    let scan_start = cmp_prefix.clone();

    let mut child_names: BTreeSet<String> = BTreeSet::new();

    for entry in table.range(scan_start.as_str()..)? {
        let (key_guard, _v) = entry?;
        let key = key_guard.value();

        if !key.starts_with(&cmp_prefix) {
            break;
        }

        let remainder = &key[cmp_prefix.len()..];
        let child = remainder
            .split(|ch| ch == '/' || ch == '\\')
            .next()
            .unwrap_or_default();

        if !child.is_empty() {
            child_names.insert(child.to_string());
        }
    }

    let mut items: Vec<FileItem> = Vec::with_capacity(child_names.len());
    let cfg = bincode::config::standard().with_fixed_int_encoding();

    for name in child_names {
        let full_path = format!("{}{}", prefix, name);

        let mut row_opt = table.get(full_path.as_str())?;
        if row_opt.is_none() && cfg!(windows) {
            row_opt = table.get(full_path.to_ascii_lowercase().as_str())?;
        }

        let item = if let Some(val) = row_opt {
            let (row, _): (AggRowBin, usize) = decode_from_slice(val.value(), cfg)?;
            let mut fi: FileItem = row.into();
            fi.path = full_path.clone();
            fi
        } else {
            FileItem {
                path: full_path.clone(),
                count: 0,
                size: 0,
                disk: 0,
                modified: 0,
                users: HashSet::new(),
            }
        };

        items.push(item);
    }

    Ok(serde_json::to_string(&items)?)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            get_files,
            get_files_memory,
            get_files_memory_user, 
            get_file_info_memory,
            search_prefix_memory,
            load_db,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
