use anyhow::{Context, Result};
use std::path::{Component, Path, PathBuf, MAIN_SEPARATOR};
use redb::{Database, TableDefinition, ReadableDatabase};
use bincode::{config, decode_from_slice, encode_to_vec, Decode, Encode};
use serde::Serialize;
use std::collections::{HashSet, BTreeSet};

#[derive(Debug, Clone)]
struct FolderStats {
    file_count: u64,
    file_size: u128,
    disk_usage: u128,
    latest_mtime: i64,
    users: HashSet<String>,
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

#[derive(Encode, Decode, Serialize, Debug)]
struct AggRowBin {
    file_count: u64,
    file_size: u128,
    disk_usage: u128,
    latest_mtime: i64,
    users: Vec<String>,
}

// ----- JSON output types -----
#[derive(Serialize)]
struct FileItem {
    path: String,
    count: u64,
    size: u128,
    disk: u128,
    modified: i64,
    users: Vec<String>,
}

// Adjust these field names/types to your AggRowBin definition.
impl From<AggRowBin> for FileItem {
    fn from(row: AggRowBin) -> Self {
        // If your AggRowBin stores a set, convert to Vec.
        let users: Vec<String> = row.users.into_iter().collect();
        FileItem {
            path: String::new(), // filled later
            count: row.file_count as u64,
            size: row.file_size as u128,
            disk: row.disk_usage as u128,
            modified: row.latest_mtime,
            users,
        }
    }
}
const AGG: TableDefinition<&str, &[u8]> = TableDefinition::new("agg");


#[tauri::command]
async fn get_files(path: String) -> Result<String, String> {
    //let db_path = PathBuf::from("c:\\Dev\\statwalker\\Dev.rdb");
    let db_path = PathBuf::from("/Users/san/dev/statwalker/rs/mac.agg.rdb");
    let json = match list_children(&db_path, &path) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("Error listing children for '{}': {:?}", path, e);
            "[]".to_string() // Return empty JSON array on error
        }
    };
    Ok(json)
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

#[inline]
fn remainder_if_ascii_ci_prefix<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes()) {
        Some(&s[prefix.len()..])
    } else {
        None
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

// ----- existing single-read helper (uses bincode) -----
fn read_one(db_path: &Path, key: &str) -> Result<Option<AggRowBin>> {
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

fn list_children(db_path: &Path, dir: &str) -> Result<String> {
    let db = Database::open(db_path)?;
    let read = db.begin_read()?;
    let table = read.open_table(AGG)?;

    // ---- build canonical scan prefix (always ends with separator) ----
    let mut dir_norm = normalize_seps(dir.trim());
    if cfg!(windows) && dir_norm.ends_with(':') {
        dir_norm.push(MAIN_SEPARATOR);
    }
    let prefix = if is_root_dir(&dir_norm) {
        ensure_trailing_sep(dir_norm)
    } else {
        ensure_trailing_sep(dir_norm.trim_end_matches(|c| c == '/' || c == '\\').to_string())
    };

    // For the range + comparisons, use the same canonical form as stored keys.
    // If your DB stores Windows paths lowercased (e.g., "c:\..."), lower the prefix here.
    let cmp_prefix = if cfg!(windows) { prefix.to_ascii_lowercase() } else { prefix.clone() };
    let scan_start = cmp_prefix.clone();

    // ---- collect immediate child names (sorted, unique) ----
    let mut child_names: BTreeSet<String> = BTreeSet::new();

    for entry in table.range(scan_start.as_str()..)? {
        let (key_guard, _v) = entry?;
        let key = key_guard.value(); // &str

        // Plain, case-sensitive check against cmp_prefix
        if !key.starts_with(&cmp_prefix) {
            break;
        }

        // Slice remainder using cmp_prefix length (same string we compared with)
        let remainder = &key[cmp_prefix.len()..];

        // First segment after prefix (handles / or \)
        let child = remainder
            .split(|ch| ch == '/' || ch == '\\')
            .next()
            .unwrap_or_default();

        if !child.is_empty() {
            child_names.insert(child.to_string());
        }
    }

    // ---- build items with agg data for each child ----
    let mut items: Vec<FileItem> = Vec::with_capacity(child_names.len());
    let cfg = bincode::config::standard().with_fixed_int_encoding();

    for name in child_names {
        let full_path = format!("{}{}", prefix, name); // keep user's original-cased prefix for output

        // Try exact key first; if your DB stores Windows paths lowercased, also try the lowered form.
        let mut row_opt = table.get(full_path.as_str())?;
        if row_opt.is_none() && cfg!(windows) {
            row_opt = table.get(full_path.to_ascii_lowercase().as_str())?;
        }

        let item = if let Some(val) = row_opt {
            let (row, _): (AggRowBin, usize) = decode_from_slice(val.value(), cfg)?;
            // If you have `impl From<AggRowBin> for FileItem`, reuse it:
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
                users: Vec::new(),
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
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
