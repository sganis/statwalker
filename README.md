[![Dutopia CI](https://github.com/sganis/dutopia/actions/workflows/ci.yml/badge.svg)](https://github.com/sganis/dutopia/actions/workflows/ci.yml)

# Dutopia

**A Rust Toolkit for High-Scale Filesystem Analytics**

Dutopia turns massive filesystems into fast, filterable, **UTF-8 clean** analytics.  
It is modular — use only the components you need.

---

## Components

- **duscan** — high-performance scanner that traverses the filesystem with concurrency and streams metadata for both files and directories.  
- **duhuman** — converts machine data (epochs, uids/gids, mode bits) into human fields (local dates, usernames, octal perms).  
- **dusum** — reads `duscan` output and produces rollups by folder, user, and file-age buckets.  
- **duzip** — compresses/expands CSV ↔ Zstandard (`.zst`) binary streams.  
- **duapi** — lightweight REST API server exposing aggregated data.  

Frontend: **Svelte SPA** (for dashboards and visualization).

---

## Features

- Multi-threaded Rust scanner with near-line-rate performance.  
- Output as **CSV or binary** (Zstandard optional).  
- UTF-8 safe, cross-platform (Linux, macOS, Windows).  
- Scales to billions of files (tested on >1B files, 30PB storage).  
- Aggregated indices for instant dashboard queries.  
- Works standalone or as a full pipeline.  

---

## Output

Scanner output (CSV):

```text
INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH
````

Columns:

1. **INODE** — device identifier + inode (Linux)
2. **ATIME** — last access time (epoch seconds)
3. **MTIME** — last modified time (epoch seconds)
4. **UID** — user ID
5. **GID** — group ID
6. **MODE** — file type + permissions (octal)
7. **SIZE** — logical size in bytes
8. **DISK** — disk usage (blocks × 512)
9. **PATH** — full path (UTF-8 safe)

---

## Build

Clone and build with Cargo:

```bash
git clone https://github.com/sganis/dutopia.git
cd dutopia/rs
cargo build --release
```

Binaries are under `target/release/`:

* `duscan`
* `duhuman`
* `dusum`
* `duzip`
* `duapi`

---

## Run Examples

### Scan (walk filesystem)

```bash
./duscan /home
```

### Humanize (optional)

```bash
.duhuman home.csv
```

### Aggregate

```bash
./dusum home.csv -o home.sum.csv
```

### Compress / Decompress

```bash
# compress CSV → Zstd
./duzip home.csv

# decompress Zstd → CSV
./duzip home.zst
```

### Serve API

```bash
./duapi home.sum.csv --port 8000
```

Open the **Svelte frontend** in your browser to explore results.

---

## Contribute

Fork and submit PRs:

```bash
git clone https://github.com/sganis/dutopia.git
```


