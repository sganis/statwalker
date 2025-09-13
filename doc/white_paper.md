# Statwalker: A Five-Stage Platform for High-Scale Filesystem Analytics

**Components:** Walk (scanner) · Resolve (optional humanizer) · Aggregate (folder·user·age rollups) · Serve (REST API) · Frontend (Svelte SPA)
**Languages:** Rust (Walk, Resolve, Aggregate, Serve) · TypeScript/Svelte (Frontend)
**Audience:** Infra / Storage / SecOps / Data Engineering
**Status:** Production-ready; evolving

---

## 0) Executive Summary

Statwalker is a modular toolchain for turning very large filesystems into fast, filterable, **UTF-8 clean** analytics. It separates concerns so each stage is simple and fast:

1. **Walk** traverses the filesystem with high concurrency and streams a compact CSV with POSIX-like metadata for **both files and directories**.
2. **Resolve** *(optional)* converts machine data (epochs, uids/gids, mode bits) into human fields (local dates, usernames, types, octal perms). Ideal for BI tools; **not required** by the later stages.
3. **Aggregate** reads **Walk output** and produces **(folder, user, age)** rollups with totals (files, on-disk bytes) and **latest file** access/change timestamps.
4. **Serve** loads the Aggregate CSV into memory, builds an index, and exposes low-latency **REST JSON** for the UI.
5. **Frontend** is a Svelte SPA that visualizes distribution by user/age and lists files in the selected folder.

**Why it works at scale**

* batched traversal (16k entries per batch), shared MPMC queue, atomic “in-flight” termination
* large write buffers (32 MiB shards; 8 MiB staged flush; 16 MiB merge)
* byte-first parsing and path handling (no unnecessary Strings)
* explicit **encoding policy**: internal stages tolerate non-UTF-8 paths; **external outputs guarantee UTF-8** via lossy replacement

---

## 1) End-to-End Architecture

```
           ┌────────────┐       ┌────────────┐       ┌──────────────┐       ┌───────────┐       ┌────────────┐
Filesystem │    Walk    │  CSV  │  Resolve   │  CSV  │  Aggregate    │  CSV  │   Serve   │  JSON │  Frontend  │
   (POSIX) └─────┬──────┘  ───► └─────┬──────┘  ───► └──────┬───────┘  ───► └────┬──────┘  ───► └──────┬─────┘
                  │                  (optional)                 │                   │                     │
                  │                                            │                   │                     │
                  ▼                                            ▼                   ▼                     ▼
        Sharded buffered writers                      In-memory folder/user/age   /api/folders, /api/files,
         + merge to single CSV                         index (trie + hash maps)    /api/users, static SPA
```

**Contract boundaries (canonical):**

* **Walk → CSV**: `INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH` (9 fields)
* **Aggregate → CSV**: `path,user,age,files,disk,accessed,modified` (7 fields) ← **Serve expects this**
* **Serve → JSON**: documented in §4.3; paths are always **UTF-8**

---

## 2) Data & Semantics (single source of truth)

### 2.1 Scanner (Walk) CSV schema

```
INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH
```

* **INODE**: `"dev-ino"` on Unix; `"0-0"` on Windows
* **ATIME / MTIME**: i64 seconds since epoch
* **UID / GID**: u32 (Unix); `0/0` on Windows
* **MODE**: u32 POSIX bits. Unix = real; Windows = synthesized (file/dir bit + rwx heuristic)
* **SIZE**: logical bytes
* **DISK**: on-disk bytes

  * Unix: `st_blocks * 512`
  * Windows: approximate `(len + 511)/512 * 512` (cluster size not consulted)
* **PATH**:

  * Unix: **raw bytes** (may be non-UTF-8)
  * Windows: UTF-8; verbatim prefixes normalized in downstream tools

**CSV correctness:** quote only when necessary; escape quotes as `""`. On Unix, quoting operates on **bytes**.

### 2.2 Aggregated (folder,user,age) schema

```
path,user,age,files,disk,accessed,modified
```

* **path**: folder path in **UTF-8** (lossy for invalid bytes). See normalization in §3.3.
* **user**: username (Unix via `getpwuid`, else numeric string; `"UNK"` on failure).
* **age**: bucket code `{0,1,2}` based on **file mtime**:

  * `0` = ≤ **60** days
  * `1` = **61–730** days
  * `2` = **>730** days **or unknown** (`mtime ≤ 0`)
* **files**: total entries contributing to the group (files, dirs, links, …).
* **disk**: sum of **DISK** bytes.
* **accessed / modified**: **latest file** atime/mtime in the group (directories’ own mtimes **do not** affect recency).

> **Recency semantics:** Folder recency is the maximum mtime/atime among **files under that folder at any depth**. Directory mtimes change on insert/delete and are **ignored**.

### 2.3 Encoding policy

* **Inside** Walk/Aggregate: path **bytes** may be non-UTF-8; no panics; no forced reencoding.
* **At outputs** (Resolve CSV, Aggregate CSV, Serve JSON, SPA): paths are **UTF-8** via `from_utf8_lossy` (replacement char `�` for invalid sequences).

---

## 3) Components (deep dive)

### 3.1 Walk — high-throughput scanner (Rust)

**Concurrency model**

* Shared **MPMC** queue (`crossbeam::channel`) of `Task::{Dir, Files, Shutdown}`
* Atomic **in-flight** counter increments before enqueue; decremented after processing
* Sentinel thread monitors `in_flight==0` → sends `Shutdown` N times (N = workers)
* Worker count: default `min(48, max(4, 2×CPU))` (I/O-bound heuristic)

**Batching & buffering**

* Directory enumeration batched: **FILE\_CHUNK = 16 384** names per `Task::Files`
* Per-worker shard writer: **32 MiB** `BufWriter`
* In-worker staging buffer: flush at **8 MiB** (`FLUSH_BYTES`)
* Final merge writer: **16 MiB**

**Hot-path formatting / parsing**

* `itoa::Buffer` in thread-locals for u32/u64/i64 → zero allocation number→ASCII
* CSV path field (Unix): written as bytes; smart quoting for `,`, `"`, `\n`, `\r`

**Platform specifics**

* **Unix**: `symlink_metadata` (does **not** follow symlinks); real `dev/ino/mode/uid/gid/blocks` via `MetadataExt`
* **Windows**:

  * `mode`: set S\_IFREG/S\_IFDIR; always owner read; copy owner perms to group/other; set write if not read-only; set exec for `.exe/.bat/.cmd/.com/.scr/.ps1/.vbs`; dirs get exec
  * `dev/ino/uid/gid`: `0`
  * `blocks/disk`: approximated as above

**Output**

* One CSV row for **every directory and file**.
* `--sort` option: loads all lines into memory, lexicographically sorts, writes. For testing only.

**Reliability**

* Errors counted (not classified); traversal continues
* Final merge currently writes directly to destination; future improvement: temp file + atomic rename

---

### 3.2 Resolve — humanized CSV (optional, Rust)

**Purpose:** BI-friendly CSV while preserving original numeric fields downstream.

**Transforms**

* **ACCESSED / MODIFIED**: local `YYYY-MM-DD` from epochs (fallback `"0001-01-01"`)
* **USER / GROUP**: Unix via `getpwuid`/`getgrgid`; else numeric strings
* **TYPE**: from `MODE` masks (FILE, DIR, LINK, SOCK, PIPE, BDEV, CDEV, UNK)
* **PERM**: `(mode & 0o7777)` as octal string
* **PATH**: **UTF-8 only** via `from_utf8_lossy` (safe for dashboards)

**Performance**

* Streaming: one input record → one output record
* Small caches: timestamp→date, uid→name, gid→name, mode→type/perm

---

### 3.3 Aggregate — folder·user·age rollups (Rust)

**Input:** Walk CSV (preferred). Compatible with Resolve if numeric columns are present.

**Core algorithm**

* Read each row as **bytes** (CSV byte records).
* Parse numeric fields with tolerant, fast routines (recommended: `atoi` on trimmed ASCII slices; default to `0` on errors/overflow).
* `sanitize_mtime(now, mtime)`: if `mtime > now + 1 day` ⇒ `0` (unknown)
* Compute **age bucket** from **file mtime** using the canonical 60/730 thresholds.
* Resolve `uid → username` with a cache (`"UNK"` on failure).
* For path bytes, compute all **ancestor folders** (normalize `\`→`/`; treat `C:\…` as `/C:/…`).
* For each `(ancestor, user, age)` group, update:

  * `files += 1`, `disk += DISK`,
  * `modified = max(modified, file.mtime)`,
  * `accessed = max(accessed, file.atime)`.
    **Directories and links count toward `files` and `disk`,** but **only files** influence `accessed/modified`.

**Data structure**

* `HashMap<(Vec<u8>, String, u8), UserStats { file_count, disk_usage, latest_mtime, latest_atime }>`
* `HashSet<u32>` for UIDs that resolve to `"UNK"` → written as `<input_stem>.unk.csv`

**Output**

* Deterministically sorted by `(path_bytes, user, age)`
* **UTF-8 path** via lossy conversion at write time
* CSV header: `path,user,age,files,disk,accessed,modified`

**Line counting UX**

* `memchr`-based `count_lines()` (fast; supports progress estimates on startup)

---

### 3.4 Serve — REST API & static SPA hosting (Rust, axum)

**Startup**

* `--input` Aggregate CSV path
* `--static-dir` (env `STATIC_DIR` honored)
* Load CSV once, build:

  * **Trie** of path components to list immediate children cheaply
  * **per\_user\_age**: `(pathKey, user, age) → Stats { files, disk, latest_atime, latest_mtime }`
  * **users\_by\_path**: `pathKey → {users…}`

**Path canonicalization**

* Normalize `\`→`/`
* Ensure leading `/`
* Trim trailing `/` except root
* Path key = canonicalized path (string)

**Endpoints (JSON; UTF-8)**

* `GET /api/users` → `string[]` (sorted)

* `GET /api/folders?path=/x/y&users=alice,bob&age=0|1|2` →

  ```json
  [
    {
      "path": "/x/y/child",
      "users": {
        "alice": { "0": { "count": 12, "disk": 12345, "atime": 1700000001, "mtime": 1700000000 } },
        "bob":   { "1": { "count": 3,  "disk": 67890,  "atime": 1690000000, "mtime": 1680000000 } }
      }
    },
    ...
  ]
  ```

  *Returns immediate children of `path`. `users` filter optional (empty = all). `age` filter optional (if present, only that bucket per user).*

* `GET /api/files?path=/x/y&users=…&age=…` *(Unix only)* →

  ```json
  [
    {"path":"/x/y/a.txt","owner":"alice","size":123,"accessed":1700000000,"modified":1700000000},
    ...
  ]
  ```

  *Scans the real directory (files only), filters by username and **age bucket**. Paths are UTF-8 via `to_string_lossy()`.*

**Performance**

* `/api/folders`: pure memory; O(children × users × ages)
* `/api/files`: I/O-bound; offloaded with `spawn_blocking`

**Security & ops**

* CORS relaxed for local dev (`5173`, `8080`). **For prod, pin origins** and consider TLS/headers/compression.
* Serve static SPA with fallback to `index.html`.
* `PORT` env var or default `8080`.

---

### 3.5 Frontend — Svelte SPA

**Major features**

* **Folder view**: stacked bars by user; width metric toggle (disk / count)
* **Files view**: live listing under current folder (Unix only), aligned with same metric
* **Filters**: age (All/0/1/2), user dropdown (Svelecte with color swatches), path navigation (Home/Back/Forward/Up)
* **Usability**: skeleton loading, single-flight fetch, path copy, viewport-aware tooltips, deterministic user colors

**API usage**

* Loads `/api/users` once
* For current path + filters, fetches **both** `/api/folders` and `/api/files` (age filter forwarded to both)
* Aggregates `Age {count,disk,atime,mtime}` across selected ages per user for display

**Important UI note**

* Ensure the reducer uses **`atime`** for “Last file read” and **`mtime`** for “Updated”. (An older snippet mistakenly reused `mtime` for both; the fix is to read `s.atime` for access.)

---

## 4) CLI & build profiles

### 4.1 Walk (scanner)

```
USAGE:
  walk [OPTIONS] [root]

ARGS:
  root                  Root folder (default: ".") — canonicalized before scanning

OPTIONS:
  -o, --output FILE     Output CSV (default: "<canonical-root>.csv")
  -t, --threads N       Workers (default: min(48, max(4, 2×CPU)))
      --sort            In-memory sort for deterministic diffs (testing only)
      --skip SUBSTR     Skip any directory whose full path contains SUBSTR
```

**Release profile (recommended for all Rust binaries):**

```toml
[profile.release]
lto = true
codegen-units = 1
panic = "abort"
opt-level = 3
```

### 4.2 Resolve

```
USAGE:
  resolve <input> [-o <file>]
```

Outputs `<stem>.res.csv` by default.

### 4.3 Aggregate

```
USAGE:
  aggregate <input> [-o <file>]
```

Outputs `<stem>.agg.csv` (and `<stem>.unk.csv` for unknown UIDs).

### 4.4 Serve

```
USAGE:
  serve --input <agg.csv> [--static-dir DIR]

ENV:
  STATIC_DIR   UI asset folder (if not provided as flag)
  PORT         Port (default 8080)
```

---

## 5) Performance & scaling

### 5.1 What drives throughput

* **Walk**: I/O parallelism, batching (16k entries), layered buffering, zero-alloc formatters, byte-level CSV writing
* **Resolve**: streaming; small caches; no reformatting of large integers
* **Aggregate**: single pass; byte parsing (`atoi` recommended); lossy UTF-8 *only at write*
* **Serve**: memory-resident index; O(1) hash lookups + trie; minimal JSON

### 5.2 Sizing guidance

Let **G** be the number of distinct `(path,user,age)` groups.

* **Aggregate memory** ≈ `G × (key + Stats + HashMap overhead)`
  Rule of thumb: \~100–200 B per group (keys dominate; depends on path length & allocator).
* **Serve memory** ≈ similar to Aggregate output + trie nodes + maps.

If **G** approaches tens of millions:

* shard Aggregate by top-level folder; concatenate results
* (future) spill intermediate groups to disk and external sort/merge

### 5.3 Benchmarking methodology (reproducible)

* Fixed dataset (document path count & depth distribution)
* Cold cache vs warm cache runs
* Collect: elapsed time, files scanned, CSV bytes, errors, RSS, files/sec
* System settings: high-performance power plan; disable background indexers
* Compare **Walk** vs. baseline tools with the same traversal scope

*(Numbers vary by hardware/filesystem; publish your rubric alongside any charts.)*

---

## 6) Reliability, security, privacy

* **Read-only**: no content mutation; metadata only
* **Graceful failure**: Walk counts errors; Aggregate skips malformed rows; default-to-zero parsing keeps the pipeline flowing
* **Encoding safety**: Internal bytes tolerated; **external outputs are UTF-8 only**
* **Privacy**: paths & usernames may be sensitive → protect CSVs and the API; consider masking rules if required
* **CORS**: restrict in production; consider auth if exposed off-LAN
* **Crash-safety improvement** (backlog): temp-file + atomic rename when finalizing Walk output

---

## 7) Testing strategy (what we cover)

**Unit**

* Path ancestor expansion (Unix/Windows styles; duplicate slashes; trailing slashes; non-UTF-8)
* ASCII trimming & `atoi` parsing; overflow → default 0
* `sanitize_mtime` (future-dated → 0)
* Age bucketing (60/730 cutoffs, boundary days)
* CSV quoting/escaping of arbitrary bytes (Walk)

**Integration**

* Mini Walk → Aggregate round-trip with non-UTF-8 paths; output is valid UTF-8
* Aggregate produces deterministic ordering
* Serve loads a synthetic Aggregate CSV; `/api/folders` and `/api/files` return expected shapes & filters

**E2E**

* Serve + SPA smoke test (dev CORS); verify bars, counts, and labels across filters

---

## 8) Operability & deployment

* **Build once, run anywhere**: ship `serve` binary + `public/` UI folder + `*.agg.csv`
* **Container**: multistage build; copy binary & UI; mount CSV read-only
* **Reverse proxy**: TLS termination, gzip/br compression of JSON & static assets
* **Observability**: add `tower_http::trace` for request spans; expose `/api/meta` (future) with dataset stats (rows, groups, age cutoffs, build sha)
* **Scheduling**: cron the Walk → Aggregate pipeline; rotate outputs; keep last N snapshots for diffing

---

## 9) Governance & evolution

* **SemVer** across CSV/JSON contracts

  * Breaking change to Aggregate CSV (e.g., new columns) → **major**
  * Serve should reject unknown schemas or downgrade gracefully
* **Schema docs** co-located with binaries; JSON OpenAPI stub (future)
* **Licensing**: MIT or Apache-2.0 recommended (aligns with Rust ecosystem)

---

## 10) Appendix: Design choices & rationale

* **Directories as rows (Walk)**: operationally useful, but **excluded from recency** because dir mtime reflects structure churn, not content change.
* **File-only recency (Aggregate/Serve/UI)**: aligns with “when did content change/get read?” questions.
* **Bytes-first ingestion**: preserves correctness for edge-case encodings; defers UTF-8 to the edges where it’s required.
* **Age buckets 60/730**: intuitive labels (“recent \~2 months”, “not too old \~2 years”, “old/unknown”) and stable across time; keep constants centralized.
* **Batched queue over work-stealing**: simpler termination & good enough balance for I/O workloads.

---

## 11) Quickstart recipes

**Scan, aggregate, serve, browse**

```bash
# 1) Scan (fastest path)
walk /data -o /tmp/data.csv

# 2) Aggregate (folder·user·age)
aggregate /tmp/data.csv -o /tmp/data.agg.csv

# 3) Serve + UI
serve --input /tmp/data.agg.csv --static-dir ./public
# open http://localhost:8080
```

**Analyst-friendly CSV (optional)**

```bash
resolve /tmp/data.csv -o /tmp/data.res.csv
# Load in Spotfire/Tableau; paths are guaranteed UTF-8
```

---

## 12) Reference constants (current builds)

* **Walk**

  * `FILE_CHUNK = 16384`
  * `FLUSH_BYTES = 8 * 1024 * 1024`
  * Shard writer buffer ≈ 32 MiB; merge writer ≈ 16 MiB
  * Threads default = `min(48, max(4, 2×CPU))`
* **Age thresholds** (Aggregate/Serve/UI)

  * Recent ≤ **60** days
  * Not too old **61–730** days
  * Old/Unknown **>730** or `mtime ≤ 0`

*(Keep these constants aligned in code and docs.)*

---

### Closing

Statwalker’s strength is its **tight contracts** and **modularity**. Walk is ruthlessly fast; Resolve makes CSVs pleasant for BI; Aggregate compacts oceans of files into meaningful per-folder/user/age slices; Serve answers queries in memory; and the Svelte SPA makes the result obvious and actionable.

Publish this white paper alongside the binaries, and you give users everything they need to trust the numbers, wire up dashboards, and scale confidently.
