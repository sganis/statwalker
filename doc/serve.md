# Statwalker UI Server & Svelte Front-End

## A White Paper on Architecture, Data Contracts, and Performance

**Version:** 1.0
**Date:** September 2025
**Author:** SAG

---

## Executive Summary

This document describes the **Statwalker UI system**—a Rust/axum backend that powers a Svelte single-page application (SPA) for exploring large filesystem scans. The system is the interactive, visual layer in the Statwalker pipeline:

```
Statwalker (scan)  →  Resolve (humanize)  →  Aggregate (per path/user/age)  →  UI Server + SPA
```

It builds upon the guarantees and output of the first two components (see the **Statwalker** and **Resolve** white papers) and turns the aggregated CSV into fast, filtered, browsable views. The backend uses an **in-memory index** and serves minimal, targeted JSON for the front-end. The SPA renders **stacked user bars** with age filtering, path navigation, and user drill-downs, while staying responsive even on millions of rows.

---

## Design Goals

1. **Fast first paint / low latency** on large datasets (millions of rows).
2. **Deterministic, stateless API** fed from a read-only CSV index.
3. **Stable data contracts** across the pipeline (scan → resolve → aggregate → serve).
4. **Robust text handling**: path strings are **always UTF-8** (lossy converted if needed).
5. **Practical filters**: by path, user(s), and **age buckets**.
6. **Safe defaults** for production (CORS hardening, path normalization, predictable responses).

---

## System Overview

### End-to-End Flow

1. **Statwalker** scans the filesystem and emits a CSV of raw metadata.
2. **Resolve** converts numeric fields to human forms and ensures **UTF-8 paths** (invalid sequences are replaced).
3. **Aggregate** produces a **per-(path,user,age)** summary with counts, disk usage, and recency signals.
4. **UI Server (Rust + axum)** loads the aggregate CSV into an in-memory index and exposes:

   * `/api/users` – all usernames
   * `/api/folders` – aggregated stats per immediate child of a path
   * `/api/files` – live on-disk listing of actual files in a directory (Unix only)
5. **Svelte SPA** consumes those APIs and renders navigable bars and file lists.

---

## Data Contracts (Authoritative)

### Aggregate CSV → Server Loader

**Server loader expects (and normalizes) this header and column order:**

```
path,user,age,files,disk,accessed,modified
```

* `path: string` – canonicalized (‘/’-rooted, forward slashes).
* `user: string` – username.
* `age: u8` – age bucket code.
* `files: u64` – total item count (all types).
* `disk: u64` – summed bytes (already integer bytes).
* `accessed: i64` – latest **file** atime (Unix seconds) in that (path,user,age) slice.
* `modified: i64` – latest **file** mtime (Unix seconds) in that (path,user,age) slice.

> **Important compatibility note:**
> Earlier drafts used only `modified`. The server’s loader currently tolerates missing columns by defaulting to `0`, but for correctness **Aggregate should emit both `accessed` and `modified`**. This aligns the UI’s “Last file read / Updated” labels and the server’s `Age { atime, mtime }` model.

### Age Buckets (Single Source of Truth)

To keep the UI, server, and aggregator aligned:

* **Age 0**: `mtime ≤ 60 days old`
* **Age 1**: `61–730 days`
* **Age 2**: `> 730 days` **or unknown** (`mtime ≤ 0`)

> The backend uses `60` and `730` day cutoffs; ensure the **Aggregate** step matches these thresholds for consistent filtering end-to-end.

### JSON API Shapes

* **GET `/api/users` →** `string[]`
  Sorted unique usernames present in the index.

* **GET `/api/folders?path=/x/y&users=alice,bob&age=0|1|2` →**

  ```ts
  type Age = { count: number; disk: number; atime: number; mtime: number };
  type FolderOut = {
    path: string;
    users: Record<string, Record<"0"|"1"|"2", Age>>;
  };
  FolderOut[]
  ```

  Returns **immediate children** of `path` (each with per-user age maps). When `users` is empty, all users are considered. When `age` is provided, only that bucket is returned per user.

* **GET `/api/files?path=/x/y&users=alice,bob&age=0|1|2` →** *(Unix only)*

  ```ts
  type FsItemOut = {
    path: string;    // UTF-8, lossy if needed
    owner: string;
    size: number;    // bytes
    accessed: number;// unix seconds
    modified: number;// unix seconds
  };
  FsItemOut[]
  ```

  Returns **files only** (directories are skipped) under the given directory on disk, optionally filtered by users and age bucket.

---

## Backend Architecture (Rust + axum)

### Key Components

* **CSV Loader & Indexer (`InMemoryFSIndex`)**

  * Loads the CSV once at startup, stores:

    * `(path, user, age) → Stats { file_count, disk_bytes, latest_atime, latest_mtime }`
    * A **trie** of path components for enumerating children quickly
    * A map of `users_by_path` for quick per-folder user filtering
  * Uses `OnceLock` to share the index across requests without synchronization overhead.

* **Path Canonicalization**

  * Converts backslashes to slashes; forces a leading slash; trims trailing slash except at root.
  * Ensures consistent lookup keys and predictable front-end display.

* **Line Counting & Progress**

  * `memchr`-based `count_lines()` avoids materializing the file and scales linearly (high throughput on big CSVs).

* **HTTP Layer**

  * `tower_http::services::ServeDir` serves the SPA; SPA fallback to `index.html`.
  * CORS relaxed for local dev origins (5173, 8080).
    *Production guidance:* pin exact domains and **disable `Any`**.

* **Live File Listing (Unix)**

  * `/api/files` uses `spawn_blocking` to scan the real filesystem for the requested folder (owner resolve via `getpwuid`).
  * Applies server-side age and user filters compatible with the aggregate buckets.
  * Returns **UTF-8** paths via `.to_string_lossy()`.

### Performance & Complexity

* **Load time**: O(N) over CSV lines with low constant factors (csv crate, `memchr` for counting).
* **Memory**: Approximately `O(unique (path,user,age))` plus trie overhead; 8–16 bytes per integer counter, path strings deduped as HashMap keys.
* **Query time**:

  * `/api/folders`: O(#children of path × #users × #ages considered). Typical millisecond responses thanks to in-memory hash lookups.
  * `/api/files`: I/O-bound on the target folder; runs off the async scheduler via `spawn_blocking`.

### Reliability & Security

* **Robust parsing**: CSV reads use `unwrap_or(“…")` defaults—invalid or short rows are skipped gracefully.
* **UTF-8 only output**: All served paths are **lossy converted** to valid UTF-8; no panics on invalid byte sequences.
* **Least privilege / blast radius**: Server never mutates disk; it reads one CSV and lists files on demand.
* **CORS**: development settings are permissive; production should:

  * Use strict allowed origins,
  * Limit methods to GET,
  * Consider rate-limiting if exposed beyond a trusted network.

---

## Front-End Architecture (Svelte SPA)

### Interaction Model

* **Navigation**: Path bar, **Back/Forward/Up/Home**, typed navigation, copy-path.
* **Filtering**: Dropdowns for **sort (Disk / Files)**, **age buckets**, and **user selection** (Svelecte with custom color renderer).
* **Two synchronized panes**:

  * **Folders**: stacked bars by user; width proportional to the chosen metric (disk or count).
  * **Files**: live listing beneath the folders for the current path (`/api/files`), sorted by the same metric.

### State & Rendering

* **Single-flight fetch** to avoid redundant calls during quick interactions.
* **Skeleton UI** while loading; SPA remains interactive.
* **Deterministic color assignment** per user (hash → palette); colors cached on first load.
* **UTF-8 everywhere**: paths are plain strings; invalid input from upstream has already been lossily converted by the backend.

### Accessibility & UX

* Keyboard support on path entry; tooltip is viewport-aware and debounced.
* Progress indicators (initial load), percent bars with precise tooltips (value and share%).
* Labels:

  * **Modified** uses the aggregated `mtime`,
  * **Last file read** uses aggregated `atime` (if newer than `mtime`).

---

## Alignment with Statwalker & Resolve

* **Path Fidelity**:

  * Statwalker writes raw bytes; Resolve **reads bytes** but **emits UTF-8** via `from_utf8_lossy` for the `PATH`.
  * The server and SPA assume **UTF-8**; this is consistent and safe for display.

* **Semantics of Folder Age**:

  * Aggregation defines a folder’s recency by **latest file mtime under it (any depth)**—**not** the directory’s own mtime.
  * The server and SPA rely on these semantics when showing “Updated …” and assigning age buckets.

* **Age Buckets**:

  * Ensure Aggregate uses the **same cutoffs** the server uses (60 days / 730 days).
  * Bucket `2` is the catch-all for **unknown** or very old entries.

---

## Notable Implementation Details

* **In-Memory Trie**: Enables `list_children(dir)` to be a pure hash/trie walk with no disk I/O.
* **Per-Path User Presence Map**: Avoids scanning the entire index when applying user filters for a folder.
* **OnceLock**: The index and user list are **immutable singletons** after startup; no lock contention at runtime.
* **Unix/Non-Unix split**:

  * Live file listing is implemented only on Unix (owner via `getpwuid`, atime/mtime via `MetadataExt`).
  * Non-Unix returns `501 Not Implemented` for `/api/files`; aggregate browsing still works (CSV-backed).

---

## Performance Considerations

* **Back-end**

  * CSV load throughput: dominated by filesystem + CSV decode; `memchr` accelerates pre-counting for UX.
  * `/api/folders`: strictly memory-bound; scales with children of the current node.
  * `/api/files`: directory-size dependent; runs in a blocking pool; results sorted.

* **Front-end**

  * Uses simple O(N) sorts for folders/files; datasets shown per page are measured in hundreds, not millions (index is pre-aggregated).
  * Bars and tooltips are DOM-light; no canvas/WebGL required.

---

## Deployment & Operations

* **Static assets**: served from `STATIC_DIR` (env) or the co-located `public/` folder.
* **Port**: `PORT` env var or default `8080`.
* **Containerization**: build a static binary for the server; host the SPA files beside it; mount the aggregate CSV read-only.
* **Monitoring**: basic stdout logs on startup; add request metrics (e.g., `tower_http::trace`) if needed.

---

## Risks & Recommendations

1. **Schema drift** between Aggregate and Server

   * **Action**: pin the 7-column header (`path,user,age,files,disk,accessed,modified`) in both docs and tests; server already tolerates short rows but they’ll degrade “last read”/“updated” accuracy.

2. **Age cutoffs drift**

   * **Action**: centralize cutoffs in Aggregate and Server (constants or config), and expose them in `/api/meta` for the SPA if you want to show labels like “2 months” / “2 years”.

3. **CORS** too permissive for prod

   * **Action**: restrict origins; consider auth if exposed on shared networks.

4. **/api/files scalability** on very large directories

   * **Action**: optional pagination / max count; lazy loading; or show only top-K by metric.

5. **Windows support for live listing**

   * **Action**: add a Windows implementation (owner resolution via Win32 APIs); keep lossy UTF-8 conversion on output.

---

## Future Enhancements

* **/api/meta** endpoint: expose cutoffs, build version, and dataset stats.
* **Server-side search**: prefix/path search over the trie, with paging.
* **Saved views**: bookmark filters (path, users, ages) and export dashboards.
* **AuthZ**: user-scoped views if running in multi-tenant environments.
* **Compression**: enable gzip/br for JSON routes and static assets.

---

## Conclusion

The Statwalker UI server and SPA deliver a **fast, unified** view on top of Statwalker and Resolve outputs. The combination of a **pre-aggregated, in-memory index**, **clean HTTP contracts**, and a **minimalist Svelte UI** yields excellent responsiveness on large filesets while preserving correctness around non-UTF-8 paths and the subtle semantics of folder age.

Aligning **CSV schema** and **age cutoffs** across the pipeline is the single most important operational detail to maintain. With these contracts pinned, the system scales cleanly and provides a strong user experience for compliance, cost, and hygiene workflows on large storage estates.
