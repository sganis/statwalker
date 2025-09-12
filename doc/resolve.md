Here’s the concise take first, then the full white paper.

**TL;DR (what Resolve does + key behaviors)**

* Reads **Statwalker** CSV as **byte records** (tolerates non-UTF-8 PATHs).
* Emits a **humanized CSV** with columns: `INODE, ACCESSED, MODIFIED, USER, GROUP, TYPE, PERM, SIZE, DISK, PATH`.
* Converts `ATIME/MTIME` (epoch seconds) → **local date `YYYY-MM-DD`** using `chrono` (`Utc.timestamp_opt(..).with_timezone(&Local)`), cached.
* Maps `UID/GID` → **names** on Unix via `libc` (`getpwuid`, `getgrgid`); otherwise uses numeric strings.
* Derives **TYPE** from `MODE` bits; **PERM** as octal `mode & 0o7777`.
* **PATH** is written as **UTF-8** with `from_utf8_lossy` (invalid bytes become `�`).
* Streams line-by-line; caches dedupe repeated conversions; robust error contexts via `anyhow`.

---

# Resolve: Humanizing Statwalker CSV

## Architecture, Design, and Transformation Semantics

**Version:** 1.0
**Date:** September 2025
**Author:** SAG

---

## Executive Summary

**Resolve** is a lightweight, streaming post-processor for **Statwalker** output. It ingests Statwalker’s raw, platform-faithful CSV (which may include non-UTF-8 paths on Unix) and produces a **clean, analyst-friendly** CSV with human names, type labels, octal permissions, and normalized dates in local time. The tool prioritizes **fidelity** (no lossy parsing of numeric fields), **robustness** (byte-wise CSV ingestion), and **throughput** (O(n) streaming with small, effective caches).

---

## 1. Input → Output Contract

### 1.1 Upstream (Statwalker) Schema (input)

```
INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH
```

* `INODE`           Opaque string (e.g., `"2049-1234567"`).
* `ATIME`,`MTIME`   Unix seconds (i64).
* `UID`,`GID`       u32 (Unix values; `0` on Windows).
* `MODE`            u32 POSIX-like bits (real on Unix; synthesized on Windows).
* `SIZE`            Decimal bytes.
* `DISK`            Decimal bytes (blocks×512 from Statwalker).
* `PATH`            **Raw bytes on Unix**, UTF-8 on Windows.

### 1.2 Resolve Output Schema

```
INODE,ACCESSED,MODIFIED,USER,GROUP,TYPE,PERM,SIZE,DISK,PATH
```

* `ACCESSED`,`MODIFIED`  Local date strings `YYYY-MM-DD`.
* `USER`,`GROUP`         Unix name via `libc`; else numeric strings.
* `TYPE`                 Derived from `MODE` (FILE, DIR, LINK, SOCK, PIPE, BDEV, CDEV, UNK).
* `PERM`                 Octal permissions: `mode & 0o7777` → e.g., `755`.
* `SIZE`,`DISK`          **Passed through** from input (no reformat).
* `PATH`                 **UTF-8 string**; invalid bytes replaced (U+FFFD) by `from_utf8_lossy`.

---

## 2. Architecture Overview

```
┌──────────────────────┐      ┌─────────────────────┐      ┌─────────────────────────┐
│ CSV Reader (bytes)   │      │ Transform Pipeline  │      │ CSV Writer (auto-quote) │
│ - csv::ReaderBuilder │ ───► │ - time/user/group   │ ───► │ - write_byte_record      │
│ - byte_records()     │      │ - type/perm         │      │ - UTF-8 PATH             │
└──────────────────────┘      │ - PATH lossy UTF-8  │      └─────────────────────────┘
                              │ - small caches      │
                              └─────────────────────┘
```

**Streaming design**: one record in, one record out; no global buffering.
**Error model**: `anyhow::Context` wraps I/O and parse steps with precise line numbers.

---

## 3. Transformation Semantics

### 3.1 Timestamps → Local Dates

* Build UTC from epoch seconds via `Utc.timestamp_opt(ts, 0)`, then `.with_timezone(&Local)`, formatted as `%Y-%m-%d`.
* Invalid/ambiguous epochs → `"0001-01-01"`.
* **Cache**: `HashMap<i64, String>` keyed by timestamp to avoid repeated formatting.

### 3.2 UID/GID → Names

* **Unix**: `getpwuid` / `getgrgid`; non-UTF-8 names fall back to `"UNK"`.
* **Non-Unix**: numeric strings (no platform calls).
* **Caches**: `HashMap<u32, String>` for users and groups.

### 3.3 TYPE and PERM from MODE

* **TYPE**: mask using `S_IFMT` and map to labels (FILE, DIR, LINK, SOCK, PIPE, BDEV, CDEV, UNK).
* **PERM**: `(mode & 0o7777)` formatted in octal (e.g., `"0755"` without leading `0` in current code).
* **Caches**: keyed by full `mode` value for both type and perm.

### 3.4 PATH normalization (UTF-8 only on output)

* Input: read as **raw bytes** (`byte_records`); tolerate non-UTF-8.
* Output: `String::from_utf8_lossy(path_b)` ensures valid UTF-8; bad sequences → `�`.
* Writer (csv crate) applies quoting/escaping as needed; no manual unquoting required.

### 3.5 Numeric fields

* `SIZE`, `DISK`: **passed through** as input byte slices; no parse/reformat (avoids overhead and preserves exact textual form).
* `ATIME`, `MTIME`, `UID`, `GID`, `MODE`: parsed from bytes with tolerant UTF-8 decode (`unwrap_or("0")` on bad UTF-8).

---

## 4. CLI & Defaults

```
USAGE:
  resolve [OPTIONS] <input>

ARGS:
  <input>           Path to Statwalker CSV

OPTIONS:
  -o, --output FILE Output CSV (default: "<stem>.res.csv" in CWD)
```

* Output filename is derived from the input **stem**.
* Header is emitted once: `INODE,ACCESSED,MODIFIED,USER,GROUP,TYPE,PERM,SIZE,DISK,PATH`.

---

## 5. Implementation Details (Code Highlights)

* **CSV ingestion**: `rdr.byte_records()` ⇒ `csv::ByteRecord` (non-UTF-8 safe).
* **Output**: `wtr.write_byte_record(&out_rec)`; let the library handle quoting.
* **Timezone**: `chrono` (`Utc` → `Local`).
* **Name resolution** (Unix): `libc` + `CStr::from_ptr(..).to_str()`; fall back to `"UNK"` if not valid UTF-8.
* **Error contexts**: Each read/write includes `line_no` in messages for quick triage.
* **Performance**: O(n) pass with small per-category caches:

  * `time_cache: HashMap<i64, String>`
  * `user_cache, group_cache: HashMap<u32, String>`
  * `type_cache, perm_cache: HashMap<u32, String>`

---

## 6. Cross-Platform Behavior

* **Linux/macOS/BSD**:

  * PATHs may be non-UTF-8 in input; Resolve accepts them and emits UTF-8 (lossy where needed).
  * Names from `/etc/passwd` and `/etc/group`.
* **Windows**:

  * Statwalker already emits UTF-8 PATHs; Resolve simply passes through as UTF-8.
  * USER/GROUP are numeric strings (no SID resolution in Resolve).

---

## 7. Reliability and Data Integrity

* **Resilient ingestion**: Malformed numeric bytes decode as `"0"` before parse—keeps the pipeline alive.
* **Traceable failures**: Any I/O/parse/write error is wrapped with file path and line number.
* **No partial headers**: header written once, then streaming records.
* **CSV correctness**: Quoting and escaping handled by `csv` crate.

---

## 8. Performance Characteristics

* **Streaming** (constant memory vs. input size); caches grow with *unique* timestamps and mode values encountered.
* **No reformat of large integers** (`SIZE`, `DISK`) reduces CPU churn.
* **Throughput** is typically gated by disk I/O and CSV parsing; Resolve adds minimal overhead versus raw copying.

**Tip:** If timestamps are mostly unique (e.g., backups), `time_cache` may not yield hits; it’s still cheap but can be bounded (see Enhancements).

---

## 9. Security & Privacy

* **No directory access**: Resolve does not touch the filesystem being described—only the CSV file.
* **Username/groupname exposure**: Output includes resolved names; treat CSV as potentially sensitive.
* **Unicode replacement**: Non-UTF-8 bytes in PATH become `�`—this protects downstream systems expecting UTF-8.

---

## 10. Limitations (Current)

* **Lossy PATH** on Unix for non-UTF-8 sequences (by design).
* **USER/GROUP on non-Unix** are numeric; no SID → name resolution.
* **Timestamps** are day-granularity strings; time-of-day and TZ offset are not preserved.
* **Silent defaults**: invalid numeric bytes decode to `"0"` before parse—consider telemetry if you need strictness.

---

## 11. Interop with Statwalker

Resolve assumes Statwalker’s semantics:

* `MODE` contains POSIX bits sufficient for type/perm derivation (Windows modes are synthesized upstream).
* `DISK` equals `blocks*512` from Statwalker; Resolve does not recompute.
* `PATH` may be non-UTF-8 on Unix; Resolve converts to UTF-8 for the final CSV.

**Pipeline**

```
[Statwalker]  -> raw.csv  ──►  [Resolve]  -> human.csv
(dev-ino,..,mode,..,path-bytes)     (names, types, octal perms, local dates, UTF-8 path)
```

---

## 12. Future Enhancements

* **Formatting options**: `--time "%Y-%m-%d %H:%M:%S"`, `--utc`, `--tz Europe/Paris`.
* **Strict mode**: fail on invalid numeric fields instead of defaulting to 0.
* **User/group on Windows**: optional SID → name resolution.
* **Output variants**: JSONL / Parquet; gzip compression.
* **Human sizes**: optional `--human` columns (`SIZE_HUMAN`, `DISK_HUMAN`).
* **Cache policies**: bounded/LRU for timestamps; pool allocations for strings.
* **Perm formatting**: include symbolic (`rwxr-xr-x`) alongside octal.

---

## 13. Usage Examples

```bash
# Basic
resolve stats.csv

# Custom output path
resolve stats.csv -o stats.res.csv

# In a pipeline with Statwalker
statwalker /data | tee stats.csv
resolve stats.csv -o stats.res.csv
```

---

## 14. Build & Dependencies

```toml
[dependencies]
anyhow = "1"
clap = { version = "4", features = ["color"] }
csv = "1"
chrono = "0.4"
libc = { version = "0.2", optional = false }  # for Unix name lookups
```

Release profile (recommended, same as Statwalker):

```toml
[profile.release]
lto = true
codegen-units = 1
panic = "abort"
opt-level = 3
```

---

## 15. Validation Checklist

* Output header exactly matches `OUT_HEADER`.
* Every record has 10 fields, last is valid UTF-8.
* `USER/GROUP` resolved on Unix; else numeric strings.
* `TYPE` matches `MODE` masks; `PERM` equals `mode & 0o7777` in octal.
* Dates are local `YYYY-MM-DD`; zero/invalid epochs → `0001-01-01`.
* Line counts: `lines(human.csv) == lines(raw.csv)`.

---

**Conclusion**
Resolve provides the **interpretation layer** atop Statwalker: converting raw, platform-accurate metadata into a stable, human-readable, UTF-8 CSV without sacrificing scale. Its streaming, cache-aware design delivers predictable performance, while the clear semantics make outputs safe for dashboards, BI tools, and further ETL.
