Here’s a quick pass on what was missing or inconsistent, then the full, final white paper.

## What I fixed/added versus your draft (at a glance)

* **Actual constants**: Updated to match code (`FILE_CHUNK = 16384`, `FLUSH_BYTES = 8 MiB`, worker shard writer = 32 MiB, merge writer = 16 MiB).
* **Concurrency wording**: It’s an **MPMC queue** (`crossbeam::channel`) with an **in-flight atomic** for termination—not true work-stealing.
* **Directory rows**: Code **emits one CSV row per directory** (via `stat_row(&dir)`); added explicitly.
* **Symlink semantics**: Uses `symlink_metadata` → **does not follow symlinks**; added.
* **CSV schema details**: Clarified that **INODE = “dev-inode”** (two numbers separated by `-`), **timestamps are Unix seconds**, **DISK = blocks×512**; added.
* **Windows specifics**: `dev/ino/uid/gid` are synthetic (`0`), **mode is approximated**, **blocks approximated as len/512**, **path normalization** strips verbatim prefixes; added.
* **Path encoding/quoting**: On Unix we write **raw bytes** (even if not UTF-8). Smart quoting for commas/quotes/newlines with correct double-quote escaping; added.
* **Sorting semantics**: `--sort` does a **full in-memory lexicographic sort of entire lines** (header already written once); added caution.
* **Termination mechanics**: Dedicated thread broadcasts `Shutdown` when **in-flight hits 0**; rationale and safety notes added.
* **Error reporting**: Code tracks **aggregate error count only** (not types); adjusted monitoring section accordingly.
* **Output finalization**: Merge **writes directly to final file**; **no atomic temp-rename** currently (draft had claimed atomic rename) — corrected and suggested improvement.
* **CLI defaults**: Threads default to `min(48, max(4, 2×CPU))`; skip is substring; root is canonicalized; output default naming rules per-OS; added.
* **Tiny nits in code banners**: “Statlaker” vs “Statwalker” and “Processes” label—called out in appendix as known UX nits.

---

# Statwalker: High-Performance Filesystem Scanner

## Architecture, Design, and Performance Techniques

**Version:** 2.0
**Date:** September 2025
**Author:** SAG

---

## Executive Summary

Statwalker is a cross-platform, high-throughput filesystem scanner implemented in Rust. It traverses large directory trees, collects POSIX-like metadata for **both files and directories**, and streams it to a compact CSV with careful attention to throughput, memory stability, and cross-platform fidelity. The design uses a single **MPMC task queue**, batched work units, large buffered writers, zero-copy path handling on Unix, and platform-specific metadata extraction. Typical scenarios include compliance inventories, storage analytics, dedup/DFIR pre-staging, and cost governance.

---

## 1. Problem Context

Modern estates regularly exceed **tens of millions** of path entries across mixed media (NVMe, HDD, network mounts). Traditional tools often:

* Under-utilize cores with largely sequential traversal
* Suffer syscall + synchronization overhead from per-entry writes
* Inflate memory via eager buffering or string conversions
* Produce platform-inconsistent metadata

Statwalker addresses those limits with **batched directory processing**, **streamed CSV emission**, and low-allocation hot paths.

---

## 2. System Overview

### 2.1 Data Model (CSV)

A single header precedes rows. **Directories and files both appear as rows.**

```
INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH
```

* **INODE**: `"<dev>-<ino>"` (two unsigned integers joined by `-`).

  * On Windows: `0-0` (no native ino/dev without extra APIs).
* **ATIME**, **MTIME**: Unix epoch seconds (`i64`).

  * Windows: converted from `SystemTime` to seconds.
* **UID**, **GID**: On Unix from `MetadataExt`; on Windows `0,0` (SIDs not mapped).
* **MODE**: POSIX style `u32`.

  * Unix: direct from `MetadataExt`.
  * Windows: **approximated** from attributes and extension heuristics.
* **SIZE**: File length in bytes; for directories this is whatever the platform returns.
* **DISK**: **blocks × 512**.

  * Unix: from `st_blocks`.
  * Windows: **approximate** as `(len + 511)/512`.
* **PATH**: Full path; smart CSV quoting as needed (see §3.4).

### 2.2 Concurrency Topology

```
Main -> (seed Task::Dir) ─┐
                          ├── crossbeam::channel (MPMC)
Workers x N  <────────────┘
   ├─ process Task::Dir: emit row, enumerate, enqueue new Tasks
   ├─ process Task::Files: batch-stat names, append CSV rows
   └─ write to per-thread shard buffers/files
```

* **Queue**: `crossbeam::channel` MPMC (multi-producer, multi-consumer).
* **In-flight counter**: `AtomicUsize` tracks outstanding tasks. A lightweight
  sentinel thread watches it and **broadcasts `Shutdown`** when it reaches **0**.
* **No work-stealing deques**: balancing is achieved via the shared MPMC queue.

### 2.3 Termination Mechanics

* Every time a task is **created**, in-flight is incremented **before** `send()`.
* A worker **decrements** in-flight after fully handling a task.
* When in-flight becomes **0**, the sentinel sends **N** `Shutdown` messages (N = worker threads).
  Because in-flight includes tasks enqueued but not yet processed, reaching 0 implies the queue is empty (save benign races around send ordering).

---

## 3. Implementation Details

### 3.1 Batching & Flush Strategy

* **Directory pagination**: file names are accumulated into pages of
  `FILE_CHUNK = 16384`, then sent as a single `Task::Files` batch.
* **Shard writers per worker**: `BufWriter` with **32 MiB** buffer.
* **Flush threshold** inside workers: `FLUSH_BYTES = 8 MiB` of formatted CSV in a staging `Vec<u8>`.
* **Merge writer**: `BufWriter` with **16 MiB** buffer for final CSV.

### 3.2 Memory Efficiency

* Hot-path numeric formatting via thread-local `itoa::Buffer` (no allocation).
* Pre-reserved staging buffers to minimize `Vec` growth.
* On Unix, paths are handled as **raw bytes** (no UTF-8 validation/conversion).
* On Windows, paths are normalized (strip `\\?\` / `\\?\UNC\`) **only for display/CSV**.

### 3.3 Platform Semantics

**Unix (Linux/macOS/BSD)**

* `symlink_metadata` is used: **symlinks are not followed** (entries describe the link object).
* `dev/ino/mode/uid/gid/blocks/size/atime/mtime` from `MetadataExt`.
* **Encoding**: `PATH` writes **raw bytes**; non-UTF-8 is preserved.

**Windows (NTFS/FAT/exFAT)**

* `dev/ino/uid/gid` are `0` (not resolved).
* `mode` is synthesized:

  * set dir/file bits (`040000`/`100000`), copy owner perms to group/other;
  * set owner read; set owner write unless read-only; set exec for known extensions (`exe`, `bat`, etc.); dirs get exec.
* `blocks` approximated as `(len + 511)/512` (not cluster-accurate).
* Timestamps converted to Unix seconds.
* Verbose prefixes removed for CSV readability.

**Fallback**

* For unsupported targets, provide best-effort fields (often zeros).

### 3.4 CSV Path Quoting & Escaping

* **Quote only if needed** (`,`, `"`, `\n`, `\r` present).
* Inside quotes, `"` is doubled per RFC4180.
* On Unix, quoting operates on **bytes**; on Windows, on UTF-16-to-UTF-8 lossless strings (via `to_string_lossy` if needed).

### 3.5 Sorting Mode

* `--sort` buffers **all** data lines (not header), then performs a **byte-wise lexicographic** sort of entire lines.
  Useful for testing or deterministic diffs; **not recommended** for huge scans due to memory.

---

## 4. CLI, Defaults, and Behaviors

```
USAGE:
  statwalker [OPTIONS] [root]

ARGS:
  root              Root folder (default: "."; canonicalized before scanning)

OPTIONS:
  -o, --output FILE Output CSV path (default: "<canonical-root>.csv" in CWD)
  -t, --threads N   Worker threads (default: min(48, max(4, 2×CPU)))
      --sort        Sort lines in-memory (expensive for large outputs)
      --skip SUBSTR Skip any directory whose full path contains SUBSTR
```

* **Root canonicalization**: Converts inputs like `../some/folder` into absolute paths and uses that everywhere (scan + output naming).
* **Output default naming**:

  * **Windows**: strip verbatim + drive colon; replace `\` with `-`.
  * **Unix**: strip leading `/`; replace `/` with `-`.
* **Rows include directories**: A directory is emitted **before** its contents are paged.

---

## 5. Reliability & Safety

* **Graceful error handling**: Individual metadata or read-dir errors are **counted**, scanning continues.
* **Aggregate error counter**: Reported at the end (no per-type breakdown).
* **Temporary shards**: Each worker writes `shard_<tid>.csv.tmp`; shards are removed after merge.
* **Finalization**: The merge currently writes **directly to the target file** and flushes.
  *Enhancement recommended*: write to a temp file and **atomic rename** for crash-safe finalization.
* **Back-pressure**: Batching reduces queue churn; buffers and `FLUSH_BYTES` limit fsync pressure.

---

## 6. Performance Techniques

* **Large batches (16k names)** collapse queue traffic and amortize syscalls.
* **Layered buffering** (staging `Vec<u8>` → `BufWriter` → OS page cache).
* **Minimal string churn**: path bytes on Unix; lossy conversions avoided on hot path.
* **Atomic in-flight counter** eliminates joins/polls across all workers.

**Thread count heuristic**: `min(48, max(4, 2×CPU))` is tuned for I/O-bound scans; override for specialized media (e.g., high-latency network mounts may prefer fewer threads).

---

## 7. Example Benchmarking (Methodology & Reference)

> Results vary by media, directory shape, and security tooling. The following rubric enables reproducible comparisons.

**Methodology**

* Pin CPU governor to “performance” (Linux) / High Performance (Windows).
* Warm up OS caches once (optional), then run **cold** and **warm** tests.
* Collect: elapsed wall-time, files counted, errors, CSV bytes written, average files/sec.

**Reference Environment (illustrative)**

* 16-core workstation, 64 GB RAM, single NVMe SSD
* Dataset: \~2.5 M entries across \~180 k directories, mixed small/large files

| Tool           | Time (s) | Files/sec | RSS (GB) | Notes                   |
| -------------- | -------: | --------: | -------: | ----------------------- |
| GNU `find`     |      847 |     2,950 |      0.1 | single-threaded         |
| `fd`           |      156 |    16,000 |      0.3 | multi-threaded          |
| **Statwalker** |       68 |    36,700 |      0.8 | stream + batch + shards |

*Your numbers will differ; keep the rubric consistent for fair comparisons.*

---

## 8. Operational Guidance

* **Local disks**: default threads are fine; consider `--threads` up to the heuristic cap for NVMe.
* **Spinning disks**: fewer threads can reduce seeking contention.
* **Network filesystems**: try lower threads; consider `--skip` for known heavy trees (e.g., caches, `node_modules`).
* **Security tools**: real-time scanning may dominate; whitelist the scan roots and shard files if policy allows.

---

## 9. Extensibility Roadmap

* **Output formats**: JSON/Parquet; direct writers to object storage; optional compression (zstd).
* **Filters**: size/type/date/regex; include/exclude lists; max depth.
* **Metadata**: ctime/btime where available; DOS attributes; file IDs/inodes on Windows via Win32 API.
* **Correct disk usage**: blocks at true cluster size on Windows via `GetDiskFreeSpace`.
* **Crash-safety**: temp file + atomic rename; periodic checkpoints.

---

## 10. Limitations (current)

* **Windows fidelity**: uid/gid and inode/device not populated; `mode` is heuristic.
* **DISK on Windows**: approximate, not cluster-accurate.
* **Sorting mode**: O(total CSV) memory; intended for small/medium test runs.
* **No progress callback**: final-only summary (elapsed, totals); no real-time rate reporting.
* **No symlink target stats**: entries describe symlinks themselves (by design).

---

## 11. Security & Privacy

* **Read-only access**: scanner never modifies data in trees being scanned.
* **Least privileges**: results depend on the permissions of the process user.
* **PII**: paths may contain sensitive names; protect CSV outputs at rest and in transit.

---

## 12. Build & Config

### 12.1 Dependencies

```toml
[dependencies]
clap = { version = "4", features = ["color"] }
colored = "2"
crossbeam = "0.8"
itoa = "1"
num_cpus = "1"
```

### 12.2 Release profile

```toml
[profile.release]
lto = true
codegen-units = 1
panic = "abort"
opt-level = 3
```

### 12.3 Targets

* x86\_64-unknown-linux-gnu
* x86\_64-pc-windows-msvc
* x86\_64-apple-darwin
* aarch64-apple-darwin

---

## 13. Usage Examples

```bash
# Default scan of current directory, CSV named after canonical root
statwalker

# Scan a project and skip vendor cache
statwalker --skip ".cache" --skip "node_modules" /projects/acme

# Force 24 threads and produce a deterministic, sorted CSV (testing only)
statwalker -t 24 --sort /data
```

---

## 14. Output Validation Checklist

* Header present exactly once.
* Every row has **9** comma-separated fields.
* Paths containing commas/quotes/newlines are properly **quoted** and **escaped**.
* Final line ends with `\n` (important for some CSV consumers).
* **Total rows = files + directories processed** (compare to a baseline if needed).

---

## Appendix A – Notable Code Points

* **Banners**: Minor cosmetic typo (“Statlaker”) and “Processes” label for thread count—pure UX; no runtime impact.
* **Shutdown loop sleep**: 10 ms; a lower value increases wakeups without measurable benefit in typical runs.
* **Unix path encoding**: raw bytes output is intentional to preserve non-UTF-8 paths; CSV consumers must handle arbitrary bytes in `PATH`.

---

## Appendix B – Repro Scripts

**Linux (bash)**

```bash
set -euo pipefail
ROOT=/data/huge
/usr/bin/time -v ./statwalker --threads 32 "$ROOT" -o out.csv
wc -l out.csv
```

**Windows (PowerShell)**

```powershell
$root = "D:\Data"
Measure-Command { .\statwalker.exe --threads 32 $root -o out.csv } | Format-List
(Get-Content out.csv).Count
```

---

## Appendix C – Future Testing Ideas

* Synthetic “wide” vs “deep” trees to stress queue depth and batching.
* Adversarial names (commas, quotes, newlines, non-UTF-8, long paths).
* Mixed permissions/ACL landscapes to validate error counting stability.

---

**Conclusion**
Statwalker focuses on the fundamentals that matter at scale: **batched traversal**, **streamed emission**, and **platform-aware metadata**. The current architecture provides dependable throughput with bounded memory and clear operational semantics, while leaving room for fidelity improvements (Windows) and production-hardening (atomic finalization, richer filters, alternative outputs).
