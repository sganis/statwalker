# Statwalker Aggregate: Folder–User–Age Summarization at Scale

## A Technical White Paper on Architecture, Data Semantics, and Performance

**Version:** 1.0
**Date:** September 2025
**Author:** SAG

---

## Executive Summary

**Statwalker Aggregate** (“aggregate”) is the third stage in the Statwalker pipeline:

1. **Statwalker** — an ultra-fast filesystem scanner that streams raw metadata to CSV.
2. **Resolve** — a low-allocation normalizer that converts raw numeric fields to human-readable forms (dates, types, perms), while preserving the original byte-level path fidelity and emitting UTF-8 path strings in the output.
3. **Aggregate** — this component (the focus of this paper), which ingests Statwalker (or Resolve) CSV and produces **per-(folder, user, age)** summaries capturing file counts, disk usage, and last modification time.

Aggregate is built to operate directly on **CSV bytes** for speed and correctness, treating **paths as opaque byte sequences** throughout the computation and converting to **UTF-8** only at output time with **lossy replacement** for any invalid sequences. This aligns with the byte-accurate philosophy of Statwalker and the UTF-8 output guarantee of Resolve, delivering deterministic, analytics-friendly rollups that scale to millions of rows.

---

## 1. Problem Statement & Goals

### 1.1 The Need for Higher-Level Summaries

Raw file-level inventories are invaluable but unwieldy for capacity planning, lifecycle management, chargeback, and compliance reporting. Teams need **rollups** by:

* **Folder (hierarchical aggregation)** — capacity and activity at each ancestor path.
* **User** — ownership-driven trends and accountability.
* **Age** — lifecycle status: recent, medium, long-tail/unknown.

### 1.2 Requirements

* **Scale:** Process millions of input rows on commodity hardware.
* **Precision:** Treat paths as **bytes**; never corrupt non-UTF-8 inputs.
* **Determinism:** Sorted output for reproducible diffing in CI/CD.
* **Robustness:** Tolerate malformed rows and clock skew outliers.
* **Interoperability:** Consume outputs from **Statwalker**/**Resolve**; produce clean CSVs for BI tools.

---

## 2. Pipeline Role & Interfaces

### 2.1 Upstream Inputs

* **From Statwalker:** `INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH` (PATH is raw bytes; may be non-UTF-8).
* **From Resolve (optional):** same schema; dates may be humanized in a separate file, but Aggregate uses the **raw numeric columns** to avoid re-parsing formatted strings.

### 2.2 Downstream Outputs

* **Aggregated CSV** (deterministically sorted):

  ```
  path,user,age,files,disk,modified
  ```

  where `path` is **UTF-8** (invalid sequences replaced with U+FFFD), `age ∈ {0,1,2}`, and `modified` is the **latest mtime** seen in the group (seconds since epoch).
* **Unknown UIDs CSV**: a sorted list of **UIDs** that resolved to `"UNK"`.

---

## 3. Data Model & Semantics

### 3.1 Grouping Key

`(folder_path_bytes, user_string, age_bucket)`

* **folder\_path\_bytes**: Each file contributes to *all* its ancestors:

  * `/a/b/file.txt` → `/`, `/a`, `/a/b`
  * Backslashes are normalized to `/` (Windows paths supported).
  * Paths are treated as **byte sequences** end-to-end (no assumed encoding).
* **user\_string**: Derived via UID→username resolution, cached in-process; on non-Unix systems, UID is stringified. Unresolvable names map to `"UNK"`.
* **age\_bucket**:

  * `0` → files modified **≤ 60 days**
  * `1` → files modified **61..600 days**
  * `2` → files modified **> 600 days** or **unknown/invalid** mtimes

### 3.2 Measures

* **files**: Count of files contributing to the (folder,user,age) group.
* **disk**: Sum of `DISK` (bytes), not `SIZE`, to reflect on-disk consumption.
* **modified**: **Maximum** `MTIME` observed in the group (seconds since epoch).

### 3.3 Time Sanity

`sanitize_mtime(now, mtime)` coerces any `mtime` **> 1 day into the future** to `0` (unknown), mitigating clock skew and future-dated anomalies. These flow into age bucket `2`.

---

## 4. Architecture & Implementation

### 4.1 High-Level Flow

```
CSV Reader (byte records, Trim::None)
        │
        ├─ Parse fields with atoi (bytes → u32/u64/i64; default 0)
        │
        ├─ sanitize_mtime(now, mtime)
        │
        ├─ resolve_user(uid)  (cache)
        │
        ├─ get_folder_ancestors(path_bytes)
        │          └─ normalize '\' → '/'
        │          └─ expand '/', '/a', '/a/b', ...
        │
        └─ aggregate[(folder_bytes, user, age)].update(disk, mtime)
                           │
                           └─ at end: sort by (path_bytes, user, age)
                                        and write CSV with path UTF-8 (lossy)
```

### 4.2 Byte-First Design

* **CSV ingestion as bytes**; no global stringifying of rows.
* Parsing via **`atoi`** on trimmed ASCII slices for `UID`, `MTIME`, `DISK`:

  * O(1) per digit, no allocations, defaults to `0` on any error/overflow.
* **Paths remain bytes** throughout; **only at write time** are they converted to UTF-8 with `String::from_utf8_lossy`, ensuring **valid UTF-8 output** while preserving progress on non-UTF-8 inputs.

### 4.3 Aggregation Structure

* `HashMap<(Vec<u8>, String, u8), UserStats>`

  * `UserStats { file_count, disk_usage, latest_mtime }`
* Deterministic sort before writing:

  * Bytewise `path`, then `user` (lexicographic), then `age`.

### 4.4 Handling Unknown UIDs

* Any UID resolving to `"UNK"` is captured in a `HashSet<u32>`, then written as a **sorted** list to `<input_stem>.unk.csv`. This supports remediation (directory services fixes) and audit workflows.

---

## 5. Correctness & Edge Cases

* **Non-UTF-8 Paths:** Safely supported end-to-end. Output replaces invalid sequences with U+FFFD, guaranteeing valid UTF-8 CSV for BI tools while never panicking on bad inputs.
* **Windows Paths:** Backslashes normalized to `/`; ancestor expansion treats drive prefixes like components (e.g., `C:\a\b\file` → `/`, `/C:`, `/C:/a`, `/C:/a/b`).
* **Weird Inputs:**

  * Empty/missing numeric fields → `0`.
  * `mtime` far in the future → sanitized to `0` (age bucket 2).
  * Files at root `/name` → ancestors `[ "/"]`.
  * Trailing slashes, duplicate slashes, deep nesting: handled by robust ancestor expansion.

---

## 6. Performance

### 6.1 Complexity

* **Per-row parsing & update:** \~O(L) where `L` is the path length (for ancestor expansion), typically small relative to disk I/O.
* **Hash aggregation:** Expected O(1) per group update.
* **Sorting:** O(G log G) over the set of groups `G`.

### 6.2 Optimizations

* **Byte-level parsing** with `atoi` (no `String` allocations).
* **Trim::None** at the CSV reader (we trim ourselves for numeric fields), preventing accidental mutation of raw path bytes.
* **Single-pass aggregation** with batched, deterministic emission.
* **Caches**:

  * UID→user string (platform-dependent resolution).
  * Unknown UID set (HashSet) for an O(1) insert path.

### 6.3 Resource Considerations

* Memory is proportional to **distinct groups** (folders × users × age buckets). For extremely broad hierarchies or multi-tenant trees, consider sharding by top-level directory or periodic flush/merge (see §9).

---

## 7. Output Schema

### 7.1 Aggregated CSV

```
path,user,age,files,disk,modified
/CORP/Finance,alice,0,154,987654321,1700000042
/CORP/Finance,bob,2,48,12345678,1670000000
...
```

* `path` — UTF-8 guaranteed (lossy replacement for invalid sequences)
* `user` — platform-resolved user name or `"UNK"`
* `age` — 0/1/2 as defined in §3.1
* `files` — u64 count
* `disk` — u64 sum of on-disk bytes (not logical size)
* `modified` — i64, latest epoch seconds across grouped files

### 7.2 Unknown UIDs CSV

```
1001
1003
50123
```

Sorted ascending for readability and deterministic diffs.

---

## 8. Reliability, Safety & Testing

### 8.1 Failure Tolerance

* **Malformed CSV rows** are **warned and skipped**; processing continues.
* **Numeric parse errors/overflow** → default `0`.
* **Future-dated mtimes** (>24h ahead) → sanitized to `0` to prevent misclassification.

### 8.2 Tests (Highlights)

The suite covers:

* **UTF-8 safety**: lossy path conversion produces valid output.
* **Byte-parsers**: whitespace, signs, overflow, invalid input (all default to `0`).
* **Ancestor expansion**: root, trailing slashes, multiple slashes, Windows backslashes, non-UTF-8 bytes.
* **Ordering**: deterministic sorting in `write_results` and `write_unknown_uids`.
* **`count_lines`**: empty files, with/without trailing newline.
* **Mini integration**: ingest tiny CSV containing a non-UTF-8 path and produce valid UTF-8 output and unknown UID list.

These tests complement the **Statwalker** and **Resolve** test suites, providing end-to-end confidence across the pipeline.

---

## 9. Operations & Deployment

### 9.1 CLI

* **Input**: `--input <file>` (required)
* **Output**: `--output <file>` (optional; default `<stem>.agg.csv`)
* Unknown UIDs always go to `<input_stem>.unk.csv`.

### 9.2 Runtime Guidance

* Prefer running **Resolve → Aggregate** when downstream teams need human-readable ownership or when cross-platform normalization matters.
* For extremely large trees with huge group cardinality:

  * **Shard & merge**: run Aggregate per top-level folder and concatenate outputs.
  * Consider **on-the-fly spilling** (future work) if the group map grows beyond memory targets.

---

## 10. Security & Privacy

* **No content reads**; only filenames/metadata previously emitted by Statwalker.
* Usernames resolved via local platform APIs (e.g., `getpwuid`) and cached in-memory only.
* Outputs are flat files suitable for standard access controls; sanitize or restrict distribution as required by policy.

---

## 11. Relationship to Statwalker & Resolve

* **Statwalker** guarantees **fast, accurate, byte-true** inventories with minimal formatting overhead.
* **Resolve** transforms raw numeric fields to human-friendly columns while **ensuring output paths are UTF-8** (lossy where needed).
* **Aggregate** then turns these normalized inventories into **actionable summaries** by folder/user/age, enforcing **UTF-8 output** while preserving the byte-correct treatment of paths throughout processing.

Together, the trio forms a **scalable, cross-platform, and deterministic** pipeline from raw filesystems to analytics-ready reports.

---

## 12. Future Enhancements

* **Configurable age buckets** (policy-driven thresholds).
* **Top-K and filtering** (e.g., largest folders per user).
* **Incremental mode** (compare two snapshots; emit deltas).
* **Multi-output formats** (Parquet/Arrow for data lakes).
* **Spill-to-disk / streaming group merge** for extremely high cardinality.

---

## Appendix A: Technical Specs

### A.1 Dependencies

```toml
[dependencies]
clap = { version = "4", features = ["derive"] }
csv = "1"
memchr = "2"
chrono = { version = "0.4", features = ["clock"] }
libc = "0.2"
atoi = "2"
```

### A.2 Build Profile (suggested)

```toml
[profile.release]
lto = true
codegen-units = 1
panic = "abort"
opt-level = 3
```

### A.3 Key Functions

* **Parsing**: `parse_field_as_{u32,u64,i64}` with `atoi` and ASCII trim.
* **Path Handling**: `get_folder_ancestors` (bytewise; `\`→`/`), `bytes_to_safe_string`.
* **Semantics**: `sanitize_mtime`, `age_bucket`.
* **I/O**: `count_lines` (fast), `write_results`, `write_unknown_uids`.

---

## Conclusion

**Aggregate** completes the Statwalker pipeline by delivering **fast, deterministic, and encoding-safe** rollups tuned for real-world enterprise filesystems. It inherits Statwalker’s byte-accuracy and Resolve’s UTF-8 output guarantees, while adding **policy-aware age bucketing, user attribution, and hierarchical summarization**. This combination enables clear, defensible capacity and lifecycle reporting at massive scale—without sacrificing performance, correctness, or portability.
