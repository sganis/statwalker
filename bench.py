#!/usr/bin/env python3
import subprocess
import re
from pathlib import Path
import statistics

# --- Config ---
TEST_DIR = "testdata"            # Folder to scan
RUNS = 5                         # Number of times to run each exe
PATTERN = "*.exe"                # Match all executables
BINARY_PREFIX = "duscan-"        # Filter prefix; set "" to include all .exe

def supports_q(exe_path: Path) -> bool:
    """Check if executable supports the --quiet flag."""
    try:
        result = subprocess.run(
            [str(f'./{exe_path}'), "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=3,
        )
        return "--quiet" in result.stdout.lower()
    except Exception:
        return False

def run_once(exe_path: Path, use_q: bool) -> float | None:
    """Run the executable once and extract Files/s value."""
    cmd = [str(exe_path)]
    if use_q:
        cmd.append("--quiet")
    cmd.append(TEST_DIR)

    try:
        result = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output = result.stdout
        match = re.search(r"Files/s[^:\n]*:\s*([\d,.]+)", output)
        if match:
            return float(match.group(1).replace(",", ""))
        return None
    except subprocess.CalledProcessError:
        return None

def benchmark_exe(exe_path: Path, runs: int = RUNS) -> dict:
    """Run the benchmark multiple times and return stats."""
    use_q = supports_q(exe_path)
    flag_info = " (with --quiet)" if use_q else ""
    print(f"🏃 Running {exe_path.name}{flag_info} ({runs} runs)...")

    values = []
    for i in range(1, runs + 1):
        val = run_once(exe_path, use_q)
        if val is not None:
            values.append(val)
            print(f"  Run {i}: {val:.2f} Files/s")
        else:
            print(f"  ⚠️ Run {i} failed or no 'Files/s:' found.")

    if values:
        avg = statistics.mean(values)
        stdev = statistics.stdev(values) if len(values) > 1 else 0.0
        return {
            "name": exe_path.name,
            "avg": avg,
            "min": min(values),
            "max": max(values),
            "std": stdev,
        }
    else:
        return {"name": exe_path.name, "avg": 0, "min": 0, "max": 0, "std": 0}

def main():
    exe_files = sorted(Path(".").glob(PATTERN))
    exe_files = [f for f in exe_files if BINARY_PREFIX in f.name]
    if not exe_files:
        print("No matching executables found.")
        return

    results = []
    for exe in exe_files:
        stats = benchmark_exe(exe)
        if stats["avg"] > 0:
            results.append(stats)

    if not results:
        print("No valid results.")
        return

    results.sort(key=lambda x: x["avg"], reverse=True)

    print("\n🏁 Benchmark Results (higher is better):\n")
    print(f"{'Executable':35} {'Avg':>10} {'Min':>10} {'Max':>10} {'StdDev':>10}")
    print("-" * 80)
    for r in results:
        print(
            f"{r['name']:35} "
            f"{r['avg']:10.2f} {r['min']:10.2f} "
            f"{r['max']:10.2f} {r['std']:10.2f}"
        )

if __name__ == "__main__":
    main()
