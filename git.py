#!/usr/bin/env python3
import subprocess
import os
import re
from pathlib import Path
import shutil

# detect repo root (if script is inside rs/, go up one level)
SCRIPT_DIR = Path(__file__).resolve().parent
if (SCRIPT_DIR / "Cargo.toml").exists():
    # script is inside rs/
    REPO_DIR = SCRIPT_DIR.parent
    RS_DIR = SCRIPT_DIR
else:
    # script is in repo root
    REPO_DIR = SCRIPT_DIR
    RS_DIR = SCRIPT_DIR / "rs"

BINARY_NAME = "duscan.exe"

def run(cmd, cwd=REPO_DIR, capture=False):
    """Run shell command with optional capture."""
    print(f"$ {' '.join(cmd)} (cwd={cwd})")
    if capture:
        result = subprocess.run(" ".join(cmd), cwd=cwd, check=True, shell=True,
                                stdout=subprocess.PIPE, 
                                text=True)
        return result.stdout.strip()
    else:
        subprocess.run(cmd, cwd=cwd, check=True)

def main():
    # 1️⃣ Get commits containing "chore"
    print("Getting chore commits...")
    log_output = run(["git", "log", "--grep=chore", "--oneline"], capture=True)
    lines = log_output.splitlines()
    if not lines:
        print("No chore commits found.")
        return

    commits = [line.split()[0] for line in lines]
    print(f"Found {len(commits)} commits: {commits}")

    # remember current branch
    current_branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture=True)

    for commit in commits:
        print("\n" + "="*60)
        print(f"Building commit {commit}")
        print("="*60)

        run(["git", "checkout", commit])
        run(["cargo", "clean"], cwd=RS_DIR)
        run(["cargo", "build", "-r", "--bin", "duscan"], cwd=RS_DIR)

        bin_path = RS_DIR / "target" / "release" / BINARY_NAME
        version_output = run([str(bin_path), "-V"], cwd=RS_DIR, capture=True)
        match = re.search(r"(\d+\.\d+\.\d+)", version_output)
        version = match.group(1) if match else "unknown"
        print(f"Detected version: {version}")
        print(f"Binary path: {bin_path}")
        dest = REPO_DIR / f"{BINARY_NAME}-{version}.exe"
        # ✅ cross-platform copy
        shutil.copy2(bin_path, dest)
        print(f"Copied binary to {dest}")

    # return to original branch
    print(f"\nReturning to original branch: {current_branch}")
    run(["git", "checkout", current_branch])

    print("\n✅ All versions built successfully.")

if __name__ == "__main__":
    main()
