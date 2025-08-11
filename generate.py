#!/usr/bin/env python3
import argparse
import string
from pathlib import Path
from itertools import product

CONTENT = b"hello world\n"

# name generator: a..z, aa..zz, aaa...
def name_generator():
    alphabet = string.ascii_lowercase
    n = 1
    while True:
        for chars in product(alphabet, repeat=n):
            yield "".join(chars)
        n += 1

def generate(
    root: Path,
    total_count: int,
    max_depth: int,
    files_per_dir: int,
    subdirs_per_dir: int,
):
    root.mkdir(parents=True, exist_ok=True)

    created = 0  # count files+folders created (root not counted)
    queue = [(root, 1)]
    file_names = name_generator()
    dir_names  = name_generator()

    while queue and created < total_count:
        d, depth = queue.pop(0)

        # 1) create up to files_per_dir files in this directory
        files_to_make = min(files_per_dir, total_count - created)
        for _ in range(files_to_make):
            if created >= total_count:
                break
            fname = f"{next(file_names)}.txt"
            (d / fname).write_bytes(CONTENT)
            created += 1

        # 2) create subdirectories if we still have budget and depth limit
        if depth < max_depth and created < total_count:
            dirs_to_make = min(subdirs_per_dir, total_count - created)
            for _ in range(dirs_to_make):
                if created >= total_count:
                    break
                sub = d / next(dir_names)
                sub.mkdir(exist_ok=True)
                created += 1
                queue.append((sub, depth + 1))

        if created % 1000 == 0:
            print(f"Created {created}/{total_count}")

    print(f"Done. Created {created} total entries (files + folders).")

def main():
    ap = argparse.ArgumentParser(description="Generate a-b-c style test tree with fixed file content.")
    ap.add_argument("--root", type=Path, default=Path("testdata"))
    ap.add_argument("--total", type=int, default=100_000, help="Total files + folders to create (root not counted)")
    ap.add_argument("--max-depth", type=int, default=3, help="Max folder depth (root is depth 1)")
    ap.add_argument("--files-per-dir", type=int, default=150, help="Files to create in each directory")
    ap.add_argument("--subdirs-per-dir", type=int, default=26, help="Subdirectories to create in each directory")
    args = ap.parse_args()

    generate(
        root=args.root,
        total_count=args.total,
        max_depth=args.max_depth,
        files_per_dir=args.files_per_dir,
        subdirs_per_dir=args.subdirs_per_dir,
    )

if __name__ == "__main__":
    main()
