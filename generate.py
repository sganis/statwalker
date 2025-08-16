#!/usr/bin/env python3
import argparse
import random
import string
import os
from pathlib import Path
from itertools import product

CONTENT = b"hello world\n"

# Reserved names on Windows
RESERVED = {"CON", "PRN", "AUX", "NUL"} | {f"COM{i}" for i in range(1, 10)} | {f"LPT{i}" for i in range(1, 10)}
IS_WINDOWS = os.name == "nt"


def is_reserved(name: str) -> bool:
    if not IS_WINDOWS:
        return False
    return name.split(".")[0].upper() in RESERVED


def name_generator():
    alphabet = string.ascii_lowercase
    n = 1
    while True:
        for chars in product(alphabet, repeat=n):
            yield "".join(chars)
        n += 1


def generate(root: Path, total: int, max_depth=6):
    root.mkdir(parents=True, exist_ok=True)
    created = 0

    queue = [(root, 1)]
    names = name_generator()

    while queue and created < total:
        d, depth = queue.pop(0)

        # Random “burst” of operations in this directory
        burst = random.randint(5, 50)

        for _ in range(burst):
            if created >= total:
                break

            # Randomly choose between file or folder creation
            make_dir = random.random() < 0.25   # 25% chance to create a dir

            if make_dir and depth < max_depth:
                # create a subdirectory
                while True:
                    n = next(names)
                    if not is_reserved(n):
                        break
                sub = d / n
                sub.mkdir(exist_ok=True)
                created += 1
                queue.append((sub, depth + 1))
            else:
                # create a file
                while True:
                    n = next(names)
                    if not is_reserved(n):
                        break
                fname = f"{n}.txt"
                (d / fname).write_bytes(CONTENT)
                created += 1

        if created % 1000 == 0:
            print(f"Created {created}/{total}")

    print(f"Done: created exactly {created} entries (files + folders).")


def main():
    ap = argparse.ArgumentParser(description="Generate a random nested directory/file tree for testing.")
    ap.add_argument("--root", type=Path, default=Path("testdata"))
    ap.add_argument("--total", type=int, required=True, help="Total number of entries (files + folders)")
    args = ap.parse_args()

    generate(args.root, args.total)


if __name__ == '__main__':
    main()
