#!/usr/bin/env python3
"""Local helper: run the release binary the same way the container does."""
import os
import subprocess
import sys
import tempfile

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR") or os.path.join(
    tempfile.gettempdir(), "shopping-scratch"
)
HERE = os.path.dirname(os.path.abspath(__file__))


def binary():
    override = os.environ.get("PIPELINE_BIN")
    if override:
        return override
    name = "shopsess.exe" if os.name == "nt" else "shopsess"
    candidates = [
        os.path.join(HERE, "..", "rust", "target", "release", name),
        os.path.join("/usr/local/bin", name),
        "/shopsess",
    ]
    for path in candidates:
        if os.path.isfile(path):
            return path
    sys.exit("shopsess binary not found; build rust/ first")


def main():
    os.makedirs(SCRATCH, exist_ok=True)
    os.makedirs(OUT, exist_ok=True)
    env = dict(os.environ, DATA_DIR=DATA, OUTPUT_DIR=OUT, SCRATCH_DIR=SCRATCH)
    subprocess.run([binary()], env=env, check=True)


if __name__ == "__main__":
    main()
