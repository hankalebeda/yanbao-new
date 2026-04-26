#!/usr/bin/env python3
"""Official round gate entrypoint.

This wrapper intentionally runs only verified scripts that exist in-repo.
If a required script is missing or any step fails, the process exits non-zero.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys


def _run(script_path: str, base_url: str) -> int:
    command = [sys.executable, script_path, "--base-url", base_url]
    return subprocess.run(command, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the official round verification chain.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    verifier = os.path.join(root, "scripts", "round_verify.py")

    if not os.path.exists(verifier):
        print(f"ERR missing verifier: {verifier}", file=sys.stderr)
        return 1

    print("=" * 60)
    print("Running round_verify.py")
    print("=" * 60)
    result = _run(verifier, args.base_url)
    if result != 0:
        print("ERR round verification failed", file=sys.stderr)
        return result

    print("\n=== round_all complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
