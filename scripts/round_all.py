#!/usr/bin/env python3
"""多轮验证统一入口：先 round_verify，再 round3_run（验证+结算+再验证）。"""
import os
import subprocess
import sys

def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts = [
        os.path.join(root, "scripts", "round_verify.py"),
        os.path.join(root, "scripts", "round3_run.py"),
    ]
    for path in scripts:
        print("\n" + "=" * 50)
        print("运行 %s" % os.path.basename(path))
        print("=" * 50)
        ret = subprocess.run([sys.executable, path], cwd=root).returncode
        if ret != 0 and "round_verify" in path:
            sys.exit(ret)
    print("\n=== round_all 完成 ===")

if __name__ == "__main__":
    main()
