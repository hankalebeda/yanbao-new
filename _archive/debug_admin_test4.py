"""Extract and run each script from admin contract tests with proper encoding."""
import subprocess, re

with open("tests/test_admin_dashboard_frontend_contract.py", encoding="utf-8") as f:
    content = f.read()

scripts = list(re.finditer(r'script\s*=\s*r"""(.*?)"""', content, re.DOTALL))
print(f"Found {len(scripts)} scripts")

for i, m in enumerate(scripts):
    print(f"\n{'='*60}")
    print(f"Script {i+1} (pos {m.start()}):")
    script_text = m.group(1)
    result = subprocess.run(
        ["node", "-e", script_text],
        capture_output=True,
        timeout=30,
        cwd="D:\\yanbao-new",
    )
    if result.returncode != 0:
        print(f"  FAILED (exit {result.returncode})")
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        lines = stderr.split("\n")
        for line in lines[-25:]:
            print(f"  {line}")
    else:
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        print(f"  OK: {stdout[:200]}")
