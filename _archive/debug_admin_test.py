import subprocess
import re

with open("tests/test_admin_dashboard_frontend_contract.py", encoding="utf-8") as f:
    src = f.read()

# Extract the second occurrence of raw script (test_admin_template_overview_success)
blocks = list(re.finditer(r"script = r\"\"\"(.*?)\"\"\"", src, re.DOTALL))
print(f"Found {len(blocks)} script blocks")

for i, m in enumerate(blocks):
    js = m.group(1).strip()
    print(f"\n=== Block {i}: {len(js)} chars ===")
    result = subprocess.run(
        ["node", "-e", js],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=r"D:\yanbao-new",
    )
    print(f"Return code: {result.returncode}")
    if result.stderr:
        print(f"STDERR: {result.stderr[:2000]}")
    if result.stdout:
        print(f"STDOUT: {result.stdout[:500]}")
