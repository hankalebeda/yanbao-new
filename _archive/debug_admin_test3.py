"""Extract and run the 3rd script (test_overview_failure) from admin contract tests."""
import subprocess, re

with open("tests/test_admin_dashboard_frontend_contract.py", encoding="utf-8") as f:
    content = f.read()

# Find all r""" ... """ blocks
scripts = list(re.finditer(r'script\s*=\s*r"""(.*?)"""', content, re.DOTALL))
print(f"Found {len(scripts)} scripts")

for i, m in enumerate(scripts):
    print(f"\n{'='*60}")
    print(f"Script {i+1} (pos {m.start()}):")
    script_text = m.group(1)
    result = subprocess.run(
        ["node", "-e", script_text],
        capture_output=True,
        text=True,
        timeout=30,
        cwd="D:\\yanbao-new",
    )
    if result.returncode != 0:
        print(f"  FAILED (exit {result.returncode})")
        stderr = result.stderr.strip()
        # Show last 20 lines of stderr
        lines = stderr.split("\n")
        for line in lines[-20:]:
            print(f"  {line}")
    else:
        print(f"  OK")
