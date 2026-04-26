"""Run the failing Node.js test scripts manually to see the actual error."""
import subprocess, re

# Read the test file to extract the two scripts
with open("tests/test_admin_dashboard_frontend_contract.py", encoding="utf-8") as f:
    content = f.read()

# Find the two test functions that fail
# test_admin_template_overview_failure_does_not_block_other_panels_or_cookie_probe
# test_admin_template_overview_success_renders_fr07_pipeline_status_mapping

# Extract the scripts - they call _run_node(script)
# Find all script = """ blocks
scripts = re.findall(r'script\s*=\s*"""(.*?)"""', content, re.DOTALL)
print(f"Found {len(scripts)} scripts")

# Run each one
for i, script in enumerate(scripts):
    print(f"\n{'='*60}")
    print(f"Script {i+1}:")
    result = subprocess.run(
        ["node", "-e", script],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        print(f"  EXIT CODE: {result.returncode}")
        print(f"  STDERR: {result.stderr[:500]}")
    else:
        print(f"  OK: {result.stdout[:200]}")
