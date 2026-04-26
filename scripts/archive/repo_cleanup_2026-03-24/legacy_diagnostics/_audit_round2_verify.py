"""Round 2 audit verification script - real HTTP + DB cross-validation."""
import json
import os
import sys
import time

# Ensure app is importable
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

import requests
from sqlalchemy import create_engine, text

BASE = "http://127.0.0.1:8001"
engine = create_engine("sqlite:///data/app.db")

passed = 0
failed = 0

def check(name, condition, msg=""):
    global passed, failed
    if condition:
        print(f"  [PASS] {name}")
        passed += 1
    else:
        print(f"  [FAIL] {name}: {msg}")
        failed += 1


def get_auth_token():
    """Get auth token for protected endpoints."""
    r = requests.post(f"{BASE}/auth/login", json={"email": "audit@test.local", "password": "AuditTest123!"})
    if r.json().get("success"):
        return r.json()["data"]["access_token"]
    return None


# === P0-03: Home + Dashboard status consistency ===
print("\n=== P0-03: Home + Dashboard status consistency ===")
r1 = requests.get(f"{BASE}/api/v1/home")
home = r1.json()["data"]
print(f"  Home: data_status={home['data_status']}, reason={home['status_reason']}")

r2 = requests.get(f"{BASE}/api/v1/dashboard/stats?window_days=30")
dash = r2.json()["data"]
print(f"  Dashboard: data_status={dash['data_status']}, reason={dash['status_reason']}")

check("home_dash_consistent", home["data_status"] == dash["data_status"],
      f"{home['data_status']} vs {dash['data_status']}")


# === P0-04: Sim dashboard shows COMPUTING when equity empty ===
print("\n=== P0-04: Sim dashboard shows COMPUTING when equity empty ===")
token = get_auth_token()
if token:
    headers = {"Authorization": f"Bearer {token}"}
    r3 = requests.get(f"{BASE}/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=headers)
    sim = r3.json()["data"]
    print(f"  SimDash: data_status={sim['data_status']}, reason={sim['status_reason']}")
    check("sim_computing", sim["data_status"] == "COMPUTING",
          f"got {sim['data_status']}")
else:
    print("  [SKIP] No auth token available")
    check("sim_computing", False, "no auth")


# === P0-07: Scheduler task run recording with trigger_source=cron ===
print("\n=== P0-07: Scheduler task run recording ===")
with engine.connect() as conn:
    count_before = conn.execute(text("SELECT COUNT(*) FROM scheduler_task_run")).scalar()
    print(f"  Tasks before: {count_before}")

# Test: directly call register_scheduler_run
from app.core.db import SessionLocal
from app.services.scheduler_ops_ssot import register_scheduler_run

db = SessionLocal()
try:
    result = register_scheduler_run(
        db,
        task_name="audit_test_market_state",
        trade_date="2026-03-12",
        schedule_slot="09:00",
        trigger_source="cron",
    )
    db.commit()
    print(f"  register result: {result}")
    check("register_created", result["action"] in ("created", "existing", "skipped_existing_success"),
          f"action={result['action']}")
finally:
    db.close()

with engine.connect() as conn:
    count_after = conn.execute(text("SELECT COUNT(*) FROM scheduler_task_run")).scalar()
    print(f"  Tasks after: {count_after}")
    rows = conn.execute(text(
        "SELECT task_run_id, task_name, trigger_source, status "
        "FROM scheduler_task_run WHERE task_name='audit_test_market_state'"
    )).fetchall()
    for row in rows:
        print(f"  Record: id={row[0][:8]}..., name={row[1]}, trigger={row[2]}, status={row[3]}")
        check("trigger_source_cron", row[2] == "cron", f"got {row[2]}")

# Also test that the scheduler.py _record_task_run function uses 'cron'
from app.services.scheduler import _record_task_run
db2 = SessionLocal()
try:
    # We can verify by reading the source
    import inspect
    src = inspect.getsource(_record_task_run)
    check("scheduler_uses_cron", 'trigger_source="cron"' in src,
          "scheduler.py still uses apscheduler")
finally:
    db2.close()


# === P0-11: Report generation task persistence ===
print("\n=== P0-11: Report generation task persistence ===")
r4 = requests.get(f"{BASE}/report/600519.SH/status")
print(f"  Status before trigger: {r4.status_code} {json.dumps(r4.json(), ensure_ascii=False)}")

# Trigger report (this sends the loading page)
r5 = requests.get(f"{BASE}/report/600519.SH", allow_redirects=False)
print(f"  Report page: {r5.status_code}")
time.sleep(3)

r6 = requests.get(f"{BASE}/report/600519.SH/status")
status_data = r6.json()
print(f"  Status after trigger: {json.dumps(status_data, ensure_ascii=False)}")

# Check DB for task record
with engine.connect() as conn:
    tasks = conn.execute(text(
        "SELECT task_id, stock_code, status FROM report_generation_task "
        "ORDER BY created_at DESC LIMIT 3"
    )).fetchall()
    print(f"  Recent tasks in DB: {len(tasks)}")
    for t in tasks:
        print(f"    task={t[0][:8]}... stock={t[1]} status={t[2]}")
    check("task_persisted", len(tasks) > 0, "no tasks in DB")


# === P1-07: Event dispatcher notification ===
print("\n=== P1-07: Event dispatcher notification logic ===")
with engine.connect() as conn:
    notif_count = conn.execute(text("SELECT COUNT(*) FROM notification")).scalar()
    print(f"  Notifications in DB: {notif_count}")
    if notif_count > 0:
        last = conn.execute(text(
            "SELECT channel, recipient_scope, status, status_reason "
            "FROM notification ORDER BY created_at DESC LIMIT 1"
        )).first()
        print(f"  Last: channel={last[0]}, scope={last[1]}, status={last[2]}, reason={last[3]}")
        check("notification_channel", last[0] in ("email", "webhook"), f"got {last[0]}")
    else:
        print("  (No notifications yet - OK, requires event trigger)")

# Verify event_dispatcher code uses valid channels
from app.services import event_dispatcher
import inspect
src = inspect.getsource(event_dispatcher)
check("dispatcher_no_push", "'push'" not in src,
      "dispatcher still uses 'push' channel")


# === P1-08: Core pool blocking message ===
print("\n=== P1-08: Core pool blocking message ===")
r7 = requests.get(f"{BASE}/report/600519.SH/status")
sd = r7.json()
if "data" in sd and "job" in sd["data"]:
    job = sd["data"]["job"]
    error = job.get("error", "")
    status = job.get("status", "")
    print(f"  Job status: {status}, error: {error}")
    check("core_pool_msg", error == "NOT_IN_CORE_POOL" or "not_in_core_pool" in error.lower(),
          f"error={error}")
else:
    print(f"  Response: {json.dumps(sd, ensure_ascii=False)}")
    check("core_pool_msg", False, "no job data")


# === HTML page accessibility ===
print("\n=== HTML Pages Accessibility ===")
pages = ["/", "/reports", "/subscribe", "/dashboard"]
for page in pages:
    try:
        r = requests.get(f"{BASE}{page}", timeout=5)
        check(f"page_{page}", r.status_code in (200, 302, 303),
              f"status={r.status_code}")
    except Exception as e:
        check(f"page_{page}", False, str(e))


# Cleanup
print("\n=== Cleanup ===")
with engine.connect() as conn:
    conn.execute(text("DELETE FROM scheduler_task_run WHERE task_name='audit_test_market_state'"))
    conn.commit()
    print("  Audit test data removed")


# Summary
print(f"\n{'='*50}")
print(f"AUDIT ROUND 2 RESULTS: {passed} passed, {failed} failed")
print(f"{'='*50}")
if failed == 0:
    print("ALL CHECKS PASSED")
else:
    print(f"WARNING: {failed} check(s) failed - needs attention")
