"""Promote audit user to admin and get internal token."""
import sqlite3

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()

# Find all admin users
cur.execute("SELECT user_id, email, role, tier FROM app_user WHERE role IN ('admin', 'super_admin')")
admins = cur.fetchall()
print(f"Existing admins ({len(admins)}):")
for a in admins:
    print(f"  {a}")

# Find audit user
cur.execute("SELECT user_id, email, role, tier FROM app_user WHERE email = 'audit@test.com'")
audit = cur.fetchone()
print(f"\nAudit user: {audit}")

if audit and audit[2] != 'admin':
    cur.execute("UPDATE app_user SET role = 'admin' WHERE email = 'audit@test.com'")
    conn.commit()
    print("  -> Promoted to admin")

# Also check internal token config
import sys
sys.path.insert(0, ".")
try:
    from app.core.config import settings
    print(f"\nInternal cron token: {settings.internal_cron_token[:20]}..." if settings.internal_cron_token else "\nNo internal cron token set")
except Exception as e:
    print(f"\nCannot read config: {e}")

conn.close()
