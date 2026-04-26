"""Check existing users and create test accounts for browser testing."""
import sqlite3, json

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
c = conn.cursor()

print("=== Existing users ===")
rows = c.execute("SELECT user_id, email, role, tier, email_verified, failed_login_count, locked_until FROM user_account LIMIT 20").fetchall()
for r in rows:
    print(dict(r))

print(f"\nTotal users: {c.execute('SELECT COUNT(*) FROM user_account').fetchone()[0]}")
conn.close()
