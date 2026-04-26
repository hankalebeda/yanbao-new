"""Fix v79 test users: verify email, set tier"""
import sqlite3

conn = sqlite3.connect("data/app.db")
c = conn.cursor()

# Verify email and set Pro tier
c.execute("UPDATE app_user SET email_verified=1, tier='Pro' WHERE email='v79_pro@test.com'")
print(f"Pro user updated: {c.rowcount}")

# Verify email and keep Free tier  
c.execute("UPDATE app_user SET email_verified=1, tier='Free' WHERE email='v79_free@test.com'")
print(f"Free user updated: {c.rowcount}")

conn.commit()

# Verify
for email in ['v79_pro@test.com', 'v79_free@test.com', 'admin@example.com']:
    row = c.execute("SELECT email, role, tier, email_verified FROM app_user WHERE email=?", (email,)).fetchone()
    print(f"  {row}")

conn.close()
print("Done")
