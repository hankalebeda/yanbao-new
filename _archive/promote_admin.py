import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()
cur.execute("UPDATE app_user SET role='admin' WHERE email='audit_admin99@test.com'")
print(f'Updated to admin: {cur.rowcount} rows')
conn.commit()

# Find internal token
with open('.env') as f:
    for line in f:
        line = line.strip()
        if 'CRON_TOKEN' in line.upper() or 'INTERNAL_TOKEN' in line.upper():
            print(f'Found: {line}')
conn.close()
