import sqlite3
from datetime import datetime, timezone

db = sqlite3.connect('data/app.db')
c = db.cursor()

now = datetime.now(timezone.utc).isoformat()
sql = "UPDATE report_generation_task SET status='Failed', status_reason='manually_expired_stuck', finished_at=:now, updated_at=:now WHERE status='Processing'"
c.execute(sql, {"now": now})
print('Fixed stuck tasks:', c.rowcount)
db.commit()
db.close()
print('Done')
