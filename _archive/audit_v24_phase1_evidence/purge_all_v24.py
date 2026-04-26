"""v24 硬闸：物理删除所有未通过 G1~G4 的研报。
按用户指示：全部 alive 研报均判错（content_json NULL + capital_game_summary 缺主力/龙虎榜/融资融券
+ sim_trade_instruction null for BUY>=0.65 + term_context 指标键不全），一律物理删除。
先备份 jsonl 以便回滚。
"""
from __future__ import annotations
import json, os, sqlite3
from datetime import datetime

DB = r"d:\yanbao-new\data\app.db"
BACKUP_DIR = r"d:\yanbao-new\_archive\backup_20260418_v24"
os.makedirs(BACKUP_DIR, exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row
cur = con.cursor()

# --- 1. 锁定目标：所有 is_deleted=0 的研报 ---
report_ids = [r["report_id"] for r in cur.execute(
    "SELECT report_id FROM report WHERE is_deleted=0"
).fetchall()]
print(f"[v24-purge] alive reports to purge: {len(report_ids)}")

if not report_ids:
    print("[v24-purge] nothing to do")
    raise SystemExit(0)

# --- 2. 备份：报告主表 + 所有关联表 ---
tables_cascade = [
    "report",
    "report_citation",
    "report_data_usage_link",
    "report_idempotency",
    "report_feedback",
    "report_generation_task",
]
# instruction_card / trade_instruction / capital_game_summary 等子表（若存在）
extra_sub_tables = [
    "instruction_card",
    "trade_instruction",
    "report_capital_game_summary",
    "report_term_context",
    "report_content",
]
for t in extra_sub_tables:
    ex = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()
    if ex:
        tables_cascade.append(t)

print(f"[v24-purge] cascade tables: {tables_cascade}")

# 把 ids 分批做 IN 查询
def chunks(lst, n=500):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

manifest = {"backup_ts": ts, "alive_count": len(report_ids), "tables": {}}
for t in tables_cascade:
    cols = [c["name"] for c in cur.execute(f"PRAGMA table_info({t})").fetchall()]
    if "report_id" not in cols:
        print(f"[v24-purge] skip {t} (no report_id col)")
        continue
    out = os.path.join(BACKUP_DIR, f"{t}_{ts}.jsonl")
    n = 0
    with open(out, "w", encoding="utf-8") as f:
        for batch in chunks(report_ids):
            q = f"SELECT * FROM {t} WHERE report_id IN ({','.join('?'*len(batch))})"
            for row in cur.execute(q, batch).fetchall():
                f.write(json.dumps(dict(row), ensure_ascii=False, default=str) + "\n")
                n += 1
    print(f"[v24-purge] backed up {t}: {n} rows -> {out}")
    manifest["tables"][t] = {"path": out, "rows": n}

# 额外备份 report_data_usage（非 cascade，但可能被重生参考）
out = os.path.join(BACKUP_DIR, f"report_data_usage_{ts}.jsonl")
n = 0
with open(out, "w", encoding="utf-8") as f:
    for row in cur.execute("SELECT * FROM report_data_usage").fetchall():
        f.write(json.dumps(dict(row), ensure_ascii=False, default=str) + "\n")
        n += 1
print(f"[v24-purge] backed up report_data_usage: {n} rows")
manifest["tables"]["report_data_usage"] = {"path": out, "rows": n}

with open(os.path.join(BACKUP_DIR, f"manifest_{ts}.json"), "w", encoding="utf-8") as f:
    json.dump(manifest, f, ensure_ascii=False, indent=2)

# --- 3. 物理删除 ---
total_deleted = {}
con.execute("BEGIN")
try:
    for t in tables_cascade:
        cols = [c["name"] for c in cur.execute(f"PRAGMA table_info({t})").fetchall()]
        if "report_id" not in cols:
            continue
        n = 0
        for batch in chunks(report_ids):
            cur.execute(
                f"DELETE FROM {t} WHERE report_id IN ({','.join('?'*len(batch))})",
                batch,
            )
            n += cur.rowcount
        total_deleted[t] = n
    con.commit()
except Exception as e:
    con.rollback()
    raise
print(f"[v24-purge] DELETED rows: {total_deleted}")

# --- 4. 验证 ---
alive_after = cur.execute("SELECT COUNT(*) FROM report WHERE is_deleted=0").fetchone()[0]
total_after = cur.execute("SELECT COUNT(*) FROM report").fetchone()[0]
print(f"[v24-purge] after: total={total_after}, alive={alive_after}")
con.close()
