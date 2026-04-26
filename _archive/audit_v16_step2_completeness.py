"""Step 2: Report completeness scan — per-report × per-dataset × kline-window."""
import sqlite3
from collections import Counter, defaultdict

REQUIRED = [
    "kline_daily",
    "hotspot_top50",
    "northbound_summary",
    "etf_flow_summary",
    "market_state_input",
]

c = sqlite3.connect("data/app.db")

# 1. Take every alive published report
alive = c.execute(
    """
    SELECT report_id, stock_code, trade_date, created_at,
           published, publish_status, quality_flag, status_reason,
           (case when conclusion_text is null or conclusion_text='' then 0 else 1 end) as has_concl,
           (case when reasoning_chain_md is null or reasoning_chain_md='' then 0 else 1 end) as has_reason
    FROM report WHERE is_deleted=0
    """
).fetchall()
print(f"alive_reports={len(alive)}")

# 2. For each (trade_date, stock_code), fetch dataset-status map
usage_map = defaultdict(dict)
for td, sc, ds, st in c.execute(
    "SELECT trade_date, stock_code, dataset_name, status FROM report_data_usage"
).fetchall():
    key = (td, (sc or "").upper())
    usage_map[key][ds] = st

# 3. kline coverage per stock
kline_ranges = {}
for sc, mn, mx, cnt in c.execute(
    "SELECT stock_code, min(trade_date), max(trade_date), count(*) FROM kline_daily GROUP BY stock_code"
).fetchall():
    kline_ranges[sc.upper()] = (mn, mx, cnt)

# 4. Score each alive report
reason_bucket = Counter()
missing_by_ds = Counter()
incomplete_reports = []
fully_ok = 0
for r in alive:
    (rid, sc, td, created, published, pub_status, qflag, sreason,
     has_concl, has_reason) = r
    sc_u = (sc or "").upper()
    problems = []
    if not has_concl:
        problems.append("missing_conclusion")
    if not has_reason:
        problems.append("missing_reasoning")
    if published != 1 or pub_status != "PUBLISHED":
        problems.append("not_published")
    if qflag != "ok":
        problems.append(f"quality_{qflag}")
    if sreason:
        problems.append(f"reason_{sreason}")
    # dataset check keyed by (td, sc)
    ds_map = usage_map.get((td, sc_u), {})
    for ds in REQUIRED:
        if ds not in ds_map:
            problems.append(f"no_usage_{ds}")
            missing_by_ds[f"no_usage_{ds}"] += 1
        elif ds_map[ds] != "ok":
            problems.append(f"bad_status_{ds}:{ds_map[ds]}")
            missing_by_ds[f"bad_status_{ds}"] += 1
    # kline window check: stock must have kline >= trade_date + 60 days ideally
    if sc_u not in kline_ranges:
        problems.append("no_kline_at_all")
    else:
        mn, mx, cnt = kline_ranges[sc_u]
        if td and mx < td:
            problems.append(f"kline_max_before_td:{mx}<{td}")

    if not problems:
        fully_ok += 1
    else:
        incomplete_reports.append((rid, sc, td, problems))
        for p in problems:
            reason_bucket[p.split(":")[0]] += 1

print(f"fully_ok={fully_ok}  incomplete={len(incomplete_reports)}  total={len(alive)}")
print("\nTop reasons (alive reports only):")
for k, v in reason_bucket.most_common(25):
    print(f"  {k}: {v}")

print("\nMissing-usage by dataset:")
for k, v in missing_by_ds.most_common():
    print(f"  {k}: {v}")

print("\nSample 5 incomplete reports:")
for rid, sc, td, probs in incomplete_reports[:5]:
    print(f"  {rid[:12]} {sc} {td}: {probs[:6]}")

# 5. Also check how many (td, stock) pairs actually have ALL 5 datasets OK in usage
pairs_all_ok = 0
pairs_total = len(usage_map)
for key, ds_map in usage_map.items():
    if all(ds_map.get(ds) == "ok" for ds in REQUIRED):
        pairs_all_ok += 1
print(f"\n(td,stock) pairs: total={pairs_total} all-5-datasets-ok={pairs_all_ok}")

# 6. data_batch_lineage summary
try:
    tot = c.execute("SELECT count(*) FROM data_batch_lineage").fetchone()[0]
    print(f"\ndata_batch_lineage rows: {tot}")
    for row in c.execute("SELECT dataset_name, count(*) FROM data_batch_lineage GROUP BY dataset_name ORDER BY 2 DESC LIMIT 20").fetchall():
        print("  ", row)
except Exception as exc:
    print("data_batch_lineage err:", exc)
