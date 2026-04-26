"""Verify the completeness of newly generated reports."""
import json
from app.core.db import SessionLocal
from app.models import Report

db = SessionLocal()
reports = db.query(Report).filter(Report.is_deleted == False).order_by(Report.created_at.desc()).limit(3).all()

for r in reports:
    print(f"=== {r.stock_code} {r.stock_name_snapshot} ===")
    cj = r.content_json or {}
    
    # Check capital_game_snapshot
    cgs = cj.get("capital_game_snapshot", {})
    mf = cgs.get("main_force", {})
    print(f"  Main Force: status={mf.get('status')}, 1d={mf.get('net_inflow_1d_fmt')}, 5d={mf.get('net_inflow_5d_fmt')}")
    dt = cgs.get("dragon_tiger", {})
    print(f"  Dragon Tiger: status={dt.get('status')}, lhb_30d={dt.get('lhb_count_30d')}, net_buy={dt.get('net_buy_total_fmt')}")
    mg = cgs.get("margin_financing", {})
    print(f"  Margin: status={mg.get('status')}, rzye={mg.get('latest_rzye_fmt')}, delta5d={mg.get('rzye_delta_5d_fmt')}")
    nb = cgs.get("northbound", {})
    print(f"  Northbound: status={nb.get('status')}")
    etf = cgs.get("etf_flow", {})
    print(f"  ETF: status={etf.get('status')}")
    
    # Check stock_profile_snapshot
    sp = cj.get("stock_profile_snapshot", {})
    print(f"  Profile: PE={sp.get('pe_ttm')}, PB={sp.get('pb')}, ROE={sp.get('roe_pct')}%, MV={sp.get('total_mv_fmt')}, Industry={sp.get('industry')}")
    
    # Check market_state_snapshot
    ms = cj.get("market_state_snapshot", {})
    print(f"  Market: state={ms.get('market_state')}, ref={ms.get('reference_date')}")
    
    # Check kline_snapshot
    kl = cj.get("kline_snapshot", {})
    print(f"  Kline: close={kl.get('close')}, ma5={kl.get('ma5')}, ma20={kl.get('ma20')}, atr={kl.get('atr_pct')}")
    
    # Check data_completeness
    dc = cj.get("data_completeness", {})
    print(f"  Completeness: {dc.get('total_ok')}/{dc.get('total_required')} all_complete={dc.get('all_complete')}")
    
    # Check evidence points have rich basis
    ev = cj.get("evidence_backing_points", [])
    print(f"  Evidence points: {len(ev)}")
    for e in ev:
        basis = str(e.get("basis", ""))[:100]
        print(f"    [{e.get('title')}] {basis}")
    
    # Count non-null data fields
    null_fields = []
    non_null = 0
    for key in ["capital_game_snapshot", "stock_profile_snapshot", "market_state_snapshot", "kline_snapshot", "data_completeness"]:
        v = cj.get(key)
        if v is None:
            null_fields.append(key)
        elif isinstance(v, dict) and not v:
            null_fields.append(f"{key}(empty)")
        else:
            non_null += 1
    
    if null_fields:
        print(f"  WARNING - Missing fields: {null_fields}")
    else:
        print(f"  ALL DATA SNAPSHOTS PRESENT ({non_null}/5)")
    
    print()

db.close()
print("Verification complete!")
