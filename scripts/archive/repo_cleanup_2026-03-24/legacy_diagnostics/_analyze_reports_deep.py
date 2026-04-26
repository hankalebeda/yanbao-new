"""Phase 5: Deep quality analysis of report citations, data usage, and instruction cards."""
from sqlalchemy import text, create_engine
from sqlalchemy.orm import Session

e = create_engine("sqlite:///./data/app.db")

with Session(e) as db:
    # Sample citations
    cits = db.execute(text("""
        SELECT c.citation_order, c.source_name, c.source_url, c.title, c.excerpt
        FROM report_citation c
        JOIN report r ON c.report_id = r.report_id
        WHERE r.stock_code = '688702.SH'
        ORDER BY c.citation_order
    """)).fetchall()
    print("=== Citations for 688702.SH ===")
    for c in cits:
        print(f"  [{c.citation_order}] {c.source_name}: {c.title}")
        print(f"      URL: {(c.source_url or '')[:80]}")
        print(f"      Excerpt: {(c.excerpt or '')[:120]}")

    # Data usage for one report
    print("\n=== Data Usage for 688702.SH (2026-03-10) ===")
    usages = db.execute(text("""
        SELECT dataset_name, source_name, status, status_reason
        FROM report_data_usage
        WHERE stock_code = '688702.SH' AND trade_date = '2026-03-10'
    """)).fetchall()
    for u in usages:
        print(f"  {u.dataset_name} ({u.source_name}): {u.status} - {u.status_reason}")

    # Instruction card for ALL reports
    print("\n=== Instruction Cards (All Reports) ===")
    ics = db.execute(text("""
        SELECT r.stock_code, r.recommendation,
               ic.signal_entry_price, ic.atr_pct, ic.atr_multiplier,
               ic.stop_loss, ic.target_price, ic.stop_loss_calc_mode
        FROM instruction_card ic
        JOIN report r ON ic.report_id = r.report_id
        ORDER BY r.stock_code
    """)).fetchall()
    for ic in ics:
        sl_ok = "OK" if ic.stop_loss and ic.signal_entry_price and ic.stop_loss < ic.signal_entry_price else "⚠"
        tp_ok = "OK" if ic.target_price and ic.signal_entry_price and ic.target_price > ic.signal_entry_price else "⚠"
        print(f"  {ic.stock_code} ({ic.recommendation}): entry={ic.signal_entry_price} stop={ic.stop_loss}({sl_ok}) "
              f"target={ic.target_price}({tp_ok}) atr%={ic.atr_pct} atr_mult={ic.atr_multiplier} mode={ic.stop_loss_calc_mode}")

    # Cross-check: BUY reports should have risk_audit_status != not_triggered
    print("\n=== SSOT Compliance Checks ===")
    buy_reports = db.execute(text("""
        SELECT stock_code, recommendation, confidence, risk_audit_status, risk_audit_skip_reason,
               llm_fallback_level, quality_flag
        FROM report
        WHERE recommendation = 'BUY' AND confidence >= 0.65
    """)).fetchall()
    
    print(f"\nBUY + conf>=0.65 reports: {len(buy_reports)}")
    for r in buy_reports:
        if r.risk_audit_status == "not_triggered":
            if r.llm_fallback_level == "failed":
                print(f"  ⚠ {r.stock_code}: audit={r.risk_audit_status}, BUT this is a rule-fallback report (LLM failed)")
                print(f"     skip_reason={r.risk_audit_skip_reason}")
                print(f"     → SSOT says: BUY+conf>=0.65 → audit required. However LLM failed → rule engine produced BUY.")
                print(f"     → This is a known design tension: rule fallback shouldn't produce BUY+high-conf without audit")
            else:
                print(f"  ❌ {r.stock_code}: audit={r.risk_audit_status} but LLM={r.llm_fallback_level} - VIOLATION!")
        else:
            print(f"  ✓ {r.stock_code}: audit={r.risk_audit_status} - correct")

    # Quality flag distribution
    print("\n=== Quality Summary ===")
    all_reports = db.execute(text("""
        SELECT stock_code, recommendation, confidence, quality_flag, llm_fallback_level,
               LENGTH(reasoning_chain_md) as chain_len,
               (SELECT COUNT(*) FROM report_citation WHERE report_citation.report_id = r.report_id) as cit_count,
               (SELECT COUNT(*) FROM report_data_usage u WHERE u.stock_code = r.stock_code AND u.trade_date = r.trade_date) as usage_count
        FROM report r
        ORDER BY llm_fallback_level, stock_code
    """)).fetchall()
    
    print(f"\n{'Stock':<12} {'Rec':<6} {'Conf':>5} {'Quality':<10} {'LLM':>8} {'Chain':>6} {'Cites':>5} {'Data':>5}")
    print("-" * 65)
    for r in all_reports:
        print(f"{r.stock_code:<12} {r.recommendation:<6} {r.confidence:>5.2f} {r.quality_flag:<10} {r.llm_fallback_level:>8} {r.chain_len:>6} {r.cit_count:>5} {r.usage_count:>5}")
