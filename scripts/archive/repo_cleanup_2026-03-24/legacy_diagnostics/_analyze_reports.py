"""Phase 5: Analyze quality of existing research reports."""
from sqlalchemy import text, create_engine
from sqlalchemy.orm import Session
import json

engine = create_engine("sqlite:///./data/app.db")

with Session(engine) as db:
    rows = db.execute(text("""
        SELECT r.report_id, r.stock_code, r.trade_date, r.recommendation, r.confidence,
               r.strategy_type, r.market_state, r.quality_flag, r.llm_fallback_level,
               r.risk_audit_status, r.conclusion_text,
               LENGTH(r.reasoning_chain_md) as chain_len,
               r.reasoning_chain_md,
               (SELECT COUNT(*) FROM report_data_usage u
                WHERE u.stock_code = r.stock_code AND u.trade_date = r.trade_date) as data_usage_count
        FROM report r
        ORDER BY r.trade_date DESC, r.stock_code
    """)).fetchall()

    print(f"═══ REPORT QUALITY ANALYSIS ({len(rows)} reports) ═══\n")

    issues = []
    for i, r in enumerate(rows, 1):
        print(f"── Report {i}: {r.stock_code} | {r.trade_date} ──")
        print(f"   ID:          {r.report_id}")
        print(f"   Rec/Conf:    {r.recommendation} / {r.confidence}")
        print(f"   Strategy:    {r.strategy_type}")
        print(f"   Market:      {r.market_state}")
        print(f"   Quality:     {r.quality_flag}")
        print(f"   LLM Fallback:{r.llm_fallback_level}")
        print(f"   Risk Audit:  {r.risk_audit_status}")
        print(f"   Chain Len:   {r.chain_len}")
        print(f"   Data Sources:{r.data_usage_count}")

        conc = (r.conclusion_text or "")[:200]
        print(f"   Conclusion:  {conc}...")

        # Quality checks
        report_issues = []

        # 1. Confidence range (should be 0.0 - 1.0)
        if r.confidence is not None and (r.confidence < 0 or r.confidence > 1):
            report_issues.append(f"Confidence out of range: {r.confidence}")

        # 2. Missing conclusion
        if not r.conclusion_text or len(r.conclusion_text.strip()) < 10:
            report_issues.append("Conclusion missing or too short")

        # 3. Missing reasoning chain
        if not r.chain_len or r.chain_len < 50:
            report_issues.append(f"Reasoning chain too short ({r.chain_len})")

        # 4. No data usage records
        if r.data_usage_count == 0:
            report_issues.append("No data usage/lineage records")

        # 5. Quality flag issues
        if r.quality_flag in ("missing", "degraded"):
            report_issues.append(f"Quality flag: {r.quality_flag}")

        # 6. Recommendation validity
        valid_rec = {"BUY", "SELL", "HOLD", "STRONG_BUY", "STRONG_SELL"}
        if r.recommendation and r.recommendation.upper() not in valid_rec:
            report_issues.append(f"Unknown recommendation: {r.recommendation}")

        # 7. Strategy type validity (A, B, C per SSOT)
        valid_strat = {"A", "B", "C"}
        if r.strategy_type and r.strategy_type.upper() not in valid_strat:
            report_issues.append(f"Unknown strategy: {r.strategy_type}")

        # 8. Market state validity
        valid_market = {"BULL", "NEUTRAL", "BEAR"}
        if r.market_state and r.market_state.upper() not in valid_market:
            report_issues.append(f"Unknown market state: {r.market_state}")

        # 9. Risk audit
        if r.risk_audit_status and r.risk_audit_status.upper() not in ("PASS", "FAIL", "SKIP", "SKIPPED"):
            report_issues.append(f"Unknown risk audit status: {r.risk_audit_status}")

        # 10. Check reasoning chain for hallucination markers
        chain = r.reasoning_chain_md or ""
        hallucination_markers = ["I don't have", "I cannot", "as an AI", "I'm sorry"]
        for marker in hallucination_markers:
            if marker.lower() in chain.lower():
                report_issues.append(f"Potential hallucination marker in chain: '{marker}'")

        if report_issues:
            print(f"   ⚠ ISSUES:")
            for iss in report_issues:
                print(f"     - {iss}")
                issues.append((r.stock_code, r.trade_date, iss))
        else:
            print(f"   ✓ No issues found")
        print()

    # Summary
    print(f"\n═══ QUALITY SUMMARY ═══")
    print(f"Total reports:  {len(rows)}")
    print(f"Reports with issues: {len(set((s, d) for s, d, _ in issues))}")
    print(f"Total issues:   {len(issues)}")

    if issues:
        print(f"\nAll issues:")
        for stock, date, issue in issues:
            print(f"  {stock} {date}: {issue}")

    # Distribution stats
    recs = [r.recommendation for r in rows if r.recommendation]
    quals = [r.quality_flag for r in rows if r.quality_flag]
    strats = [r.strategy_type for r in rows if r.strategy_type]
    confs = [r.confidence for r in rows if r.confidence is not None]

    print(f"\nRecommendation distribution: {dict((v, recs.count(v)) for v in set(recs))}")
    print(f"Quality distribution:        {dict((v, quals.count(v)) for v in set(quals))}")
    print(f"Strategy distribution:       {dict((v, strats.count(v)) for v in set(strats))}")
    if confs:
        print(f"Confidence range:            {min(confs):.2f} - {max(confs):.2f} (avg: {sum(confs)/len(confs):.2f})")

    # Check a sample reasoning chain
    print(f"\n═══ SAMPLE REASONING CHAIN (Report 1) ═══")
    sample_chain = (rows[0].reasoning_chain_md or "")[:1500]
    print(sample_chain)
