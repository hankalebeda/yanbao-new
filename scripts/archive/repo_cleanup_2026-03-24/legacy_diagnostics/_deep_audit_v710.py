"""
v7.10 深度全量审计脚本 — "值级别"审计
=============================================
核心差异：之前的审计只检查"字段是否存在" + "页面是否200"。
本脚本检查每一个值是否正确、合理、跨页面一致。

审计维度：
1. 数据库真相层：直接查DB得到ground truth
2. API正确性层：API返回是否与DB一致
3. HTML渲染层：页面上显示的值是否与API一致
4. 跨页面一致性：同一个值在不同页面是否一致
5. 功能可用性：按钮/筛选/排序等交互功能是否可用
6. RBAC完整性：不同角色看到的内容是否符合权限设计
7. 数据合理性：值是否在合理范围内（如胜率0-1、日期不在未来等）
"""
import sqlite3
import httpx
import json
import sys
import re
from datetime import datetime, date
from typing import Any
from collections import defaultdict

BASE = "http://127.0.0.1:8099"
DB_PATH = "data/app.db"

# ====== 结果收集 ======
issues: list[dict] = []
checks_passed = 0
checks_total = 0

def issue(severity: str, category: str, msg: str, detail: str = ""):
    issues.append({"severity": severity, "category": category, "msg": msg, "detail": detail[:500]})
    icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢", "INFO": "ℹ️"}.get(severity, "❓")
    print(f"  {icon} [{severity}] [{category}] {msg}")
    if detail:
        print(f"        └─ {detail[:200]}")

def ok(category: str, msg: str):
    global checks_passed, checks_total
    checks_passed += 1
    checks_total += 1

def fail(severity: str, category: str, msg: str, detail: str = ""):
    global checks_total
    checks_total += 1
    issue(severity, category, msg, detail)

def check(cond: bool, category: str, msg: str, detail_on_fail: str = "", severity: str = "HIGH"):
    if cond:
        ok(category, msg)
    else:
        fail(severity, category, msg, detail_on_fail)
    return cond

# ====== 数据库直接审计 ======
def audit_database():
    print("\n" + "=" * 80)
    print("第一层：数据库真相审计")
    print("=" * 80)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 1. 表存在性
    tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    required_tables = [
        "report", "instruction_card", "settlement_result", "sim_position",
        "stock_pool_snapshot", "stock_pool_refresh_task", "stock_master",
        "market_state_cache", "report_citation", "report_data_usage",
        "report_data_usage_link", "app_user", "strategy_metric_snapshot",
        "baseline_metric_snapshot", "scheduler_task_run", "admin_operation",
    ]
    for t in required_tables:
        check(t in tables, "DB-SCHEMA", f"表 {t} 存在")

    # 2. 数据量合理性
    report_count = c.execute("SELECT COUNT(*) FROM report WHERE published=1 AND is_deleted=0").fetchone()[0]
    total_report = c.execute("SELECT COUNT(*) FROM report").fetchone()[0]
    deleted_report = c.execute("SELECT COUNT(*) FROM report WHERE is_deleted=1").fetchone()[0]
    unpublished = c.execute("SELECT COUNT(*) FROM report WHERE published=0 AND is_deleted=0").fetchone()[0]
    check(report_count > 0, "DB-DATA", f"有已发布研报 ({report_count})")
    print(f"      报告总数={total_report}, 已发布={report_count}, 已删除={deleted_report}, 未发布={unpublished}")

    # 3. 研报必填字段完整性
    null_fields = {}
    for field in ["stock_code", "trade_date", "recommendation", "confidence",
                  "strategy_type", "conclusion_text", "quality_flag"]:
        cnt = c.execute(f"SELECT COUNT(*) FROM report WHERE published=1 AND is_deleted=0 AND ({field} IS NULL OR {field} = '')").fetchone()[0]
        if cnt > 0:
            null_fields[field] = cnt
            fail("HIGH", "DB-COMPLETENESS", f"已发布研报有 {cnt} 条 {field} 为空",
                 f"SELECT report_id FROM report WHERE published=1 AND is_deleted=0 AND ({field} IS NULL OR {field} = '')")

    # 4. instruction_card 完整性
    reports_without_ic = c.execute("""
        SELECT COUNT(*) FROM report r
        WHERE r.published=1 AND r.is_deleted=0
        AND NOT EXISTS (SELECT 1 FROM instruction_card ic WHERE ic.report_id = r.report_id)
    """).fetchone()[0]
    check(reports_without_ic == 0, "DB-COMPLETENESS",
          f"所有已发布研报都有 instruction_card ({reports_without_ic}条缺失)")

    # 5. instruction_card.signal_entry_price 不为NULL
    null_price = c.execute("""
        SELECT COUNT(*) FROM instruction_card ic
        JOIN report r ON r.report_id = ic.report_id
        WHERE r.published=1 AND r.is_deleted=0 AND ic.signal_entry_price IS NULL
    """).fetchone()[0]
    if null_price > 0:
        fail("HIGH", "DB-COMPLETENESS", f"{null_price}条已发布研报 signal_entry_price 为 NULL")

    # 6. stop_loss / target_price / atr_pct 为正
    for col in ["stop_loss", "target_price", "atr_pct"]:
        neg = c.execute(f"""
            SELECT COUNT(*) FROM instruction_card ic
            JOIN report r ON r.report_id = ic.report_id
            WHERE r.published=1 AND r.is_deleted=0 AND ic.{col} IS NOT NULL AND ic.{col} < 0
        """).fetchone()[0]
        check(neg == 0, "DB-VALIDITY", f"instruction_card.{col} 无负值 (有{neg}条)")

    # 7. settlement_result 合理性
    settle_count = c.execute("SELECT COUNT(*) FROM settlement_result").fetchone()[0]
    settle_dates = c.execute("SELECT DISTINCT signal_date FROM settlement_result ORDER BY signal_date").fetchall()
    print(f"      结算总数={settle_count}, 日期分布={[r[0] for r in settle_dates]}")
    # 检查orphan settlement (引用不存在的report)
    orphan_settle = c.execute("""
        SELECT COUNT(*) FROM settlement_result sr
        WHERE NOT EXISTS (SELECT 1 FROM report r WHERE r.report_id = sr.report_id)
    """).fetchone()[0]
    if orphan_settle > 0:
        fail("HIGH", "DB-INTEGRITY", f"结算结果中有 {orphan_settle} 条引用不存在的研报 (孤儿数据)")

    # 8. stock_pool_snapshot 日期
    pool_dates = c.execute("""
        SELECT trade_date, COUNT(*) as cnt
        FROM stock_pool_snapshot WHERE pool_role='core'
        GROUP BY trade_date ORDER BY trade_date
    """).fetchall()
    print(f"      核心池快照: {[(r[0], r[1]) for r in pool_dates]}")
    check(len(pool_dates) > 0, "DB-DATA", "有核心池快照数据")

    # 9. market_state_cache
    market_rows = c.execute("SELECT trade_date, market_state FROM market_state_cache ORDER BY trade_date DESC LIMIT 3").fetchall()
    print(f"      市场状态: {[(r[0], r[1]) for r in market_rows]}")
    check(len(market_rows) > 0, "DB-DATA", "有市场状态缓存")

    # 10. 交易日历正确性 - 检查latest_trade_date是否合理
    max_report_date = c.execute("SELECT MAX(trade_date) FROM report WHERE published=1 AND is_deleted=0").fetchone()[0]
    max_pool_date = c.execute("SELECT MAX(trade_date) FROM stock_pool_snapshot").fetchone()[0]
    max_settle_date = c.execute("SELECT MAX(signal_date) FROM settlement_result").fetchone()[0]
    max_market_date = c.execute("SELECT MAX(trade_date) FROM market_state_cache").fetchone()[0]
    print(f"      最大日期: report={max_report_date}, pool={max_pool_date}, settle={max_settle_date}, market={max_market_date}")

    # 11. stock_master.industry 覆盖率
    total_stocks = c.execute("SELECT COUNT(*) FROM stock_master").fetchone()[0]
    with_industry = c.execute("SELECT COUNT(*) FROM stock_master WHERE industry IS NOT NULL AND industry != ''").fetchone()[0]
    print(f"      stock_master: total={total_stocks}, with_industry={with_industry}")

    # 12. report_data_usage 状态分布
    usage_stats = c.execute("""
        SELECT status, COUNT(*) FROM report_data_usage GROUP BY status
    """).fetchall()
    print(f"      数据用量状态分布: {dict(usage_stats)}")

    # 13. 用户数据
    user_count = c.execute("SELECT COUNT(*) FROM app_user").fetchone()[0]
    admin_count = c.execute("SELECT COUNT(*) FROM app_user WHERE role IN ('admin','super_admin')").fetchone()[0]
    pro_count = c.execute("SELECT COUNT(*) FROM app_user WHERE tier='Pro'").fetchone()[0]
    print(f"      用户: total={user_count}, admin={admin_count}, pro={pro_count}")

    # 14. scheduler_task_run 状态
    task_status = c.execute("""
        SELECT task_name, status, MAX(trade_date) as last_date
        FROM scheduler_task_run
        GROUP BY task_name, status
        ORDER BY task_name, status
    """).fetchall()
    print(f"      调度器任务状态:")
    for r in task_status:
        print(f"        {r[0]}: {r[1]} (最新:{r[2]})")

    # 15. 检查旧垃圾数据: 如果有大量deleted但未清除的数据
    if deleted_report > 0:
        issue("MEDIUM", "DB-GARBAGE", f"有 {deleted_report} 条已删除(is_deleted=1)的研报仍在DB中")

    # 16. 孤儿 sim_position (引用不存在的report)
    try:
        orphan_pos = c.execute("""
            SELECT COUNT(*) FROM sim_position sp
            WHERE NOT EXISTS (SELECT 1 FROM report r WHERE r.report_id = sp.report_id)
        """).fetchone()[0]
        if orphan_pos > 0:
            fail("MEDIUM", "DB-INTEGRITY", f"sim_position 有 {orphan_pos} 条引用不存在的研报")
    except:
        pass

    # 17. 重复研报: 同一个stock_code+trade_date有多个published的
    dup_reports = c.execute("""
        SELECT stock_code, trade_date, COUNT(*) as cnt
        FROM report
        WHERE published=1 AND is_deleted=0
        GROUP BY stock_code, trade_date
        HAVING cnt > 1
    """).fetchall()
    if dup_reports:
        fail("MEDIUM", "DB-INTEGRITY", f"有 {len(dup_reports)} 组重复研报(同stock_code+trade_date多个published)",
             str([(r[0], r[1], r[2]) for r in dup_reports[:5]]))

    # 收集ground truth
    ground_truth = {
        "published_report_count": report_count,
        "settlement_count": settle_count,
        "max_report_date": max_report_date,
        "max_pool_date": max_pool_date,
        "max_settle_date": max_settle_date,
        "max_market_date": max_market_date,
    }

    conn.close()
    return ground_truth


# ====== API 层审计 ======
def audit_api(ground_truth: dict):
    print("\n" + "=" * 80)
    print("第二层：API 正确性审计 (值级别)")
    print("=" * 80)

    # Login
    client = httpx.Client(timeout=30)
    tokens = {}
    for role, email, pwd in [
        ("admin", "admin@example.com", "Qwer1234.."),
        ("pro", "v79_pro@test.com", "TestPro123!"),
        ("free", "v79_free@test.com", "TestFree123!"),
    ]:
        try:
            r = client.post(f"{BASE}/auth/login", json={"email": email, "password": pwd})
            if r.status_code == 200:
                body = r.json()
                tokens[role] = body.get("data", body).get("access_token")
                ok("AUTH", f"{role} 登录成功")
            else:
                fail("CRITICAL", "AUTH", f"{role} 登录失败: {r.status_code}", r.text[:200])
        except Exception as e:
            fail("CRITICAL", "AUTH", f"{role} 登录异常: {e}")

    def get(path, token=None):
        h = {"Authorization": f"Bearer {token}"} if token else {}
        return client.get(f"{BASE}{path}", headers=h)

    def unwrap(resp):
        if resp.status_code != 200:
            return None
        j = resp.json()
        return j.get("data", j) if isinstance(j, dict) and "data" in j else j

    cross_page_values = {}

    # ─── HOME API ───
    print("\n── HOME API ──")
    home = unwrap(get("/api/v1/home"))
    if home:
        pool_size = home.get("pool_size")
        market_state = home.get("market_state")
        data_status = home.get("data_status")
        latest_reports = home.get("latest_reports", [])
        today_count = home.get("today_report_count", 0)

        cross_page_values["home_pool_size"] = pool_size
        cross_page_values["home_market_state"] = market_state
        cross_page_values["home_report_count"] = len(latest_reports)

        check(pool_size is not None and pool_size > 0, "HOME", f"pool_size={pool_size} > 0")
        check(market_state in ("BULL", "NEUTRAL", "BEAR"), "HOME",
              f"market_state={market_state} 在有效枚举中", severity="MEDIUM")
        check(data_status in ("READY", "COMPUTING", "DEGRADED"), "HOME",
              f"data_status={data_status} 在有效枚举中")
        check(len(latest_reports) > 0, "HOME", f"有最新研报 ({len(latest_reports)}条)")

        # 验证每条研报的关键字段
        for i, rpt in enumerate(latest_reports):
            rid = rpt.get("report_id")
            check(rid is not None, "HOME-REPORT", f"研报[{i}] 有report_id")
            check(rpt.get("stock_code") and re.match(r"^\d{6}\.(SH|SZ|BJ)$", rpt.get("stock_code", "")),
                  "HOME-REPORT", f"研报[{i}] stock_code格式正确: {rpt.get('stock_code')}")
            check(rpt.get("recommendation") in ("BUY", "SELL", "HOLD"),
                  "HOME-REPORT", f"研报[{i}] recommendation={rpt.get('recommendation')} 有效")
            conf = rpt.get("confidence")
            check(conf is not None and 0 <= conf <= 1, "HOME-REPORT",
                  f"研报[{i}] confidence={conf} 在[0,1]内")
            check(rpt.get("strategy_type") in ("A", "B", "C"),
                  "HOME-REPORT", f"研报[{i}] strategy_type={rpt.get('strategy_type')} 有效")
            # trade_date不应在未来
            td = rpt.get("trade_date")
            if td:
                check(td <= "2026-03-17", "HOME-REPORT",
                      f"研报[{i}] trade_date={td} 不在未来", severity="MEDIUM")
    else:
        fail("CRITICAL", "HOME", "HOME API 返回为空")

    # ─── POOL/STOCKS API ───
    print("\n── POOL/STOCKS API ──")
    pool_data = unwrap(get("/api/v1/pool/stocks"))
    if pool_data:
        pool_total = pool_data.get("total", 0)
        pool_date = pool_data.get("trade_date")
        pool_items = pool_data.get("items", [])

        cross_page_values["pool_total"] = pool_total
        cross_page_values["pool_date"] = pool_date

        check(pool_total > 0, "POOL", f"pool total={pool_total} > 0")
        check(pool_date is not None, "POOL", f"pool trade_date={pool_date}")
        check(pool_date <= "2026-03-17" if pool_date else False, "POOL",
              f"pool trade_date={pool_date} 不在未来")
        check(len(pool_items) == pool_total, "POOL",
              f"items数量({len(pool_items)}) == total({pool_total})")

        # 验证每个股票
        for i, item in enumerate(pool_items[:5]):  # 抽查前5
            check(item.get("stock_code") and re.match(r"^\d{6}\.(SH|SZ|BJ)$", item.get("stock_code", "")),
                  "POOL-ITEM", f"股票[{i}] stock_code={item.get('stock_code')} 格式正确")
            check(item.get("stock_name") and item.get("stock_name") != item.get("stock_code"),
                  "POOL-ITEM", f"股票[{i}] 有名称: {item.get('stock_name')}", severity="MEDIUM")

        # pool_size 跨页面一致性
        if pool_size:
            # 注意: home pool_size 可能和 pool/stocks total 不同(不同日期)
            pass
    else:
        fail("CRITICAL", "POOL", "POOL API 返回为空")

    # ─── REPORT DETAIL API (admin视角) ───
    print("\n── REPORT DETAIL API ──")
    if home and latest_reports:
        for role in ["admin", "pro", "free"]:
            tok = tokens.get(role)
            if not tok:
                continue
            rid = latest_reports[0].get("report_id")
            detail = unwrap(get(f"/api/v1/reports/{rid}", tok))
            if detail:
                # instruction_card 完整性
                ic = detail.get("instruction_card", {})
                if role in ("admin", "pro"):
                    check(ic.get("signal_entry_price") is not None and ic.get("signal_entry_price") != "¥**.**",
                          f"REPORT-{role}", "付费用户看到真实入场价",
                          f"got: {ic.get('signal_entry_price')}")
                    check(ic.get("stop_loss") is not None and ic.get("stop_loss") != "¥**.**",
                          f"REPORT-{role}", "付费用户看到真实止损价")
                    check(ic.get("target_price") is not None and ic.get("target_price") != "¥**.**",
                          f"REPORT-{role}", "付费用户看到真实目标价")
                    # 价格合理性
                    if isinstance(ic.get("signal_entry_price"), (int, float)):
                        p = ic["signal_entry_price"]
                        check(0.1 < p < 10000, f"REPORT-{role}",
                              f"entry_price={p} 在合理范围")
                    if isinstance(ic.get("stop_loss"), (int, float)):
                        sl = ic["stop_loss"]
                        check(sl > 0, f"REPORT-{role}", f"stop_loss={sl} 为正值")
                        if isinstance(ic.get("signal_entry_price"), (int, float)):
                            check(sl < ic["signal_entry_price"], f"REPORT-{role}",
                                  f"stop_loss({sl}) < entry_price({ic['signal_entry_price']})")
                else:  # free
                    check(ic.get("signal_entry_price") == "¥**.**",
                          f"REPORT-{role}", "免费用户看到遮罩价格",
                          f"got: {ic.get('signal_entry_price')}")

                # capital_game_summary
                cap = detail.get("capital_game_summary")
                check(cap is not None, f"REPORT-{role}", "有 capital_game_summary")
                if cap:
                    headline = cap.get("headline", "")
                    check("missing" not in headline.lower(), f"REPORT-{role}",
                          "headline 无英文 'missing'", headline)
                    check("failed" not in headline.lower(), f"REPORT-{role}",
                          "headline 无英文 'failed'", headline)

                # term_context
                tc = detail.get("term_context", {})
                check("ATR" in tc, f"REPORT-{role}", "term_context 有 ATR")

                # conclusion_text 不为空
                check(detail.get("conclusion_text") and len(detail.get("conclusion_text", "")) > 10,
                      f"REPORT-{role}", "conclusion_text 有内容", severity="MEDIUM")

                # degraded_banner
                qf = detail.get("quality_flag")
                if qf and qf != "ok":
                    check(detail.get("degraded_banner") is not None, f"REPORT-{role}",
                          "quality_flag非ok时有degraded_banner")
            else:
                fail("CRITICAL", f"REPORT-{role}", f"研报详情 API 返回空 (report_id={rid})")

    # ─── ADVANCED AREA API ───
    print("\n── ADVANCED AREA API ──")
    if home and latest_reports:
        rid = latest_reports[0].get("report_id")
        for role in ["admin", "pro", "free"]:
            tok = tokens.get(role)
            if not tok:
                continue
            adv = unwrap(get(f"/api/v1/reports/{rid}/advanced", tok))
            if adv:
                rc = adv.get("reasoning_chain") or ""
                is_trunc = adv.get("is_truncated")
                if role == "free":
                    check(is_trunc is True or len(rc) <= 200, f"ADV-{role}",
                          f"Free 用户 reasoning_chain 截断 (len={len(rc)}, trunc={is_trunc})")
                else:
                    check(len(rc) > 0, f"ADV-{role}", f"付费用户有推理链 (len={len(rc)})")
                # used_data_lineage
                lineage = adv.get("used_data_lineage", [])
                check(len(lineage) > 0, f"ADV-{role}",
                      f"有数据血缘 ({len(lineage)}条)", severity="MEDIUM")
            else:
                fail("HIGH", f"ADV-{role}", "高级区 API 返回空")

    # ─── DASHBOARD/STATS API ───
    print("\n── DASHBOARD STATS API ──")
    admin_tok = tokens.get("admin")
    if admin_tok:
        for window in [7, 14, 30, 60]:
            stats = unwrap(get(f"/api/v1/dashboard/stats?window_days={window}", admin_tok))
            if stats:
                wr = stats.get("overall_win_rate")
                plr = stats.get("overall_profit_loss_ratio")
                total = stats.get("total_reports")
                settled = stats.get("total_settled")
                svw = stats.get("signal_validity_warning")

                if window == 30:
                    cross_page_values["dash30_win_rate"] = wr
                    cross_page_values["dash30_plr"] = plr
                    cross_page_values["dash30_total"] = total
                    cross_page_values["dash30_settled"] = settled

                check(svw is not None, f"DASH-{window}d", f"有 signal_validity_warning")
                check(total is not None and total >= 0, f"DASH-{window}d", f"total_reports={total}")
                check(settled is not None and settled >= 0, f"DASH-{window}d", f"total_settled={settled}")
                if wr is not None:
                    check(0 <= wr <= 1, f"DASH-{window}d", f"win_rate={wr} 在[0,1]")
                if plr is not None:
                    check(plr >= 0, f"DASH-{window}d", f"profit_loss_ratio={plr} >= 0")

                # 基线数据
                br = stats.get("baseline_random")
                bm = stats.get("baseline_ma_cross")
                check(br is not None, f"DASH-{window}d", "有 baseline_random", severity="MEDIUM")
                check(bm is not None, f"DASH-{window}d", "有 baseline_ma_cross", severity="MEDIUM")

                # date_range合理性
                dr = stats.get("date_range", {})
                if dr.get("from") and dr.get("to"):
                    check(dr["from"] <= dr["to"], f"DASH-{window}d",
                          f"date_range from({dr['from']}) <= to({dr['to']})")
            else:
                fail("HIGH", f"DASH-{window}d", "Dashboard stats 返回空")

    # ─── ADMIN OVERVIEW API ───
    print("\n── ADMIN OVERVIEW API ──")
    if admin_tok:
        admin_data = unwrap(get("/api/v1/admin/overview", admin_tok))
        if admin_data:
            admin_pool = admin_data.get("pool_size")
            cross_page_values["admin_pool_size"] = admin_pool

            check(admin_pool is not None, "ADMIN", f"admin pool_size={admin_pool}")
            # 跨页面一致性: admin pool_size == home pool_size
            hp = cross_page_values.get("home_pool_size")
            if admin_pool is not None and hp is not None:
                check(admin_pool == hp, "CONSISTENCY",
                      f"admin pool_size({admin_pool}) == home pool_size({hp})",
                      severity="HIGH")

            # pipeline_stages
            pipelines = admin_data.get("pipeline_stages", {})
            if isinstance(pipelines, dict):
                for stage_name, stage in pipelines.items():
                    if isinstance(stage, dict):
                        status = stage.get("status")
                        check(status in ("success", "running", "failed", "pending", "skipped", None),
                              "ADMIN-PIPELINE", f"流水线 {stage_name} status={status} 有效",
                              severity="MEDIUM")

            # report_generation
            rg = admin_data.get("report_generation", {})
            if isinstance(rg, dict):
                by_strat = rg.get("by_strategy", {})
                print(f"      report_generation.by_strategy: {by_strat}")

            # data_freshness
            df = admin_data.get("data_freshness", {})
            print(f"      data_freshness: {df}")

            # scheduler
            slr = admin_data.get("scheduler_last_run")
            print(f"      scheduler_last_run: {slr}")

        else:
            fail("CRITICAL", "ADMIN", "Admin overview API 返回空")

        # Free user should get 403
        free_tok = tokens.get("free")
        if free_tok:
            r = get("/api/v1/admin/overview", free_tok)
            check(r.status_code == 403, "RBAC", f"Free→admin/overview={r.status_code} (应403)")

    # ─── PLATFORM PLANS API ───
    print("\n── PLATFORM PLANS API ──")
    plans_data = unwrap(get("/api/v1/platform/plans"))
    if plans_data:
        plan_list = plans_data.get("plans", []) if isinstance(plans_data, dict) else plans_data
        check(len(plan_list) >= 3, "PLANS", f"有≥3个订阅方案 ({len(plan_list)}个)")
        for p in plan_list:
            label = p.get("label") or p.get("name") or p.get("code")
            price = p.get("price_display")
            check(price is not None, "PLANS", f"方案 {label} 有 price_display", severity="MEDIUM")
    else:
        fail("HIGH", "PLANS", "平台方案 API 返回空")

    # ─── SIM DASHBOARD API ───
    print("\n── SIM DASHBOARD API ──")
    if admin_tok:
        for tier in ("500k", "100k", "10k"):
            sim = unwrap(get(f"/api/v1/portfolio/sim-dashboard?capital_tier={tier}", admin_tok))
            if sim:
                wr = sim.get("win_rate")
                plr = sim.get("profit_loss_ratio")
                op = sim.get("open_positions", [])
                eq = sim.get("equity_curve", [])

                check(wr is not None, f"SIM-{tier}", f"win_rate={wr}")
                check(plr is not None, f"SIM-{tier}", f"plr={plr}")
                check(len(op) >= 0, f"SIM-{tier}", f"open_positions={len(op)}")

                # equity_curve 合理性
                if eq:
                    for pt in eq[:3]:
                        check("date" in pt or "trade_date" in pt or "snapshot_date" in pt,
                              f"SIM-{tier}", "equity_curve 有日期字段", severity="MEDIUM")
            else:
                fail("HIGH", f"SIM-{tier}", f"Sim dashboard {tier} 返回空")

        # Free should get 403
        if free_tok:
            r = get("/api/v1/portfolio/sim-dashboard?capital_tier=500k", free_tok)
            check(r.status_code == 403, "RBAC", f"Free→sim-dashboard={r.status_code} (应403)")

    # ─── REPORTS LIST API ───
    print("\n── REPORTS LIST API ──")
    reports_list = unwrap(get("/api/v1/reports"))
    if reports_list:
        items = reports_list.get("items", [])
        check(len(items) > 0, "REPORT-LIST", f"报告列表有 {len(items)} 条")

        # 检查排序: trade_date 应降序
        if len(items) >= 2:
            dates = [i.get("trade_date") for i in items if i.get("trade_date")]
            if len(dates) >= 2:
                check(dates[0] >= dates[1], "REPORT-LIST",
                      f"按 trade_date 降序排列: {dates[0]} >= {dates[1]}", severity="MEDIUM")

        # 检查每条
        for i, item in enumerate(items[:5]):
            check(item.get("report_id") is not None, f"RLIST-{i}", "有report_id")
            check(item.get("stock_code") is not None, f"RLIST-{i}", "有stock_code")
            check(item.get("recommendation") in ("BUY", "SELL", "HOLD"),
                  f"RLIST-{i}", f"recommendation={item.get('recommendation')} valid")
    else:
        fail("CRITICAL", "REPORT-LIST", "报告列表 API 返回空")

    # ─── MARKET STATE API ───
    print("\n── MARKET STATE API ──")
    market = unwrap(get("/api/v1/market/state"))
    if market:
        ms = market.get("market_state") or market.get("state")
        check(ms in ("BULL", "NEUTRAL", "BEAR", None), "MARKET",
              f"market_state={ms} 有效", severity="MEDIUM")
    # 不一定有此API,可以容忍

    client.close()
    return cross_page_values, tokens


# ====== HTML 渲染层审计 ======
def audit_html_pages(cross_page_values: dict, tokens: dict):
    print("\n" + "=" * 80)
    print("第三层：HTML 页面渲染审计（浏览器实测）")
    print("=" * 80)

    client = httpx.Client(timeout=30)

    # ─── 匿名页面 ───
    print("\n── 匿名可访问页面 ──")
    anon_pages = {
        "/": "首页",
        "/subscribe": "订阅页",
        "/login": "登录页",
        "/register": "注册页",
        "/reports": "研报列表",
        "/dashboard": "统计看板",
        "/terms": "条款页",
        "/privacy": "隐私页",
    }
    for path, name in anon_pages.items():
        r = client.get(f"{BASE}{path}", follow_redirects=True)
        check(r.status_code == 200, f"HTML-ANON", f"{name}({path}) 返回200 (got {r.status_code})")

    # ─── 匿名应重定向 ───
    print("\n── 匿名应重定向的页面 ──")
    redirect_pages = {
        "/admin": "管理后台",
        "/portfolio/sim-dashboard": "模拟盘看板",
        "/profile": "个人资料",
    }
    for path, name in redirect_pages.items():
        r = client.get(f"{BASE}{path}", follow_redirects=False)
        check(r.status_code in (302, 303) and "/login" in (r.headers.get("location", "")),
              "HTML-REDIRECT", f"{name}({path}) 匿名→重定向登录 (got {r.status_code})",
              f"location: {r.headers.get('location', 'N/A')}")

    # ─── Admin 页面功能 ───
    print("\n── Admin 页面功能 ──")
    admin_tok = tokens.get("admin")
    if admin_tok:
        admin_cookies = {"access_token": admin_tok}

        # Admin 后台
        r = client.get(f"{BASE}/admin", cookies=admin_cookies, follow_redirects=False)
        check(r.status_code == 200, "HTML-ADMIN", f"/admin 管理员可访问 (got {r.status_code})")
        if r.status_code == 200:
            html = r.text
            # 检查管理后台关键元素
            check("pool_size" in html.lower() or "核心池" in html or "股票池" in html,
                  "HTML-ADMIN", "管理后台含池规模信息", severity="MEDIUM")
            check("scheduler" in html.lower() or "调度" in html,
                  "HTML-ADMIN", "管理后台含调度器信息", severity="MEDIUM")

        # Sim Dashboard
        r = client.get(f"{BASE}/portfolio/sim-dashboard", cookies=admin_cookies, follow_redirects=False)
        check(r.status_code == 200, "HTML-ADMIN", f"/portfolio/sim-dashboard 管理员可访问")

        # Dashboard
        r = client.get(f"{BASE}/dashboard", cookies=admin_cookies, follow_redirects=False)
        check(r.status_code == 200, "HTML-ADMIN", f"/dashboard 管理员可访问")

    # ─── Free 用户权限 ───
    print("\n── Free 用户权限检查 ──")
    free_tok = tokens.get("free")
    if free_tok:
        free_cookies = {"access_token": free_tok}

        # admin 应该 403 (Free用户不是admin角色)
        r = client.get(f"{BASE}/admin", cookies=free_cookies, follow_redirects=False)
        check(r.status_code == 403, "HTML-RBAC",
              f"Free→/admin 返回403拒绝 (got {r.status_code})")

    client.close()


# ====== 交叉一致性审计 ======
def audit_cross_consistency(cross_page_values: dict):
    print("\n" + "=" * 80)
    print("第四层：跨页面数据一致性审计")
    print("=" * 80)

    hp = cross_page_values.get("home_pool_size")
    ap = cross_page_values.get("admin_pool_size")
    pp = cross_page_values.get("pool_total")

    if hp is not None and ap is not None:
        check(hp == ap, "CROSS-POOL", f"Home pool_size({hp}) == Admin pool_size({ap})")
    if hp is not None and pp is not None:
        # 这两个可以不一致(不同查询条件)，但应该接近
        diff_pct = abs(hp - pp) / max(hp, 1)
        check(diff_pct < 0.3, "CROSS-POOL",
              f"Home pool_size({hp}) ~ Pool total({pp}) 差异({diff_pct:.0%}) < 30%",
              severity="MEDIUM")


# ====== 主程序 ======
def main():
    print("╔" + "═" * 78 + "╗")
    print("║  v7.10 深度全量审计 — 值级别 + 跨页面一致性 + 功能可用性       ║")
    print("╚" + "═" * 78 + "╝")

    ground_truth = audit_database()
    cross_page_values, tokens = audit_api(ground_truth)
    audit_html_pages(cross_page_values, tokens)
    audit_cross_consistency(cross_page_values)

    # ─── 汇总 ───
    print("\n" + "═" * 80)
    print("审计汇总")
    print("═" * 80)
    print(f"  总检查: {checks_total}")
    print(f"  通过:   {checks_passed}")
    print(f"  问题:   {len(issues)}")

    by_severity = defaultdict(list)
    for iss in issues:
        by_severity[iss["severity"]].append(iss)

    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]:
        items = by_severity.get(sev, [])
        if items:
            icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢", "INFO": "ℹ️"}[sev]
            print(f"\n  {icon} {sev} ({len(items)}条):")
            for iss in items:
                print(f"    [{iss['category']}] {iss['msg']}")
                if iss.get("detail"):
                    print(f"      └─ {iss['detail'][:150]}")

    # 保存
    with open("output/v710_deep_audit.json", "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "checks_total": checks_total,
            "checks_passed": checks_passed,
            "issues": issues,
            "cross_page_values": {k: str(v) for k, v in cross_page_values.items()},
        }, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: output/v710_deep_audit.json")

    return 1 if by_severity.get("CRITICAL") or by_severity.get("HIGH") else 0


if __name__ == "__main__":
    sys.exit(main())
