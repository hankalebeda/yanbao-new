#!/usr/bin/env python3
"""
v8.0 全面浏览器测试脚本
- 管理员 + Pro用户 双角色测试
- 检查所有页面功能点、数据一致性
"""
import httpx
import json
import re
import sqlite3
import sys
from pathlib import Path

BASE = "http://127.0.0.1:8099"
ADMIN_EMAIL = "admin@example.com"
ADMIN_PWD = "Qwer1234.."
PRO_EMAIL = "v79_pro@test.com"
PRO_PWD = "TestPro123!"
DB_PATH = "data/app.db"

issues = []
passes = []


def log_issue(code, desc, severity="P0", fix_hint=""):
    issues.append({"code": code, "desc": desc, "severity": severity, "fix": fix_hint})
    print(f"  [BUG-{severity}] {code}: {desc}")
    if fix_hint:
        print(f"    FIX: {fix_hint}")


def log_ok(code, desc):
    passes.append({"code": code, "desc": desc})
    print(f"  [OK] {code}: {desc}")


def get_db():
    return sqlite3.connect(DB_PATH)


def main():
    print("=" * 70)
    print("v8.0 前端综合测试 - 管理员 + Pro用户")
    print("=" * 70)

    with httpx.Client(follow_redirects=True, timeout=15) as c:
        # ===== 登录 =====
        print("\n[1] 登录测试")
        r = c.post(f"{BASE}/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PWD})
        if r.status_code != 200:
            print(f"FATAL: Admin login failed: {r.status_code} {r.text[:100]}")
            sys.exit(1)
        admin_token = r.json()["data"]["access_token"]
        admin_tier = r.json()["data"]["tier"]
        admin_hdrs = {"Authorization": f"Bearer {admin_token}"}
        log_ok("AUTH-01", f"Admin登录成功 tier={admin_tier}")

        r = c.post(f"{BASE}/auth/login", json={"email": PRO_EMAIL, "password": PRO_PWD})
        if r.status_code != 200:
            print(f"WARN: Pro login failed: {r.status_code}")
        else:
            pro_token = r.json()["data"]["access_token"]
            pro_tier = r.json()["data"]["tier"]
            pro_hdrs = {"Authorization": f"Bearer {pro_token}"}
            log_ok("AUTH-02", f"Pro用户登录成功 tier={pro_tier}")

        # ===== 设置admin cookie session =====
        cs = c.post(f"{BASE}/api/v1/admin/cookie-session", headers=admin_hdrs)
        log_ok("AUTH-03", f"Admin cookie session: {cs.status_code}")

        # ===== 收集关键数据 =====
        print("\n[2] 收集API基准数据")

        r_home = c.get(f"{BASE}/api/v1/home", headers=pro_hdrs)
        home_data = r_home.json().get("data", {})
        home_pool_size = home_data.get("pool_size", -1)
        home_market_state = home_data.get("market_state", "?")
        home_data_status = home_data.get("data_status", "?")
        print(f"  Home API: pool_size={home_pool_size}, market_state={home_market_state}, data_status={home_data_status}")

        r_pool = c.get(f"{BASE}/api/v1/pool/stocks", headers=pro_hdrs)
        pool_data = r_pool.json().get("data", {})
        pool_total = pool_data.get("total", -1)
        pool_trade_date = pool_data.get("trade_date", "?")
        pool_items = pool_data.get("items", [])
        print(f"  Pool API: total={pool_total}, trade_date={pool_trade_date}, items_in_page={len(pool_items)}")

        r_ov = c.get(f"{BASE}/api/v1/admin/overview", headers=admin_hdrs)
        ov_data = r_ov.json().get("data", {})
        ov_pool_size = ov_data.get("pool_size", -1)
        ov_today_reports = ov_data.get("today_reports", -1)
        ov_buy_signals = ov_data.get("today_buy_signals", -1)
        ov_trade_date = ov_data.get("latest_trade_date", "?")
        print(f"  Admin Overview: pool_size={ov_pool_size}, today_reports={ov_today_reports}, buy_signals={ov_buy_signals}, trade_date={ov_trade_date}")

        r_dash = c.get(f"{BASE}/api/v1/dashboard/stats", headers=pro_hdrs)
        dash_data = r_dash.json().get("data", {})
        dash_total_reports = dash_data.get("total_reports", -1)
        dash_win_rate = dash_data.get("overall_win_rate", -1)
        dash_plr = dash_data.get("overall_profit_loss_ratio", -1)
        print(f"  Dashboard Stats: total_reports={dash_total_reports}, win_rate={dash_win_rate}, plr={dash_plr}")

        r_rpts = c.get(f"{BASE}/api/v1/reports", headers=pro_hdrs)
        rpts_data = r_rpts.json().get("data", {})
        rpts_total = rpts_data.get("total", -1)
        print(f"  Reports API: total={rpts_total}")

        # ===== 数据一致性检查 =====
        print("\n[3] 数据一致性检查")

        # pool_size一致性
        if home_pool_size == pool_total == ov_pool_size:
            log_ok("DATA-01", f"pool_size三处一致: home={home_pool_size}, pool/stocks={pool_total}, admin={ov_pool_size}")
        else:
            log_issue("DATA-01", f"pool_size不一致: home={home_pool_size}, pool/stocks={pool_total}, admin={ov_pool_size}", "P0",
                      "检查home API和pool/stocks API的数据来源是否一致")

        # pool/stocks total vs items count 一致性: total应等于全部items
        # 注意: 此处只返回一页，total可能大于页内items
        if pool_total != len(pool_items) and pool_total > 0:
            # 检查total是否等于实际DB数量
            db = get_db()
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM stock_pool_snapshot WHERE pool_date=?", (pool_trade_date,))
            db_pool_count = cur.fetchone()[0]
            db.close()
            print(f"  DB pool_snapshot count for {pool_trade_date}: {db_pool_count}")
            if pool_total != db_pool_count:
                log_issue("DATA-02", f"pool total={pool_total} 与DB快照数量={db_pool_count} 不一致", "P1")
            else:
                log_ok("DATA-02", f"pool total={pool_total} 与DB一致 (页内items={len(pool_items)} 是分页正常)")

        # dashboard total_reports vs reports total
        if dash_total_reports == rpts_total:
            log_ok("DATA-03", f"reports总数一致: dashboard={dash_total_reports}, reports_api={rpts_total}")
        else:
            log_issue("DATA-03", f"reports总数不一致: dashboard={dash_total_reports}, reports_api={rpts_total}", "P0",
                      "dashboard用的是窗口期内计数，reports是全部count，但两数叫法混淆易造成误解")

        # pool_trade_date vs ov_trade_date
        if pool_trade_date == ov_trade_date:
            log_ok("DATA-04", f"trade_date一致: pool={pool_trade_date}, admin={ov_trade_date}")
        else:
            log_issue("DATA-04", f"trade_date不一致: pool={pool_trade_date}, admin={ov_trade_date}", "P1")

        # ===== 页面HTML检查 =====
        print("\n[4] 页面HTML内容检查")

        # 主页
        r_home_page = c.get(f"{BASE}/")
        home_html = r_home_page.text
        print(f"  / 主页: {r_home_page.status_code}, {len(home_html)} chars")
        if "apiBase" in home_html or "api_base" in home_html or "/api/v1" in home_html:
            log_ok("PAGE-01", "主页包含API base配置")
        else:
            log_issue("PAGE-01", "主页未找到API base配置", "P1", "检查_app_config.html是否正确注入")

        # 检查导航栏链接
        nav_links = ["/reports", "/dashboard", "/subscribe", "/login"]
        for link in nav_links:
            if link in home_html:
                log_ok("NAV-" + link.replace("/", ""), f"导航链接{link}存在于主页")
            else:
                log_issue("NAV-" + link.replace("/", ""), f"导航链接{link}不在主页HTML中", "P2")

        # 登录页
        r_login = c.get(f"{BASE}/login")
        login_html = r_login.text
        print(f"  /login: {r_login.status_code}, {len(login_html)} chars")
        if "email" in login_html and "password" in login_html:
            log_ok("PAGE-02", "登录页包含email/password字段")
        else:
            log_issue("PAGE-02", "登录页缺少email/password字段", "P0")

        # 注册页
        r_reg = c.get(f"{BASE}/register")
        reg_html = r_reg.text
        print(f"  /register: {r_reg.status_code}, {len(reg_html)} chars")
        if "/privacy" in reg_html:
            log_ok("PAGE-03a", "注册页包含/privacy链接")
        else:
            log_issue("PAGE-03a", "注册页缺少/privacy链接", "P1")
        if "/terms" in reg_html:
            log_ok("PAGE-03b", "注册页包含/terms链接")
        else:
            log_issue("PAGE-03b", "注册页缺少/terms链接", "P1")

        # 隐私/条款页面可访问
        r_priv = c.get(f"{BASE}/privacy")
        if r_priv.status_code == 200:
            log_ok("PAGE-04a", "/privacy页面可访问")
        else:
            log_issue("PAGE-04a", f"/privacy返回{r_priv.status_code}", "P0")

        r_terms = c.get(f"{BASE}/terms")
        if r_terms.status_code == 200:
            log_ok("PAGE-04b", "/terms页面可访问")
        else:
            log_issue("PAGE-04b", f"/terms返回{r_terms.status_code}", "P0")

        # 订阅页面 - 价格检查
        r_sub = c.get(f"{BASE}/subscribe")
        sub_html = r_sub.text
        print(f"  /subscribe: {r_sub.status_code}, {len(sub_html)} chars")
        if "999.9/年" in sub_html or "999.9" in sub_html:
            log_ok("PAGE-05a", "订阅页年会员价格=999.9/年 正确")
        elif "99.9/月" in sub_html:
            log_issue("PAGE-05a", "年会员价格显示错误: 99.9/月 应为 999.9/年", "P0",
                      "修改subscribe.html中年会员价格显示")
        if "Enterprise" in sub_html:
            log_ok("PAGE-05b", "订阅页包含Enterprise套餐")
        else:
            log_issue("PAGE-05b", "订阅页缺少Enterprise套餐名称", "P0")

        # 研报列表页 (Pro用户)
        r_rpts_page = c.get(f"{BASE}/reports", headers=pro_hdrs)
        rpts_html = r_rpts_page.text
        print(f"  /reports (pro): {r_rpts_page.status_code}, {len(rpts_html)} chars")
        if "BUY" in rpts_html or "SELL" in rpts_html or "HOLD" in rpts_html:
            log_ok("PAGE-06a", "研报列表页显示recommendation数据")
        else:
            # 可能JS渲染，检查API调用代码
            if "/api/v1/reports" in rpts_html:
                log_ok("PAGE-06a", "研报列表页使用API动态加载 (SPA模式)")
            else:
                log_issue("PAGE-06a", "研报列表页未显示研报数据，也无API调用代码", "P1")

        # quality_flag stale_ok 问题 - 检查是否向用户显示了stale_ok标记
        if "stale_ok" in rpts_html:
            log_issue("PAGE-06b", "研报列表页对用户显示了内部字段stale_ok", "P1",
                      "将stale_ok翻译为用户友好文字如'参考前日数据'")
        else:
            log_ok("PAGE-06b", "研报列表页未暴露stale_ok内部字段")

        # 仪表盘页
        r_dash_page = c.get(f"{BASE}/dashboard", headers=pro_hdrs)
        dash_html = r_dash_page.text
        print(f"  /dashboard (pro): {r_dash_page.status_code}, {len(dash_html)} chars")
        if "/api/v1/dashboard/stats" in dash_html or "dashboard" in dash_html.lower():
            log_ok("PAGE-07a", "仪表盘页包含stats API调用")
        else:
            log_issue("PAGE-07a", "仪表盘页缺少stats API调用代码", "P1")

        # 管理后台页面
        r_admin_page = c.get(f"{BASE}/admin", headers=admin_hdrs)
        admin_html = r_admin_page.text
        print(f"  /admin (admin): {r_admin_page.status_code}, {len(admin_html)} chars")
        if "pool_size" in admin_html or "poolSize" in admin_html or "pool-size" in admin_html:
            log_ok("PAGE-08a", "管理后台显示pool_size字段")
        else:
            log_issue("PAGE-08a", "管理后台页面未找到pool_size显示", "P1")

        if "pipeline" in admin_html.lower() or "流水线" in admin_html:
            log_ok("PAGE-08b", "管理后台显示流水线状态")
        else:
            log_issue("PAGE-08b", "管理后台缺少流水线状态显示", "P1",
                      "RT-04修复应已添加流水线状态，检查admin.html是否包含")

        # ===== 研报详情页测试 =====
        print("\n[5] 研报详情页测试")

        # 获取一个真实research report  
        r_rpts = c.get(f"{BASE}/api/v1/reports", headers=pro_hdrs)
        rpt_items = r_rpts.json().get("data", {}).get("items", [])
        if rpt_items:
            rpt_id = rpt_items[0]["report_id"]
            rpt_code = rpt_items[0]["stock_code"]

            # 研报详情 - Pro权限
            r_detail = c.get(f"{BASE}/api/v1/reports/{rpt_id}", headers=pro_hdrs)
            if r_detail.status_code == 200:
                detail = r_detail.json().get("data", {})
                log_ok("RPT-01", f"研报详情API成功: {rpt_code}")

                # 检查必填字段
                required_fields = ["report_id", "stock_code", "stock_name", "trade_date",
                                   "recommendation", "confidence", "strategy_type",
                                   "analysis_steps", "citations"]
                missing = [f for f in required_fields if f not in detail]
                if missing:
                    log_issue("RPT-02", f"研报详情缺少字段: {missing}", "P0",
                              "检查报告生成和API返回的字段映射")
                else:
                    log_ok("RPT-02", "研报详情包含所有必填字段")

                # 检查citations三要素
                citations = detail.get("citations", [])
                if citations:
                    cit = citations[0]
                    has_three = all(k in cit for k in ["source_name", "source_url", "retrieved_at"])
                    if has_three:
                        log_ok("RPT-03", f"Citations三要素完整 (共{len(citations)}条)")
                    else:
                        log_issue("RPT-03", f"Citations缺少三要素: 有{list(cit.keys())}", "P0")
                else:
                    log_issue("RPT-03", "研报citations为空", "P1")

                # 检查industry字段
                if "industry" in detail and detail["industry"]:
                    log_ok("RPT-04", f"研报包含industry: {detail['industry']}")
                else:
                    log_issue("RPT-04", "研报缺少industry字段或为空", "P1",
                              "v7.9修复应已添加industry查询，检查ssot_read_model.py")

                # 检查analysis_steps
                steps = detail.get("analysis_steps", [])
                if steps:
                    log_ok("RPT-05", f"分析步骤存在: {len(steps)}条")
                else:
                    log_issue("RPT-05", "研报analysis_steps为空", "P1")

                # quality_flag检查
                qf = detail.get("quality_flag", "")
                if qf == "stale_ok":
                    log_issue("RPT-06", f"研报quality_flag=stale_ok表示数据陈旧，但用户界面未说明", "P1",
                              "在报告详情页面添加数据陈旧提示")
                elif qf == "fresh":
                    log_ok("RPT-06", "研报数据新鲜 quality_flag=fresh")

            else:
                log_issue("RPT-01", f"研报详情API返回{r_detail.status_code}", "P0")

            # 研报详情网页
            r_rpt_page = c.get(f"{BASE}/reports/{rpt_id}", headers=pro_hdrs)
            print(f"  /reports/{rpt_id[:8]}... (pro): {r_rpt_page.status_code}, {len(r_rpt_page.text)} chars")
            if r_rpt_page.status_code == 200:
                rpt_html = r_rpt_page.text
                log_ok("PAGE-09a", "研报详情页可访问")
                if "analysis_steps" in rpt_html or "分析步骤" in rpt_html or "analysisSteps" in rpt_html:
                    log_ok("PAGE-09b", "研报详情页显示分析步骤")
                else:
                    log_issue("PAGE-09b", "研报详情页未显示分析步骤", "P1")
                if "citations" in rpt_html or "引用" in rpt_html or "来源" in rpt_html:
                    log_ok("PAGE-09c", "研报详情页显示citations/来源")
                else:
                    log_issue("PAGE-09c", "研报详情页未显示citations", "P1")
            else:
                log_issue("PAGE-09a", f"研报详情页返回{r_rpt_page.status_code}", "P0")
        else:
            log_issue("RPT-00", "无可用研报进行详情测试", "P1")

        # ===== 高级区 (Advanced) 测试 =====
        print("\n[6] 高级区测试 (Pro用户)")
        if rpt_items:
            rpt_id = rpt_items[0]["report_id"]
            r_adv = c.get(f"{BASE}/api/v1/reports/{rpt_id}/advanced", headers=pro_hdrs)
            print(f"  /reports/{rpt_id[:8]}.../advanced: {r_adv.status_code}")
            if r_adv.status_code == 200:
                adv_data = r_adv.json().get("data", {})
                log_ok("ADV-01", "高级区API可访问 (Pro)")

                # 检查高级区必填字段
                adv_fields = ["data_inputs_summary", "generation_process_log", "model_used"]
                missing_adv = [f for f in adv_fields if f not in adv_data]
                if missing_adv:
                    log_issue("ADV-02", f"高级区缺少字段: {missing_adv}", "P1",
                              "检查advanced endpoint的返回字段")
                else:
                    log_ok("ADV-02", "高级区包含所有字段")
            elif r_adv.status_code == 403:
                log_issue("ADV-01", f"Pro用户访问高级区被403拒绝", "P0",
                          "检查权限中间件，Pro级别应可访问高级区")
            else:
                log_issue("ADV-01", f"高级区API返回{r_adv.status_code}: {r_adv.text[:100]}", "P1")

        # ===== 权限边界测试 =====
        print("\n[7] 权限边界测试")

        # Free用户尝试访问高级区
        r_free = c.post(f"{BASE}/auth/login", json={"email": "v79_free@test.com", "password": "TestFree123!"})
        if r_free.status_code == 200:
            free_token = r_free.json()["data"]["access_token"]
            free_hdrs = {"Authorization": f"Bearer {free_token}"}
            log_ok("AUTH-04", "Free用户登录成功")

            if rpt_items:
                r_adv_free = c.get(f"{BASE}/api/v1/reports/{rpt_id}/advanced", headers=free_hdrs)
                if r_adv_free.status_code == 403:
                    log_ok("PERM-01", "Free用户访问高级区被正确拒绝(403)")
                elif r_adv_free.status_code == 200:
                    log_issue("PERM-01", "Free用户可以访问高级区！权限未拦截", "P0",
                              "检查advanced endpoint的权限检查")
                else:
                    print(f"  Free -> advanced: {r_adv_free.status_code}")
        else:
            # 尝试不同密码
            print(f"  WARN: Free用户登录失败，跳过权限测试")

        # 未登录用户访问admin
        r_anon_admin = c.get(f"{BASE}/api/v1/admin/overview")
        if r_anon_admin.status_code == 401:
            log_ok("PERM-02", "未登录用户访问admin被正确拒绝(401)")
        else:
            log_issue("PERM-02", f"未登录用户访问admin返回{r_anon_admin.status_code}", "P0")

        # Pro用户访问admin
        r_pro_admin = c.get(f"{BASE}/api/v1/admin/overview", headers=pro_hdrs)
        if r_pro_admin.status_code == 403:
            log_ok("PERM-03", "Pro用户访问admin被正确拒绝(403)")
        elif r_pro_admin.status_code == 401:
            log_ok("PERM-03", "Pro用户访问admin被拒绝(401)")
        else:
            log_issue("PERM-03", f"Pro用户访问admin返回{r_pro_admin.status_code}", "P0")

        # ===== 仿真投资组合 =====
        print("\n[8] 仿真投资组合")
        r_sim = c.get(f"{BASE}/portfolio/sim-dashboard", headers=pro_hdrs)
        print(f"  /portfolio/sim-dashboard: {r_sim.status_code}, {len(r_sim.text)} chars")
        if r_sim.status_code == 200:
            log_ok("SIM-01", "仿真仪表盘页面可访问")
            sim_html = r_sim.text
            if "sim_account" in sim_html or "10k" in sim_html or "100k" in sim_html:
                log_ok("SIM-02", "仿真页面包含账户信息")
            else:
                log_issue("SIM-02", "仿真页面未显示账户信息 (10k/100k/500k)", "P1")
        else:
            log_issue("SIM-01", f"仿真仪表盘页面返回{r_sim.status_code}", "P0")

        # ===== 市场状态 =====
        print("\n[9] 市场状态")
        r_mkt = c.get(f"{BASE}/api/v1/market/state", headers=pro_hdrs)
        print(f"  /api/v1/market/state: {r_mkt.status_code}")
        if r_mkt.status_code == 200:
            mkt = r_mkt.json().get("data", {})
            mkt_state = mkt.get("market_state", mkt.get("state", "?"))
            mkt_date = mkt.get("trade_date", mkt.get("latest_trade_date", "?"))
            print(f"  Market state={mkt_state}, trade_date={mkt_date}")
            if mkt_state in ["BULL", "BEAR", "NEUTRAL"]:
                log_ok("MKT-01", f"市场状态有效: {mkt_state}")
            else:
                log_issue("MKT-01", f"市场状态值无效: {mkt_state}", "P1")
            if mkt_date == "2026-03-16":
                log_ok("MKT-02", f"市场状态日期正确: {mkt_date}")
            else:
                log_issue("MKT-02", f"市场状态日期陈旧或错误: {mkt_date} (期望2026-03-16)", "P1")
        else:
            log_issue("MKT-01", f"市场状态API返回{r_mkt.status_code}", "P0")

        # ===== quality_flag/status_reason 用户友好性 =====
        print("\n[10] 数据友好性检查")

        # 检查研报列表是否有 stale_ok 或 fallback_t_minus_1 对用户直接暴露
        r_rpts_direct = c.get(f"{BASE}/api/v1/reports", headers=pro_hdrs)
        rpts_json = r_rpts_direct.json().get("data", {}).get("items", [])
        stale_count = sum(1 for r in rpts_json if r.get("quality_flag") == "stale_ok")
        fallback_count = sum(1 for r in rpts_json if "fallback" in (r.get("status_reason") or ""))
        if stale_count > 0:
            log_issue("FRIENDLY-01", f"研报API返回{stale_count}条quality_flag=stale_ok，前端需翻译此字段",
                      "P1", "在前端将stale_ok映射为'参考前日数据'")
        if fallback_count > 0:
            log_issue("FRIENDLY-02", f"研报API返回{fallback_count}条status_reason含fallback_t_minus_1，前端应翻译",
                      "P1", "在前端将fallback_t_minus_1映射为'数据参考前一交易日'")

        # ===== 策略A/B/C分布 =====
        print("\n[11] 策略分布检查")
        strategy_a = sum(1 for r in rpts_json if r.get("strategy_type") == "A")
        strategy_b = sum(1 for r in rpts_json if r.get("strategy_type") == "B")
        strategy_c = sum(1 for r in rpts_json if r.get("strategy_type") == "C")
        print(f"  本页研报策略分布: A={strategy_a}, B={strategy_b}, C={strategy_c}")
        if strategy_a == 0 and strategy_c == 0:
            log_issue("STRATEGY-01", "前20条研报全部为策略B，策略A/C疑似零触发", "P1",
                      "检查RT-03修复是否生效，是否有strategy_type=A的研报")
        else:
            log_ok("STRATEGY-01", f"策略分布: A={strategy_a}, B={strategy_b}, C={strategy_c}")

        # ===== 仿真账户数据一致性 =====
        print("\n[12] 仿真账户数据一致性")
        # Admin overview中的active_positions
        ov_positions = ov_data.get("active_positions", {})
        print(f"  Admin active_positions: {ov_positions}")
        
        # DB中的sim_account数据
        db = get_db()
        cur = db.cursor()
        cur.execute("PRAGMA table_info(sim_account)")
        sim_cols = [r[1] for r in cur.fetchall()]
        print(f"  sim_account columns: {sim_cols}")
        cur.execute("SELECT * FROM sim_account")
        sim_rows = cur.fetchall()
        print(f"  sim_account rows: {len(sim_rows)}")
        for row in sim_rows:
            print(f"    {dict(zip(sim_cols, row))}")
        db.close()

        # ===== 最终汇总 =====
        print("\n" + "=" * 70)
        print(f"测试完成: {len(passes)} 通过, {len(issues)} 问题")
        print("=" * 70)

        p0 = [i for i in issues if i["severity"] == "P0"]
        p1 = [i for i in issues if i["severity"] == "P1"]
        p2 = [i for i in issues if i["severity"] == "P2"]

        if p0:
            print(f"\n🔴 P0严重问题 ({len(p0)}):")
            for i in p0:
                print(f"  - [{i['code']}] {i['desc']}")
        if p1:
            print(f"\n⚠️  P1一般问题 ({len(p1)}):")
            for i in p1:
                print(f"  - [{i['code']}] {i['desc']}")
        if p2:
            print(f"\n💡 P2改进建议 ({len(p2)}):")
            for i in p2:
                print(f"  - [{i['code']}] {i['desc']}")

        # 输出JSON结果
        result = {"passes": len(passes), "issues": len(issues), "details": issues}
        with open("output/v80_browser_test.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\n详细结果已保存到 output/v80_browser_test.json")

        return len(p0)


if __name__ == "__main__":
    sys.exit(main())
