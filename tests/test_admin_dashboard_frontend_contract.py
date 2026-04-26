from __future__ import annotations

import json
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_node(script: str) -> dict:
    completed = subprocess.run(
        ["node", "-e", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=REPO_ROOT,
        check=True,
    )
    return json.loads(completed.stdout)


def test_admin_page_removes_retired_dag_retrigger_entry(client, create_user):
    account = create_user(
        email="admin-no-retrigger@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )
    login = client.post("/auth/login", json={"email": account["user"].email, "password": account["password"]})
    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

    response = client.get("/admin", headers=headers)

    assert response.status_code == 200
    assert "/api/v1/admin/dag/retrigger" not in response.text
    assert "触发研报生成" not in response.text
    assert "手动重触发" not in response.text


def test_admin_template_overview_failure_does_not_block_other_panels_or_cookie_probe() -> None:
    script = r"""
const fs = require('fs');
const vm = require('vm');

function sanitizeTemplate(text) {
  return text.replace(/\{\{[\s\S]*?\}\}/g, '').replace(/\{%[\s\S]*?%\}/g, '');
}

function extractLastInlineScript(path) {
  const raw = sanitizeTemplate(fs.readFileSync(path, 'utf8'));
  const matches = [...raw.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)];
  if (!matches.length) throw new Error('no inline script found');
  return matches[matches.length - 1][1].replace(/loadAdmin\(\);\s*/g, '');
}

function createClassList(element) {
  const set = new Set();
  return {
    add(...names) { names.forEach((name) => set.add(name)); element.className = Array.from(set).join(' '); },
    remove(...names) { names.forEach((name) => set.delete(name)); element.className = Array.from(set).join(' '); },
    toggle(name, force) {
      if (force === undefined ? !set.has(name) : force) set.add(name);
      else set.delete(name);
      element.className = Array.from(set).join(' ');
      return set.has(name);
    },
    contains(name) { return set.has(name); },
  };
}

function createElement(id = '') {
  const element = {
    id,
    textContent: '',
    innerHTML: '',
    value: '',
    style: { display: '' },
    dataset: {},
    className: '',
    disabled: false,
  };
  element.classList = createClassList(element);
  element.addEventListener = () => {};
  element.querySelectorAll = () => [];
  return element;
}

function createDocument(queryMap) {
  const elements = new Map();
  return {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, createElement(id));
      return elements.get(id);
    },
    querySelectorAll(selector) {
      return queryMap[selector] || [];
    },
  };
}

const mobileTabs = Array.from({ length: 5 }, (_, index) => {
  const el = createElement('mobile-' + index);
  el.dataset.index = String(index);
  return el;
});
const document = createDocument({ '.admin-mobile-tab': mobileTabs });
let cookieCalls = 0;

const context = {
  document,
  window: {},
  console,
  JSON,
  Date,
  Promise,
  Array,
  Object,
  Set,
  Math,
  Number,
  String,
  parseInt,
  parseFloat,
  isNaN,
  alert: () => {},
  confirm: () => true,
  buildRequestId: () => 'req-test',
  getAdminOverview: async () => ({ success: false, error_code: 'OVERVIEW_DOWN', message: 'overview failed' }),
  getAdminSchedulerStatus: async () => ({
    success: true,
    data: {
      items: [
        {
          task_name: 'billing_poller',
          trade_date: '2026-03-22',
          schedule_slot: 'interval_5m',
          trigger_source: 'cron',
          status: 'SUCCESS',
          started_at: '2026-03-22T01:02:03+00:00',
          finished_at: '2026-03-22T01:03:03+00:00',
          retry_count: 0,
          status_reason: 'ok',
        },
      ],
    },
  }),
  getAdminUsers: async () => ({ success: true, data: { items: [] } }),
  getAdminReports: async () => ({ success: true, data: { items: [] } }),
  getAdminSystemStatus: async () => ({
    success: true,
    data: {
      metrics: {
        prediction: {},
        report: {},
        service_health: {},
        business_health: {},
        data_quality: {},
        runtime_state: 'normal',
      },
      source_runtime: {},
    },
  }),
  getAdminCookieSessionHealth: async ({ login_source }) => {
    cookieCalls += 1;
    return {
      success: true,
      data: {
        status: 'ok',
        status_reason: null,
        last_refresh_at: '2026-03-22T02:00:00+00:00',
        login_source,
      },
    };
  },
  patchAdminUser: async () => ({ success: true }),
  module: { exports: {} },
  exports: {},
};

const source = extractLastInlineScript('app/web/templates/admin.html');
vm.runInNewContext(source + '\nmodule.exports = { loadAdmin, switchSection };', context, { filename: 'admin.html' });

(async () => {
  await context.module.exports.loadAdmin();
  const firstCookieCalls = cookieCalls;
  context.module.exports.switchSection('sessions');
  await context.module.exports.loadAdmin();
  process.stdout.write(JSON.stringify({
    errorText: document.getElementById('admin-err').textContent,
    schedulerHtml: document.getElementById('scheduler-body').innerHTML,
    systemStatusHtml: document.getElementById('system-status-grid').innerHTML,
    sourceDatesHtml: document.getElementById('overview-source-dates').innerHTML,
    firstCookieCalls,
    finalCookieCalls: cookieCalls,
    sessionsHtml: document.getElementById('sessions-body').innerHTML,
  }));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
"""
    result = _run_node(script)

    assert "总览加载失败" in result["errorText"]
    assert "billing_poller" not in result["schedulerHtml"]
    assert "支付对账" in result["schedulerHtml"]
    assert "运行健康" in result["systemStatusHtml"]
    assert "总览接口加载失败" in result["sourceDatesHtml"]
    assert result["firstCookieCalls"] == 0
    assert result["finalCookieCalls"] == 4
    assert "未录入" not in result["sessionsHtml"]


def test_admin_template_overview_success_renders_fr07_pipeline_status_mapping() -> None:
    script = r"""
const fs = require('fs');
const vm = require('vm');

function sanitizeTemplate(text) {
  return text.replace(/\{\{[\s\S]*?\}\}/g, '').replace(/\{%[\s\S]*?%\}/g, '');
}

function extractLastInlineScript(path) {
  const raw = sanitizeTemplate(fs.readFileSync(path, 'utf8'));
  const matches = [...raw.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)];
  if (!matches.length) throw new Error('no inline script found');
  return matches[matches.length - 1][1].replace(/loadAdmin\(\);\s*/g, '');
}

function createClassList(element) {
  const set = new Set();
  return {
    add(...names) { names.forEach((name) => set.add(name)); element.className = Array.from(set).join(' '); },
    remove(...names) { names.forEach((name) => set.delete(name)); element.className = Array.from(set).join(' '); },
    toggle(name, force) {
      if (force === undefined ? !set.has(name) : force) set.add(name);
      else set.delete(name);
      element.className = Array.from(set).join(' ');
      return set.has(name);
    },
    contains(name) { return set.has(name); },
  };
}

function createElement(id = '') {
  const element = {
    id,
    textContent: '',
    innerHTML: '',
    value: '',
    style: { display: '' },
    dataset: {},
    className: '',
    disabled: false,
  };
  element.classList = createClassList(element);
  element.addEventListener = () => {};
  element.querySelectorAll = () => [];
  return element;
}

function createDocument() {
  const elements = new Map();
  return {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, createElement(id));
      return elements.get(id);
    },
    querySelectorAll() { return []; },
  };
}

const document = createDocument();
const context = {
  document,
  window: {},
  console,
  JSON,
  Date,
  Promise,
  Array,
  Object,
  Set,
  Math,
  Number,
  String,
  parseInt,
  parseFloat,
  isNaN,
  alert: () => {},
  confirm: () => true,
  module: { exports: {} },
  exports: {},
};

const source = extractLastInlineScript('app/web/templates/admin.html');
vm.runInNewContext(source + '\nmodule.exports = { renderOverview };', context, { filename: 'admin.html' });
context.module.exports.renderOverview({
  pool_size: 200,
  today_reports: 120,
  today_buy_signals: 40,
  pending_review: 3,
  data_freshness: { latest_kline_date: '2026-03-23', latest_market_state_date: '2026-03-23' },
  report_generation: { total: 120, pool_size: 200, progress_pct: 60, by_strategy: { A: 50, B: 40, C: 30 } },
  source_dates: { runtime_trade_date: '2026-03-23', public_pool_trade_date: '2026-03-23', stats_snapshot_date: '2026-03-23', sim_snapshot_date: '2026-03-23' },
  pipeline_stages: {
    fr07_settlement: {
      status: 'PARTIAL_SUCCESS',
      pipeline_status: 'DEGRADED',
      started_at: '2026-03-23T08:00:00+00:00',
      completed_at: '2026-03-23T08:05:00+00:00',
      error: 'partial_failure'
    }
  },
  active_positions: { '100k': 2 }
});

process.stdout.write(JSON.stringify({
  pipelineHtml: document.getElementById('pipeline-stage-body').innerHTML,
  sourceDatesHtml: document.getElementById('overview-source-dates').innerHTML
}));
"""
    result = _run_node(script)

    assert "部分成功" in result["pipelineHtml"]
    assert "partial_failure" in result["pipelineHtml"]
    assert "运行日" in result["sourceDatesHtml"]


def test_dashboard_template_clears_stale_ui_on_api_failure() -> None:
    script = r"""
const fs = require('fs');
const vm = require('vm');

function sanitizeTemplate(text) {
  return text.replace(/\{\{[\s\S]*?\}\}/g, '').replace(/\{%[\s\S]*?%\}/g, '');
}

function extractLastInlineScript(path) {
  const raw = sanitizeTemplate(fs.readFileSync(path, 'utf8'));
  const matches = [...raw.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)];
  if (!matches.length) throw new Error('no inline script found');
  return matches[matches.length - 1][1].replace(/loadDashboard\(\);\s*/g, '');
}

function createClassList(element) {
  const set = new Set();
  return {
    add(...names) { names.forEach((name) => set.add(name)); element.className = Array.from(set).join(' '); },
    remove(...names) { names.forEach((name) => set.delete(name)); element.className = Array.from(set).join(' '); },
    toggle(name, force) {
      if (force === undefined ? !set.has(name) : force) set.add(name);
      else set.delete(name);
      element.className = Array.from(set).join(' ');
      return set.has(name);
    },
    contains(name) { return set.has(name); },
  };
}

function createElement(id = '') {
  const element = {
    id,
    textContent: '',
    innerHTML: '',
    value: '',
    style: { display: '' },
    dataset: {},
    className: '',
    disabled: false,
  };
  element.classList = createClassList(element);
  element.addEventListener = () => {};
  element.querySelectorAll = () => [];
  return element;
}

function createDocument(queryMap) {
  const elements = new Map();
  return {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, createElement(id));
      return elements.get(id);
    },
    querySelectorAll(selector) {
      return queryMap[selector] || [];
    },
  };
}

const tabs = Array.from({ length: 5 }, (_, index) => {
  const el = createElement('tab-' + index);
  el.dataset.window = ['1', '7', '14', '30', '60'][index] || '30';
  return el;
});
const document = createDocument({ '.db-window-tab': tabs });
document.getElementById('overall-win-rate').textContent = '99.9%';
document.getElementById('overall-win-rate').className = 'db-sum-val positive';
document.getElementById('overall-pnl-ratio').textContent = '3.40';
document.getElementById('overall-pnl-ratio').className = 'db-sum-val positive';
document.getElementById('strategy-grid').innerHTML = 'STALE_STRATEGY';
document.getElementById('baseline-body').innerHTML = 'STALE_BASELINE';
document.getElementById('date-range').textContent = 'stale-range';
document.getElementById('total-reports').textContent = '88';
document.getElementById('total-settled').textContent = '44';
document.getElementById('stats-anchor-note').textContent = 'stale-note';
document.getElementById('kpi-settled').textContent = '44';
document.getElementById('kpi-total').textContent = '88';

const context = {
  document,
  window: {},
  console,
  JSON,
  Date,
  Promise,
  getDashboardStats: async () => ({ success: false, error_code: 'UPSTREAM_TIMEOUT', message: 'boom' }),
  module: { exports: {} },
  exports: {},
};

const source = extractLastInlineScript('app/web/templates/dashboard.html');
vm.runInNewContext(source + '\nmodule.exports = { loadDashboard };', context, { filename: 'dashboard.html' });

(async () => {
  await context.module.exports.loadDashboard();
  process.stdout.write(JSON.stringify({
    statusText: document.getElementById('dashboard-status').textContent,
    winRate: document.getElementById('overall-win-rate').textContent,
    pnlRatio: document.getElementById('overall-pnl-ratio').textContent,
    strategyHtml: document.getElementById('strategy-grid').innerHTML,
    baselineHtml: document.getElementById('baseline-body').innerHTML,
    dateRange: document.getElementById('date-range').textContent,
    statsAnchorNote: document.getElementById('stats-anchor-note').textContent,
  }));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
"""
    result = _run_node(script)

    assert "请求失败" in result["statusText"]
    assert result["winRate"] == "—"
    assert result["pnlRatio"] == "—"
    assert "STALE_STRATEGY" not in result["strategyHtml"]
    assert "加载失败" in result["strategyHtml"]
    assert "STALE_BASELINE" not in result["baselineHtml"]
    assert "统计接口加载失败" in result["baselineHtml"]
    assert result["dateRange"] == "加载失败"
    assert "统计接口加载失败" in result["statsAnchorNote"]


def test_sim_dashboard_template_clears_stale_ui_on_api_failure() -> None:
    script = r"""
const fs = require('fs');
const vm = require('vm');

function sanitizeTemplate(text) {
  return text.replace(/\{\{[\s\S]*?\}\}/g, '').replace(/\{%[\s\S]*?%\}/g, '');
}

function extractLastInlineScript(path) {
  const raw = sanitizeTemplate(fs.readFileSync(path, 'utf8'));
  const matches = [...raw.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)];
  if (!matches.length) throw new Error('no inline script found');
  return matches[matches.length - 1][1].replace(/initTiers\(\)\.then\(loadSimDashboard\);\s*/g, '');
}

function createClassList(element) {
  const set = new Set();
  return {
    add(...names) { names.forEach((name) => set.add(name)); element.className = Array.from(set).join(' '); },
    remove(...names) { names.forEach((name) => set.delete(name)); element.className = Array.from(set).join(' '); },
    toggle(name, force) {
      if (force === undefined ? !set.has(name) : force) set.add(name);
      else set.delete(name);
      element.className = Array.from(set).join(' ');
      return set.has(name);
    },
    contains(name) { return set.has(name); },
  };
}

function createElement(id = '') {
  const element = {
    id,
    textContent: '',
    innerHTML: '',
    value: '',
    style: { display: '' },
    dataset: {},
    className: '',
    disabled: false,
  };
  element.classList = createClassList(element);
  element.addEventListener = () => {};
  element.querySelectorAll = () => [];
  return element;
}

function createDocument(queryMap) {
  const elements = new Map();
  return {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, createElement(id));
      return elements.get(id);
    },
    querySelectorAll(selector) {
      return queryMap[selector] || [];
    },
  };
}

const document = createDocument({});
document.getElementById('metric-return').textContent = '88.8%';
document.getElementById('metric-return').className = 'val positive';
document.getElementById('metric-win-rate').textContent = '66.0%';
document.getElementById('metric-pnl-ratio').textContent = '2.20';
document.getElementById('metric-drawdown').textContent = '-1.0%';
document.getElementById('metric-sample-size').textContent = '18';
document.getElementById('chart-hint').textContent = 'stale hint';
document.getElementById('sim-source-dates').style.display = '';
document.getElementById('sim-source-dates').textContent = 'stale source';
document.getElementById('drawdown-banner').style.display = 'block';
document.getElementById('drawdown-banner').textContent = 'stale drawdown';
document.getElementById('equity-chart').style.display = '';
document.getElementById('equity-chart').innerHTML = 'STALE_CHART';
document.getElementById('chart-empty').style.display = 'none';
document.getElementById('chart-legend').style.display = 'flex';
document.getElementById('underperform-warning').style.display = 'block';
document.getElementById('open-positions').innerHTML = 'STALE_POSITIONS';

const context = {
  document,
  window: {},
  console,
  JSON,
  Date,
  Promise,
  getPortfolioSimDashboard: async () => ({ success: false, error_code: 'UPSTREAM_TIMEOUT', message: 'sim boom' }),
  module: { exports: {} },
  exports: {},
};

const source = extractLastInlineScript('app/web/templates/sim_dashboard.html');
vm.runInNewContext(source + '\nmodule.exports = { loadSimDashboard };', context, { filename: 'sim_dashboard.html' });

(async () => {
  await context.module.exports.loadSimDashboard();
  process.stdout.write(JSON.stringify({
    statusText: document.getElementById('sim-status').textContent,
    metricReturn: document.getElementById('metric-return').textContent,
    metricWinRate: document.getElementById('metric-win-rate').textContent,
    chartDisplay: document.getElementById('equity-chart').style.display,
    chartHtml: document.getElementById('equity-chart').innerHTML,
    chartEmptyText: document.getElementById('chart-empty').textContent,
    legendDisplay: document.getElementById('chart-legend').style.display,
    warningDisplay: document.getElementById('underperform-warning').style.display,
    sourceDisplay: document.getElementById('sim-source-dates').style.display,
    sourceText: document.getElementById('sim-source-dates').textContent,
    drawdownDisplay: document.getElementById('drawdown-banner').style.display,
    positionsHtml: document.getElementById('open-positions').innerHTML,
    chartHint: document.getElementById('chart-hint').textContent,
  }));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
"""
    result = _run_node(script)

    assert "请求失败" in result["statusText"]
    assert result["metricReturn"] == "计算中"
    assert result["metricWinRate"] == "计算中"
    assert result["chartDisplay"] == "none"
    assert result["chartHtml"] == ""
    assert result["chartEmptyText"] == "模拟收益接口加载失败"
    assert result["legendDisplay"] == "none"
    assert result["warningDisplay"] == "none"
    assert result["sourceDisplay"] == "none"
    assert result["sourceText"] == ""
    assert result["drawdownDisplay"] == "none"
    assert "加载失败" in result["positionsHtml"]
    assert result["chartHint"] == "模拟收益接口加载失败"


def test_admin_system_status_uses_metrics_today_reports_as_canonical(client, create_user, monkeypatch):
    import app.api.routes_business as routes_business
    import app.services.observability as observability
    import app.services.source_state as source_state

    account = create_user(
        email="system-status-canonical@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )
    login = client.post("/auth/login", json={"email": account["user"].email, "password": account["password"]})
    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

    monkeypatch.setattr(routes_business, "report_storage_mode", lambda db: "ssot")
    monkeypatch.setattr(routes_business, "count_reports_ssot", lambda db, created_at_from=None: 99 if created_at_from is None else 3)
    monkeypatch.setattr(
        routes_business,
        "get_public_pool_snapshot_ssot",
        lambda db: {"pool_view": None, "pool_size": 0},
    )
    monkeypatch.setattr(
        observability,
        "runtime_metrics_summary",
        lambda db: {
            "report": {"today_reports": 7, "generated_24h": 12},
            "prediction": {},
            "service_health": {},
            "business_health": {},
            "data_quality": {},
        },
    )
    monkeypatch.setattr(source_state, "get_source_runtime_status", lambda: {})

    response = client.get("/api/v1/admin/system-status", headers=headers)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["metrics"]["report"]["today_reports"] == 7
    assert data["counts"]["reports_today"] == 7
    assert data["tasks"]["reports_today"] == 7
