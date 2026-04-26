from __future__ import annotations

import json
import subprocess
from pathlib import Path


def _run_api_bridge_case(case_name: str) -> str:
    script = f"""
const fs = require('fs');
const vm = require('vm');

const source = fs.readFileSync('app/web/api-bridge.js', 'utf8');

async function runCase() {{
  let fetchImpl;
  if ({case_name!r} === 'empty-200') {{
    fetchImpl = async () => ({{
      ok: true,
      status: 200,
      text: async () => '',
    }});
  }} else if ({case_name!r} === 'html-200') {{
    fetchImpl = async () => ({{
      ok: true,
      status: 200,
      text: async () => '<html>ok</html>',
    }});
  }} else if ({case_name!r} === 'login-unauthorized') {{
    fetchImpl = async () => ({{
      ok: false,
      status: 401,
      text: async () => JSON.stringify({{
        success: false,
        error_code: 'UNAUTHORIZED',
        error_message: 'UNAUTHORIZED',
        request_id: 'req-login-unauthorized',
        data: null
      }}),
    }});
  }} else if ({case_name!r} === 'login-email-not-verified') {{
    fetchImpl = async () => ({{
      ok: false,
      status: 401,
      text: async () => JSON.stringify({{
        success: false,
        error_code: 'EMAIL_NOT_VERIFIED',
        error_message: 'EMAIL_NOT_VERIFIED',
        request_id: 'req-login-unverified',
        data: null
      }}),
    }});
  }} else if ({case_name!r} === 'json-no-envelope') {{
    fetchImpl = async () => ({{
      ok: true,
      status: 200,
      text: async () => JSON.stringify({{ foo: 'bar' }}),
    }});
  }} else {{
    fetchImpl = async () => ({{
      ok: true,
      status: 200,
      text: async () => JSON.stringify({{ success: true, data: {{ status: 'ok' }} }}),
    }});
  }}

  const context = {{
    fetch: fetchImpl,
    window: {{ __API_BASE__: '/api/v1' }},
    crypto: {{ randomUUID: () => 'req-test-id' }},
    console,
    JSON,
    module: {{ exports: {{}} }},
    exports: {{}},
  }};

  vm.runInNewContext(source + '\\nmodule.exports = {{ apiFetch }};', context, {{ filename: 'api-bridge.js' }});
  const result = await context.module.exports.apiFetch('/auth/login', {{
    method: 'POST',
    body: JSON.stringify({{ email: 'demo@example.com', password: 'Password123' }}),
  }});
  process.stdout.write(JSON.stringify(result));
}}

runCase().catch((error) => {{
  console.error(error);
  process.exit(1);
}});
"""
    completed = subprocess.run(
        ["node", "-e", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=Path(__file__).resolve().parents[1],
        check=True,
    )
    return completed.stdout


def _run_api_bridge_helper_probe() -> str:
    script = """
const fs = require('fs');
const vm = require('vm');

const source = fs.readFileSync('app/web/api-bridge.js', 'utf8');
const calls = [];

const context = {
  fetch: async (url, options = {}) => {
    calls.push({ url, method: options.method || 'GET' });
    return {
      ok: true,
      status: 200,
      text: async () => JSON.stringify({
        success: true,
        data: { ok: true },
        request_id: 'req-bridge-helper'
      }),
    };
  },
  window: { __API_BASE__: '/api/v1' },
  crypto: { randomUUID: () => 'req-test-id' },
  console,
  JSON,
  module: { exports: {} },
  exports: {},
};

vm.runInNewContext(
  source + '\\nmodule.exports = { getFeaturesCatalog, getGovernanceCatalog, getHealthStatus };',
  context,
  { filename: 'api-bridge.js' }
);

async function runProbe() {
  const live = await context.module.exports.getFeaturesCatalog('live');
  const snapshot = await context.module.exports.getGovernanceCatalog('snapshot');
  const health = await context.module.exports.getHealthStatus();
  process.stdout.write(JSON.stringify({ calls, live, snapshot, health }));
}

runProbe().catch((error) => {
  console.error(error);
  process.exit(1);
});
"""
    completed = subprocess.run(
        ["node", "-e", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=Path(__file__).resolve().parents[1],
        check=True,
    )
    return completed.stdout


def _run_api_bridge_home_helper_probe() -> str:
    script = """
const fs = require('fs');
const vm = require('vm');

const source = fs.readFileSync('app/web/api-bridge.js', 'utf8');
const calls = [];

const context = {
  fetch: async (url, options = {}) => {
    calls.push({ url, method: options.method || 'GET' });
    if (url === '/api/v1/market/hot-stocks?limit=6') {
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({
          success: true,
          data: {
            items: [{
              stock_code: '600519.SH',
              stock_name: '贵州茅台',
              rank: 1,
              topic_title: 'AI热搜',
              source_name: 'hotspot',
              heat_score: 99,
            }],
            source: 'hotspot',
          },
          request_id: 'req-bridge-home-helper'
        }),
      };
    }
    return {
      ok: true,
      status: 200,
      text: async () => JSON.stringify({
        success: true,
        data: { ok: true },
        request_id: 'req-bridge-home-helper'
      }),
    };
  },
  window: { __API_BASE__: '/api/v1' },
  crypto: { randomUUID: () => 'req-test-id' },
  console,
  JSON,
  module: { exports: {} },
  exports: {},
};

vm.runInNewContext(
  source + '\\nmodule.exports = { ApiBridge: window.ApiBridge, getHomePayload, getMarketState, getHotStocks, getPoolStocks, resolveHomeAuthoritativeTradeDate, resolveHomeAnchorMismatchReason };',
  context,
  { filename: 'api-bridge.js' }
);

async function runProbe() {
  const home = await context.module.exports.getHomePayload();
  const market = await context.module.exports.getMarketState();
  const hot = await context.module.exports.getHotStocks(6);
  const pool = await context.module.exports.getPoolStocks('2026-03-23');
  const authoritative = context.module.exports.resolveHomeAuthoritativeTradeDate({
    trade_date: '2026-03-23',
    public_performance: { runtime_trade_date: '2026-03-22' },
  });
  const fallback = context.module.exports.resolveHomeAuthoritativeTradeDate({
    trade_date: null,
    public_performance: { runtime_trade_date: '2026-03-22' },
  });
  const mismatch = context.module.exports.resolveHomeAnchorMismatchReason('2026-03-23', {
    market_state_date: '2099-01-02',
    reference_date: '2099-01-02',
  });
  process.stdout.write(JSON.stringify({ calls, home, market, hot, pool, authoritative, fallback, mismatch, hasApiBridge: !!context.module.exports.ApiBridge }));
}

runProbe().catch((error) => {
  console.error(error);
  process.exit(1);
});
"""
    completed = subprocess.run(
        ["node", "-e", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=Path(__file__).resolve().parents[1],
        check=True,
    )
    return completed.stdout


def _run_api_bridge_result_helper_probe() -> str:
    script = """
const fs = require('fs');
const vm = require('vm');

const source = fs.readFileSync('app/web/api-bridge.js', 'utf8');

const context = {
  fetch: async () => ({
    ok: true,
    status: 200,
    text: async () => JSON.stringify({
      success: true,
      data: { ok: true },
      request_id: 'req-bridge-helper'
    }),
  }),
  window: { __API_BASE__: '/api/v1' },
  crypto: { randomUUID: () => 'req-test-id' },
  console,
  JSON,
  module: { exports: {} },
  exports: {},
};

vm.runInNewContext(
  source + '\\nmodule.exports = { getBridgeData, getSettledBridgeData };',
  context,
  { filename: 'api-bridge.js' }
);

async function runProbe() {
  const fulfilled = context.module.exports.getSettledBridgeData(
    { status: 'fulfilled', value: { success: true, data: { anchor: '2026-03-23' } } },
    '目录'
  );
  const fulfilledError = context.module.exports.getSettledBridgeData(
    { status: 'fulfilled', value: { success: false, message: 'bridge-failed', data: null } },
    '目录'
  );
  const rejected = context.module.exports.getSettledBridgeData(
    { status: 'rejected', reason: { message: 'network down' } },
    '目录'
  );
  const direct = context.module.exports.getBridgeData(
    { success: true, data: { state: 'ready' } },
    '目录'
  );
  process.stdout.write(JSON.stringify({ fulfilled, fulfilledError, rejected, direct }));
}

runProbe().catch((error) => {
  console.error(error);
  process.exit(1);
});
"""
    completed = subprocess.run(
        ["node", "-e", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=Path(__file__).resolve().parents[1],
        check=True,
    )
    return completed.stdout


def _run_api_bridge_auth_payload_probe() -> str:
    script = """
const fs = require('fs');
const vm = require('vm');

const source = fs.readFileSync('app/web/api-bridge.js', 'utf8');
const calls = [];

const context = {
  fetch: async (url, options = {}) => {
    calls.push({
      url,
      method: options.method || 'GET',
      body: options.body ? JSON.parse(options.body) : null,
    });
    return {
      ok: true,
      status: 200,
      text: async () => JSON.stringify({
        success: true,
        data: { ok: true },
        request_id: 'req-auth-payload-probe'
      }),
    };
  },
  window: { __API_BASE__: '/api/v1' },
  crypto: { randomUUID: () => 'req-test-id' },
  console,
  JSON,
  URLSearchParams,
  encodeURIComponent,
  module: { exports: {} },
  exports: {},
};

vm.runInNewContext(
  source + '\\nmodule.exports = { postLogin, postRegister };',
  context,
  { filename: 'api-bridge.js' }
);

async function runProbe() {
  await context.module.exports.postLogin({ email: 'demo@example.com', password: 'Password123' });
  await context.module.exports.postRegister({ email: 'demo@example.com', password: 'Password123', password_confirm: 'Password123' });
  process.stdout.write(JSON.stringify({ calls }));
}

runProbe().catch((error) => {
  console.error(error);
  process.exit(1);
});
"""
    completed = subprocess.run(
        ["node", "-e", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=Path(__file__).resolve().parents[1],
        check=True,
    )
    return completed.stdout


def test_api_bridge_rejects_empty_200() -> None:
    output = _run_api_bridge_case("empty-200")
    assert '"success":false' in output
    assert '"error_code":"INVALID_RESPONSE"' in output


def test_api_bridge_rejects_non_json_200() -> None:
    output = _run_api_bridge_case("html-200")
    assert '"success":false' in output
    assert '"error_code":"INVALID_RESPONSE"' in output


def test_api_bridge_rejects_json_without_envelope() -> None:
    output = _run_api_bridge_case("json-no-envelope")
    assert '"success":false' in output
    assert '"error_code":"INVALID_RESPONSE"' in output


def test_api_bridge_accepts_valid_envelope() -> None:
    output = _run_api_bridge_case("valid-json")
    assert '"success":true' in output
    assert '"status":"ok"' in output


def test_api_bridge_maps_login_unauthorized_to_friendly_message() -> None:
    output = _run_api_bridge_case("login-unauthorized")
    assert '"success":false' in output
    assert '"error_code":"UNAUTHORIZED"' in output
    assert '邮箱或密码不正确，请重新输入。' in output
    assert '"message":"UNAUTHORIZED"' not in output


def test_api_bridge_maps_login_unverified_to_friendly_message() -> None:
    output = _run_api_bridge_case("login-email-not-verified")
    assert '"success":false' in output
    assert '"error_code":"EMAIL_NOT_VERIFIED"' in output
    assert '当前账号尚未完成邮箱激活（若系统启用该能力）。' in output
    assert '"message":"EMAIL_NOT_VERIFIED"' not in output


def test_api_bridge_exposes_shared_helpers_for_features_catalog_and_health() -> None:
    output = _run_api_bridge_helper_probe()
    assert '"/api/v1/features/catalog?source=live"' in output
    assert '"/api/v1/governance/catalog?source=snapshot"' in output
    assert '"/health"' in output
    assert '"/api/v1/health"' not in output
    assert '"success":true' in output


def test_api_bridge_exposes_shared_home_helpers_for_authoritative_and_supplemental_calls() -> None:
    payload = json.loads(_run_api_bridge_home_helper_probe())
    calls = payload["calls"]
    assert payload["hasApiBridge"] is True
    assert any(call["url"] == "/api/v1/home" for call in calls)
    assert any(call["url"] == "/api/v1/market/state" for call in calls)
    assert any(call["url"] == "/api/v1/market/hot-stocks?limit=6" for call in calls)
    assert any(call["url"] == "/api/v1/pool/stocks?trade_date=2026-03-23" for call in calls)
    assert payload["home"]["success"] is True
    assert payload["hot"]["success"] is True
    assert payload["hot"]["data"]["items"][0]["rank"] == 1
    assert payload["hot"]["data"]["items"][0]["topic_title"] == "AI热搜"
    assert payload["hot"]["data"]["items"][0]["source_name"] == "hotspot"
    assert payload["authoritative"] == "2026-03-23"
    assert payload["fallback"] == "2026-03-22"
    assert "2099-01-02" not in payload["mismatch"]


def test_api_bridge_exposes_shared_result_helpers_for_page_level_consumers() -> None:
    payload = json.loads(_run_api_bridge_result_helper_probe())
    assert payload["fulfilled"] == {
        "ok": True,
        "data": {"anchor": "2026-03-23"},
        "error": None,
    }
    assert payload["fulfilledError"] == {
        "ok": False,
        "data": None,
        "error": "目录读取失败 · bridge-failed",
    }
    assert payload["rejected"] == {
        "ok": False,
        "data": None,
        "error": "目录读取失败 · network down",
    }
    assert payload["direct"] == {"state": "ready"}


def test_api_bridge_auth_posts_email_only_payload() -> None:
    payload = json.loads(_run_api_bridge_auth_payload_probe())
    calls = payload["calls"]
    assert calls[0]["url"] == "/auth/login"
    assert calls[0]["body"] == {"email": "demo@example.com", "password": "Password123"}
    assert "account" not in calls[0]["body"]
    assert calls[1]["url"] == "/auth/register"
    assert calls[1]["body"] == {
        "email": "demo@example.com",
        "password": "Password123",
        "password_confirm": "Password123",
    }
    assert "account" not in calls[1]["body"]
