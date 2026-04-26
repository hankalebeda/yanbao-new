/**
 * API 调用（与 05_API与数据契约 路径一致）
 * 接入真实 /api/v1/* 接口；API_BASE 由 _app_config.html 注入或使用默认值
 */
const API_BASE = (typeof window !== 'undefined' && window.__API_BASE__) || '/api/v1';

const _FRIENDLY_MESSAGES = {
  'UNAUTHORIZED': '邮箱或密码不正确，请重新输入。',
  'EMAIL_NOT_VERIFIED': '当前账号尚未完成邮箱激活（若系统启用该能力）。',
};

function _resolveApiPath(path) {
  // Auth/billing routes bypass API_BASE prefix
  if (path.startsWith('/auth/') || path.startsWith('/billing/')) return path;
  if (path.startsWith('http')) return path;
  if (path.startsWith('/auth')) return path;
  if (path.startsWith('/health')) return path;
  return API_BASE + path;
}

async function apiFetch(path, options = {}) {
  const url = _resolveApiPath(path);
  try {
    const r = await fetch(url, { ...options, credentials: 'same-origin', headers: { 'Content-Type': 'application/json', ...options.headers } });
    const text = await r.text();
    var j = null;
    try { j = text ? JSON.parse(text) : null; } catch(_) { j = null; }
    if (r.ok) {
      // Validate: must be parseable JSON with envelope shape
      if (!j || typeof j !== 'object') {
        return { success: false, error_code: 'INVALID_RESPONSE', message: '服务器返回了无效响应', data: null };
      }
      if (j.success !== undefined) return j;
      // No success field — not a valid envelope
      if (j.data === undefined) {
        return { success: false, error_code: 'INVALID_RESPONSE', message: '服务器返回了无效响应', data: null };
      }
      return { success: true, data: j.data, request_id: j.request_id || null };
    }
    // Error responses — pass through if already has envelope shape
    if (j && j.success === false) {
      // Map known error codes to friendly messages
      if (j.error_code && _FRIENDLY_MESSAGES[j.error_code]) {
        j.error_message = _FRIENDLY_MESSAGES[j.error_code];
        j.message = _FRIENDLY_MESSAGES[j.error_code];
      }
      return j;
    }
    var msg = (j && typeof j.detail === 'string') ? j.detail
      : (j && Array.isArray(j.detail) && j.detail[0] && j.detail[0].msg) ? j.detail[0].msg
      : (j && j.message) ? j.message
      : (text && text.length < 200) ? text
      : '请求失败';
    var result = { success: false, message: typeof msg === 'string' ? msg : JSON.stringify(msg), data: null };
    if (j && j.error_code) result.error_code = j.error_code;
    if (j && j.error_message) result.error_message = j.error_message;
    if (j && j.error) result.error = j.error;
    if (j && j.request_id) result.request_id = j.request_id;
    return result;
  } catch (e) {
    return { success: false, message: '网络错误: ' + (e.message || '未知'), error: { type: 'NetworkError' }, data: null };
  }
}

async function getReports(params = {}) {
  const q = new URLSearchParams(params).toString();
  return apiFetch(`/reports${q ? '?' + q : ''}`);
}
async function getReport(id) { return apiFetch(`/reports/${id}`); }

async function getSimPositions(params = {}) {
  const q = new URLSearchParams(params).toString();
  return apiFetch(`/sim/positions${q ? '?' + q : ''}`);
}
async function getSimPositionByReport(reportId) { return apiFetch(`/sim/positions/by-report/${reportId}`); }
async function getPlatformConfig() { return apiFetch('/platform/config'); }
async function getPlatformPlans() { return apiFetch('/platform/plans'); }
async function getOAuthProviders() { return apiFetch('/auth/oauth/providers'); }

async function getSimAccountSummary(capitalTier) {
  if (capitalTier == null) {
    try { var c = await getPlatformConfig(); capitalTier = (c && c.success && c.data && c.data.default_capital_tier) ? c.data.default_capital_tier : '10w'; } catch (_) { capitalTier = '10w'; }
  }
  return apiFetch(`/sim/account/summary?capital_tier=${capitalTier}`);
}
async function getSimAccountSnapshots(params = {}) {
  const q = new URLSearchParams(params).toString();
  return apiFetch(`/sim/account/snapshots${q ? '?' + q : ''}`);
}

async function getMarketState() { return apiFetch('/market/state'); }
async function getHotStocks(limit) { return apiFetch('/market/hot-stocks' + (limit != null ? '?limit=' + limit : '')); }
async function getPlatformSummary() { return apiFetch('/platform/summary'); }

function _authBody(body) {
  return body || {};
}
async function postLogin(body) { return apiFetch('/auth/login', { method: 'POST', body: JSON.stringify({ email: (body && body.email) || (body && body.account), password: body && body.password }) }); }
async function postRegister(body) {
  var b = {};
  if (body) { b.email = body.email || body.account; b.password = body.password; if (body.password_confirm) b.password_confirm = body.password_confirm; }
  return apiFetch('/auth/register', { method: 'POST', body: JSON.stringify(b) });
}

async function postReportFeedback(body) { return apiFetch('/report-feedback', { method: 'POST', body: JSON.stringify(body) }); }

async function getAuthMe() { return apiFetch('/auth/me'); }
async function postAuthRefresh(body) { return apiFetch('/auth/refresh', { method: 'POST', body: JSON.stringify(body || {}) }); }
async function postForgotPassword(body) { return apiFetch('/auth/forgot-password', { method: 'POST', body: JSON.stringify(body || {}) }); }
async function postResetPassword(body) { return apiFetch('/auth/reset-password', { method: 'POST', body: JSON.stringify(body || {}) }); }
async function getMembershipStatus(userId) { return apiFetch('/membership/subscription/status?user_id=' + encodeURIComponent(userId)); }

async function getAdminUsers(params) {
  const q = new URLSearchParams(params || {}).toString();
  return apiFetch('/admin/users' + (q ? '?' + q : ''));
}
async function patchAdminUser(userId, body) { return apiFetch('/admin/users/' + userId, { method: 'PATCH', body: JSON.stringify(body || {}) }); }
async function getAdminSystemStatus() { return apiFetch('/admin/system-status'); }
async function getAdminOverview() { return apiFetch('/admin/overview'); }
async function getAdminReports(params) {
  var q = new URLSearchParams(params || {}).toString();
  return apiFetch('/admin/reports' + (q ? '?' + q : ''));
}
async function getAdminSchedulerStatus(params) {
  var q = new URLSearchParams(params || {}).toString();
  return apiFetch('/admin/scheduler/status' + (q ? '?' + q : ''));
}
async function getAdminCookieSessionHealth() { return apiFetch('/admin/cookie-session/health'); }
async function getDashboardStats(days) {
  return apiFetch('/dashboard/stats' + (days != null ? '?window_days=' + days : ''));
}
async function getPortfolioSimDashboard(params) {
  var query = {};
  if (typeof params === 'string') {
    query.capital_tier = params;
  } else if (params && typeof params === 'object') {
    query = params;
  }
  var q = new URLSearchParams(query).toString();
  return apiFetch('/portfolio/sim-dashboard' + (q ? '?' + q : ''));
}

async function getPoolStocks(tradeDate) { return apiFetch('/pool/stocks' + (tradeDate ? '?trade_date=' + tradeDate : '')); }
async function getHomePayload() { return apiFetch('/home'); }
async function getFeaturesCatalog(source) { return apiFetch('/features/catalog' + (source ? '?source=' + source : '')); }
async function getGovernanceCatalog(source) { return apiFetch('/governance/catalog' + (source ? '?source=' + source : '')); }
async function getHealthStatus() { return apiFetch('/health'); }

function resolveHomeAuthoritativeTradeDate(payload) {
  if (!payload) return null;
  if (payload.trade_date) return payload.trade_date;
  if (payload.public_performance && payload.public_performance.runtime_trade_date) return payload.public_performance.runtime_trade_date;
  return null;
}

function resolveHomeAnchorMismatchReason(authoritative, refs) {
  if (!authoritative || !refs) return null;
  var reasons = [];
  if (refs.market_state_date && refs.market_state_date !== authoritative) reasons.push('market_state_date_mismatch');
  if (refs.reference_date && refs.reference_date !== authoritative) reasons.push('reference_date_mismatch');
  return reasons.length > 0 ? reasons.join(';') : null;
}

function getBridgeData(response, label) {
  if (!response) return null;
  if (response.success) return response.data || null;
  return null;
}

function getSettledBridgeData(settled, label) {
  if (!settled) return { ok: false, data: null, error: label + '读取失败 · no_settled' };
  if (settled.status === 'fulfilled') {
    var val = settled.value || {};
    if (val.success) return { ok: true, data: val.data || null, error: null };
    var errMsg = val.message || 'unknown';
    return { ok: false, data: null, error: label + '读取失败 · ' + errMsg };
  }
  var reason = (settled.reason && settled.reason.message) || 'rejected';
  return { ok: false, data: null, error: label + '读取失败 · ' + reason };
}

// ApiBridge facade for template consumers
if (typeof window !== 'undefined') {
  window.ApiBridge = {
    apiFetch: apiFetch,
    getReports: getReports,
    getReport: getReport,
    postLogin: postLogin,
    postRegister: postRegister,
    getAuthMe: getAuthMe,
    getMarketState: getMarketState,
    getHotStocks: getHotStocks,
    getPoolStocks: getPoolStocks,
    getHomePayload: getHomePayload,
    getFeaturesCatalog: getFeaturesCatalog,
    getGovernanceCatalog: getGovernanceCatalog,
    getHealthStatus: getHealthStatus,
    getAdminSystemStatus: getAdminSystemStatus,
    getAdminOverview: getAdminOverview,
    getAdminSchedulerStatus: getAdminSchedulerStatus,
    getBridgeData: getBridgeData,
    getSettledBridgeData: getSettledBridgeData,
    resolveHomeAuthoritativeTradeDate: resolveHomeAuthoritativeTradeDate,
    resolveHomeAnchorMismatchReason: resolveHomeAnchorMismatchReason,
  };
}
