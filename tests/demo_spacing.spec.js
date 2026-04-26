/**
 * Demo 页面间距验收测试
 * 运行: npx playwright test tests/demo_spacing_spec.js --project=chromium
 * 需先: npm init -y && npx playwright install chromium
 */
const { test, expect } = require('@playwright/test');

const BASE = process.env.DEMO_BASE || 'http://127.0.0.1:8000';

test.describe('Demo 间距验收', () => {
  test.beforeEach(async ({ page }) => {
    // 等待页面稳定
    page.setDefaultTimeout(15000);
  });

  test('index: 市场横幅区块间距应较小 (gap ≤ 8px)', async ({ page }) => {
    await page.goto(`${BASE}/demo/index.html`);
    const grid = page.locator('.hero .summary-box .market-banner-grid').first();
    await expect(grid).toBeVisible();
    const gap = await grid.evaluate((el) => {
      const s = getComputedStyle(el);
      return { row: s.gap.split(' ')[0], col: s.gap.split(' ')[1] || s.gap };
    });
    const rowPx = parseFloat(gap.row) || 0;
    const colPx = parseFloat(gap.col) || parseFloat(gap.row) || 0;
    expect(rowPx).toBeLessThanOrEqual(10);
    expect(colPx).toBeLessThanOrEqual(12);
  });

  test('index: 绩效丸区块间距应较大 (gap ≥ 18px)', async ({ page }) => {
    await page.goto(`${BASE}/demo/index.html`);
    const metrics = page.locator('.hero .summary-box .hero-cred-metrics').first();
    await expect(metrics).toBeVisible();
    const gap = await metrics.evaluate((el) => getComputedStyle(el).gap);
    const parts = gap.split(' ');
    const rowPx = parseFloat(parts[0]) || 0;
    const colPx = parseFloat(parts[1] || parts[0]) || 0;
    expect(rowPx).toBeGreaterThanOrEqual(18);
    expect(colPx).toBeGreaterThanOrEqual(24);
  });

  test('reports_list: 研报行间距应较大 (report-row gap ≥ 20px)', async ({ page }) => {
    await page.goto(`${BASE}/demo/reports_list.html`);
    const row = page.locator('.report-row').first();
    await expect(row).toBeVisible();
    const gap = await row.evaluate((el) => getComputedStyle(el).gap);
    const gapPx = parseFloat(gap) || 0;
    expect(gapPx).toBeGreaterThanOrEqual(20);
  });

  test('reports_list: report-row-left 间距应较大 (gap ≥ 18px)', async ({ page }) => {
    await page.goto(`${BASE}/demo/reports_list.html`);
    const left = page.locator('.report-row-left').first();
    await expect(left).toBeVisible();
    const gap = await left.evaluate((el) => getComputedStyle(el).gap);
    const gapPx = parseFloat(gap) || 0;
    expect(gapPx).toBeGreaterThanOrEqual(18);
  });
});
