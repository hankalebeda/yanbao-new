module.exports = {
  testDir: 'tests',
  timeout: 15000,
  use: {
    baseURL: process.env.DEMO_BASE || 'http://127.0.0.1:8000',
    headless: true,
  },
  projects: [{ name: 'chromium', use: { browserName: 'chromium' } }],
};
