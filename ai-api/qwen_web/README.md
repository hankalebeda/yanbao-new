# Qwen Web - AI API Module

Use Playwright + local Chrome session to call Qwen web chat at `https://chat.qwen.ai/`.

## Endpoints

- `POST /api/v1/qwen/analyze`
- `POST /api/v1/qwen/analyze/batch`
- `DELETE /api/v1/qwen/session`

## Run Test

```bash
python ai-api/qwen_web/test.py
```

Optional: set `qwen_proxy_url` in `.env` when direct access to `assets.alicdn.com` is blocked.

Default behavior: `qwen_force_no_proxy=true`, which forces Chrome direct connection for Qwen and bypasses local/system proxy (for example `127.0.0.1:10808`).

## CDP Attach Mode

You can also attach to an already running Chrome session (recommended when you have an active logged-in Qwen tab):

1. Start Chrome with remote debugging (example):
   `chrome.exe --remote-debugging-port=9222 --user-data-dir=C:\chrome-cdp-profile`
2. Log in at `https://chat.qwen.ai/` in that Chrome window.
3. Set `.env`: `QWEN_CDP_URL="http://127.0.0.1:9222"`
4. Run the API/test normally.
