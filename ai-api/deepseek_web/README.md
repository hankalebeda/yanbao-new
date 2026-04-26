# DeepSeek Web - AI API Module

Use Playwright + local Chrome session to call DeepSeek web chat at `https://chat.deepseek.com/`.

## Endpoints

- `POST /api/v1/deepseek/analyze`
- `POST /api/v1/deepseek/analyze/batch`
- `DELETE /api/v1/deepseek/session`

## Notes

- This module uses Google Chrome.
- Proxy is disabled by default (`deepseek_chrome_force_no_proxy=true`).

## Run Test

```bash
python ai-api/deepseek_web/test.py
```

