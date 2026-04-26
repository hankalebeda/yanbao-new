# Codex local provider setup

`ccswitch://v1/import?...` is a provider import deep link. It is not a shell command by itself.

This directory now supports importing that link into a local provider folder without replacing the user's global `~/.codex` files.

## Import a provider

```powershell
python ai-api/codex/import_ccswitch_provider.py "ccswitch://v1/import?resource=provider&app=codex&name=My+Codex&endpoint=https%3A%2F%2Fapi.example.com%2Fv1&apiKey=sk-xxxx&model=gpt-5.4&homepage=https%3A%2F%2Fapi.example.com&enabled=true"
```

That creates:

- `ai-api/codex/<host>/config.toml`
- `ai-api/codex/<host>/provider.json`
- `ai-api/codex/<host>/auth.json`

`auth.json` is local-only and is ignored by git.

## Run Codex with a provider

Interactive:

```powershell
powershell -ExecutionPolicy Bypass -File ai-api/codex/run_codex_provider.ps1 -ProviderDir ai.qaq.al
```

One-shot:

```powershell
powershell -ExecutionPolicy Bypass -File ai-api/codex/run_codex_provider.ps1 -ProviderDir ai.qaq.al exec --skip-git-repo-check "Reply with exactly OK"
```

Shortcut for `ai.qaq.al`:

```powershell
powershell -ExecutionPolicy Bypass -File ai-api/codex/run_codex_aiqaqal.ps1
```

Shortcut for `sub.jlypx.de`:

```powershell
powershell -ExecutionPolicy Bypass -File ai-api/codex/run_codex_subjlypx.ps1
```

Shortcut for `api.925214.xyz`:

```powershell
powershell -ExecutionPolicy Bypass -File ai-api/codex/run_codex_925214.ps1
```

Shortcut for `infiniteai.cc`:

```powershell
powershell -ExecutionPolicy Bypass -File ai-api/codex/run_codex_infiniteai.ps1
```

The runner copies the selected provider's `config.toml` and `auth.json` into a provider-specific portable home under `ai-api/codex/portable_<provider>/`, then launches `codex` from there.

## Probe a provider

```powershell
python ai-api/codex/probe_provider_live.py api.925214.xyz
```

This checks provider discovery, `GET /models`, `POST /responses`, and a real `codex exec` smoke run.

Shortcut for `api.925214.xyz` probe:

```powershell
powershell -ExecutionPolicy Bypass -File probe-with-925214.ps1
```

## Context policy

- Mainline long-context providers use `gpt-5.4` with `model_context_window = 1000000` and `model_auto_compact_token_limit = 900000`.
- Compatibility fallback providers stay on `gpt-5.2` with `model_context_window = 400000` and `model_auto_compact_token_limit = 320000`.
- The importer now derives context settings from the selected model instead of hard-coding a single 1M window for every provider.

## Notes

- If the import link already contains `/v1`, it is preserved.
- If the endpoint omits `/v1`, the importer appends it.
- Because the key was exposed in plain text, rotate it if this link has been shared outside your local machine.
