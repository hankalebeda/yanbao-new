$root = Split-Path -Parent $MyInvocation.MyCommand.Path
& (Join-Path $root "run_codex_provider.ps1") -ProviderDir "sub.jlypx.de" @args
