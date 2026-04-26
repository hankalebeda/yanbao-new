import urllib.request, json, os, re

providers_dir = r'D:\yanbao\ai-api\codex'
skip_prefixes = ('portable_', 'newapi', '__pycache__')
dirs = [d for d in os.listdir(providers_dir) 
        if not any(d.startswith(p) for p in skip_prefixes) 
        and os.path.isdir(os.path.join(providers_dir, d))]

for d in sorted(dirs):
    auth_f = os.path.join(providers_dir, d, 'auth.json')
    cfg_f = os.path.join(providers_dir, d, 'config.toml')
    if not (os.path.exists(auth_f) and os.path.exists(cfg_f)):
        continue
    try:
        with open(auth_f) as f:
            auth = json.load(f)
        key = auth.get('OPENAI_API_KEY', '')
        cfg = open(cfg_f).read()
        m = re.search(r'base_url\s*=\s*"([^"]+)"', cfg)
        if not m:
            continue
        base_url = m.group(1)
        req = urllib.request.Request(f'{base_url}/models', 
                                      headers={'Authorization': f'Bearer {key}'})
        with urllib.request.urlopen(req, timeout=6) as r:
            data = json.loads(r.read())
            models = [x['id'] for x in data.get('data', [])]
            has54 = 'gpt-5.4' in models
            has53c = 'gpt-5.3-codex' in models
            print(f'OK {d}: gpt-5.4={has54} gpt-5.3-codex={has53c} total={len(models)} sample={models[:3]}')
    except Exception as e:
        print(f'FAIL {d}: {type(e).__name__} {str(e)[:80]}')
