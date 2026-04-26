import httpx
r = httpx.post(
    'http://192.168.232.141:8317/v1/chat/completions',
    headers={'Authorization': 'Bearer cpa-api-yjEgDXE2lgi4mFjH', 'Content-Type': 'application/json'},
    json={'model': 'gpt-5.4', 'messages': [{'role': 'user', 'content': 'reply with short JSON {"ok":true}'}], 'max_tokens': 40},
    timeout=60,
    trust_env=False,
)
print('HTTP', r.status_code)
print(r.text[:800])

r2 = httpx.get('http://192.168.232.141:8317/v1/models', headers={'Authorization': 'Bearer cpa-api-yjEgDXE2lgi4mFjH'}, timeout=10, trust_env=False)
print('MODELS HTTP', r2.status_code)
print(r2.text[:2000])
