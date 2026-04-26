import json
p = r'd:\yanbao-new\_archive\retest_core_20260416.json'
with open(p, 'r', encoding='utf-16') as f:
    data = json.load(f)
print('report_id_sample', data.get('report_id_sample'))
for c in data.get('checks', []):
    print(c.get('name'), c.get('status'))
