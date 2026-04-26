import re

for filepath in ['app/services/capital_flow.py', 'app/services/company_data.py']:
    content = open(filepath, encoding='utf-8').read()
    count_before = content.count('trust_env=False')
    
    # Add trust_env=False to all httpx.AsyncClient(... ) calls that don't already have it
    def add_trust_env(match):
        call = match.group(0)
        if 'trust_env=False' in call:
            return call
        # Remove the closing ) and add trust_env=False
        return call[:-1] + ', trust_env=False)'
    
    new_content = re.sub(r'httpx\.AsyncClient\([^)]+\)', add_trust_env, content)
    count_after = new_content.count('trust_env=False')
    
    open(filepath, 'w', encoding='utf-8').write(new_content)
    print(f'{filepath}: {count_before} -> {count_after} trust_env=False occurrences')
