"""Fix remaining broken concat patterns."""
with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

# Fix: '⚠总览加载失败！ + ( ... || '未知错误')  ->  '⚠ 总览加载失败！' + (...
t = t.replace("'⚠总览加载失败！ + (", "'⚠ 总览加载失败！' + (")
print("Fixed errEl.textContent")

# Fix: alert('操作失败！ + (  ->  alert('操作失败！' + (
t = t.replace("alert('操作失败！ + (", "alert('操作失败！' + (")
print("Fixed alert")

# Also check for similar patterns
import re
lines = t.split("\n")
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    # Check for missing quote before + concatenation with Chinese
    matches = list(re.finditer(r"[\u4e00-\u9fff\uff01\uff1f] \+ ", line))
    for m in matches:
        before_char = line[m.start()]
        # Check if the char before + is not preceded by a closing quote
        pre = line[max(0, m.start()-1):m.start()+1]
        if pre[0] != "'":
            print(f"  Still broken line {i+1}: ...{line[max(0,m.start()-10):m.end()+10]}...")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(t)
print("Saved.")
