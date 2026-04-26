"""Fix all remaining broken JS string concatenations."""
with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

# Fix: 总览接口加载失败！ + text + '  ->  总览接口加载失败！' + text + '
# The closing quote after 失败！ was eaten
t = t.replace(
    "总览接口加载失败！ + text + '",
    "总览接口加载失败！' + text + '"
)
# Wait, that changes the meaning. Let me look at what it should be:
# original: '<div ...>总览接口加载失败！' + text + '</div>'
# current:  '<div ...>总览接口加载失败！ + text + '</div>'
# The fix: add ' after 失败！
# But I need to be careful about what I'm replacing
# Revert my change and do it properly

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

# The line is:
# innerHTML = '<div style="color:#991b1b">总览接口加载失败！ + text + '</div>';
# Should be:
# innerHTML = '<div style="color:#991b1b">总览接口加载失败！' + text + '</div>';
# 
# So I need to insert ' between 失败！ and  +

t = t.replace(
    '总览接口加载失败！ + text',
    "总览接口加载失败！' + text"
)
print("Fixed source-dates innerHTML")

# Now let me also check for any similar broken concatenation patterns
import re
lines = t.split("\n")
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    # Pattern: Chinese + space + text/variable (missing closing quote before concatenation)
    if re.search(r"[\u4e00-\u9fff\uff01\uff1f！？] \+ ", line):
        # Check if there's a proper string delimiter before the +
        idx = line.find(" + ")
        if idx > 0:
            before = line[idx-1]
            if before not in "'\")" and before != " ":
                print(f"  Potential broken concat line {i+1}: {line.strip()[:80]}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(t)
print("Saved.")
