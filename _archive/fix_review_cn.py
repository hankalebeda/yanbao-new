"""Fix REVIEW_CN line using line-level replacement."""
with open("app/web/templates/admin.html", encoding="utf-8") as f:
    lines = f.readlines()

new_review = "    var REVIEW_CN = {'PENDING_REVIEW':'待审核','APPROVED':'已通过','REJECTED':'已拒绝','AUTO_APPROVED':'自动通过'};\n"

count = 0
for i, line in enumerate(lines):
    if "var REVIEW_CN" in line:
        lines[i] = new_review
        count += 1
        print(f"Fixed line {i+1}")

print(f"Total: {count}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.writelines(lines)
print("Saved.")
