"""Debug: show exact bytes of garbled strings in admin.html."""

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    lines = f.readlines()

# Line 616
line = lines[615]  # 0-indexed
print("Line 616 STATUS_CN:")
# Find the RUNNING part
idx = line.find("RUNNING")
if idx >= 0:
    segment = line[idx:idx+40]
    print(f"  text: {repr(segment)}")
    for ch in segment:
        print(f"    {ch!r} = U+{ord(ch):04X}")

print()
# Line 630
line = lines[629]
print("Line 630 PUBLIC_STATUS_CN:")
idx = line.find("UNKNOWN")
if idx >= 0:
    segment = line[idx:idx+30]
    print(f"  text: {repr(segment)}")
    for ch in segment:
        print(f"    {ch!r} = U+{ord(ch):04X}")

print()
# Line 620: ROLE_CN user entry
line = lines[619]
print("Line 620 ROLE_CN:")
idx = line.find("user")
if idx >= 0:
    segment = line[idx:idx+30]
    print(f"  text: {repr(segment)}")
    for ch in segment:
        print(f"    {ch!r} = U+{ord(ch):04X}")

print()
# Line 618: TRIGGER_CN startup
line = lines[617]
print("Line 618 TRIGGER_CN:")
idx = line.find("startup")
if idx >= 0:
    segment = line[idx:idx+30]
    print(f"  text: {repr(segment)}")
    for ch in segment:
        print(f"    {ch!r} = U+{ord(ch):04X}")

print()
# Line 636: statusCn fallback with 鏈煡
line = lines[635]
print("Line 636 statusCn:")
print(f"  text: {repr(line.strip()[:120])}")
