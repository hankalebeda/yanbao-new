"""Fix remaining garbled text in admin.html."""
with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

# Fix 1: 运行时 → 运行日 (test expects 运行日 for runtime_trade_date)
t = t.replace("['运行时', sourceDates.runtime_trade_date]", "['运行日', sourceDates.runtime_trade_date]")
print("Fixed 运行时 → 运行日")

# Fix 2: garbled 涓?confidence鈮?.65 → 中 confidence≥0.65
t = t.replace("涓?confidence鈮?.65", "中 confidence≥0.65")
print("Fixed confidence garbled text")

# Fix 3: garbled HTML tag missing <
t = t.replace("鈮?.65。/div>", "≥0.65。</div>")  # fallback if previous didn't catch all
print("Fixed ≥0.65 variants")

# Fix 4: BUY 涓?confidence → BUY 中 confidence  
t = t.replace("BUY 涓?confidence", "BUY 中 confidence")
print("Fixed BUY garbled")

# Fix 5: garbled alignment warning 褰撳墠鍚勯〉闈㈡棩鏈熼敋鐐规湭瀹屽叏瀵归綈。
t = t.replace(
    "褰撳墠鍚勯〉闈㈡棩鏈熼敋鐐规湭瀹屽叏瀵归綈。/div>",
    "当前各项页面日期锚点未完全对齐。</div>"
)
print("Fixed alignment warning garbled text")

# Fix 6: HTML entity in non-HTML context
t = t.replace("confidence&ge;0.65", "confidence≥0.65")
print("Fixed &ge; entity")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(t)
print("Saved.")
