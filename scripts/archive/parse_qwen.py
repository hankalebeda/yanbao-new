from bs4 import BeautifulSoup
with open("qwen_dump.html", "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f, "html.parser")
text = soup.get_text()
print("--- Body text ---")
print(text[:1000])
