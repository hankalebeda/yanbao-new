"""Hard visual gates for official HTML pages.

These tests freeze structural anchors and block external font regressions.
"""

from __future__ import annotations

import re
from pathlib import Path


def _get_page(client, path: str) -> str:
    response = client.get(path)
    assert response.status_code == 200, f"{path} returned {response.status_code}"
    return response.text


def test_gate_visual_all_pages_share_nav(client):
    pages = ["/", "/login", "/register", "/forgot-password", "/reports", "/subscribe"]
    for path in pages:
        html = _get_page(client, path)
        assert "nav" in html.lower() or "navbar" in html.lower(), f"{path} missing nav structure"


def test_gate_visual_css_tokens_defined(client):
    response = client.get("/web/demo.css")
    if response.status_code != 200:
        response = client.get("/static/demo.css")
    assert response.status_code == 200, "demo.css must be reachable for visual gate"

    css = response.text
    required_tokens = ["--bg-dark", "--radius-md", "--radius-card", "--radius-inner"]
    for token in required_tokens:
        assert token in css, f"demo.css missing token {token}"


def test_gate_visual_auth_pages_consistent_structure(client):
    login_html = _get_page(client, "/login")
    register_html = _get_page(client, "/register")
    forgot_html = _get_page(client, "/forgot-password")

    for name, html in [("login", login_html), ("register", register_html), ("forgot-password", forgot_html)]:
        assert "auth-wrap" in html, f"/{name} missing auth-wrap container"


def test_gate_visual_report_view_no_undefined_tokens(client):
    response = client.get("/web/demo.css")
    if response.status_code != 200:
        response = client.get("/static/demo.css")
    assert response.status_code == 200, "demo.css must be reachable for report view gate"

    defined_tokens = set(re.findall(r"--([\w-]+)\s*:", response.text))
    report_view = Path("app/web/templates/report_view.html")
    assert report_view.exists(), "report_view.html must exist for report visual gate"

    html = report_view.read_text(encoding="utf-8")
    referenced = set(re.findall(r"var\(--([\w-]+)\)", html))
    undefined = referenced - defined_tokens
    undefined -= {"bs-body-color", "bs-body-bg"}
    assert not undefined, f"report_view.html references undefined CSS variables: {undefined}"


def test_gate_visual_official_pages_have_no_external_font_dependency():
    forbidden_tokens = ("fonts.googleapis.com", "fonts.gstatic.com")
    targets = list(Path("app/web").rglob("*.css")) + list(Path("app/web/templates").rglob("*.html"))
    violations = []
    for path in targets:
        text = path.read_text(encoding="utf-8")
        if any(token in text for token in forbidden_tokens):
            violations.append(str(path))
    assert not violations, f"official pages still depend on external font CDNs: {violations}"
