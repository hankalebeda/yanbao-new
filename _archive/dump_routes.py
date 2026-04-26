"""Dump all registered routes from the FastAPI app."""
from app.main import app

routes = []
for route in app.routes:
    methods = getattr(route, "methods", set())
    path = getattr(route, "path", "")
    name = getattr(route, "name", "")
    if path and methods:
        routes.append((sorted(methods), path, name))

routes.sort(key=lambda x: x[1])
for m, p, n in routes:
    ms = " ".join(m)
    print(f"{ms:10s} {p:60s} {n}")
