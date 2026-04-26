"""Debug the FR01 pool refresh 500 error."""
import os, sys
os.environ.setdefault("MOCK_LLM", "true")
os.environ.setdefault("ENABLE_SCHEDULER", "false")
os.environ.setdefault("SETTLEMENT_INLINE_EXECUTION", "true")
sys.path.insert(0, ".")

from datetime import date, timedelta
from uuid import uuid4
import tempfile

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models import Base
from app.main import app
from app.core.db import get_db
from fastapi.testclient import TestClient

# Create isolated DB
tmpdir = tempfile.mkdtemp()
db_url = f"sqlite:///{tmpdir}/test.db"
engine = create_engine(db_url, connect_args={"check_same_thread": False})
Base.metadata.create_all(engine)

session = Session(engine)

def override_db():
    try:
        yield session
    finally:
        pass

app.dependency_overrides[get_db] = override_db
client = TestClient(app, raise_server_exceptions=False)

# Seed data
from tests.test_fr01_pool_refresh import _seed_universe

trade_date = "2026-03-09"
_seed_universe(session, trade_date=trade_date)

# Create user + login
from app.models import User
import hashlib
user = User(
    user_id=str(uuid4()),
    username="admin1",
    email="admin1@test.com",
    hashed_password="$2b$12$LJ3m4dCxJ2pT4eN8xT5u5.v7u8EWFG4JMwN1pOOyVvSRB1LZ2tS9i",
    role="admin",
    email_verified=True,
    status="ACTIVE",
)
session.add(user)
session.commit()

login = client.post("/api/v1/auth/login", json={"email": "admin1@test.com", "password": "Password123"})
print(f"Login: {login.status_code}")
if login.status_code != 200:
    print(f"Login body: {login.text[:300]}")
    # Try direct registration
    reg = client.post("/api/v1/auth/register", json={
        "username": "admin2", "email": "admin2@test.com", "password": "Password123!"
    })
    print(f"Register: {reg.status_code} {reg.text[:300]}")
    sys.exit(1)

token = login.json()["data"]["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# Call pool refresh
response = client.post(
    "/api/v1/admin/pool/refresh",
    headers=headers,
    json={"trade_date": trade_date, "force_rebuild": False},
)
print(f"Status: {response.status_code}")
print(f"Body: {response.text[:2000]}")
