from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from app.core.enums import (
    CAPITAL_TIER_VALUES,
    SHARED_ENUMS,
    CapitalTier,
    EnumContractConflict,
    build_enum_registry,
)


def test_shared_enum_contract_exports_nine_enums():
    registry = build_enum_registry()
    assert len(SHARED_ENUMS) == 9
    assert tuple(registry["CapitalTier"]) == ("10k", "100k", "500k")
    assert tuple(CAPITAL_TIER_VALUES) == ("10k", "100k", "500k")


def test_duplicate_enum_registration_fails_closed():
    with pytest.raises(EnumContractConflict):
        build_enum_registry((CapitalTier, CapitalTier))


def test_capital_tier_enum_is_pydantic_validated():
    class Payload(BaseModel):
        capital_tier: CapitalTier

    assert Payload(capital_tier="100k").capital_tier is CapitalTier.HUNDRED_K
    with pytest.raises(ValidationError):
        Payload(capital_tier="10w")


def test_features_endpoint_returns_public_capital_tiers(client):
    response = client.get("/api/v1/features")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["capital_tiers"] == ["10k", "100k", "500k"]
    assert body["data"]["default_capital_tier"] == "100k"