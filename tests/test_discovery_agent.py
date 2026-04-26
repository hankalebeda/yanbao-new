from __future__ import annotations

from automation.agents.discovery import DiscoveryAgent


class _DummyProbe:
    def __init__(self, name: str):
        self.name = name


def test_probe_timeout_seconds_extends_slow_probes():
    assert DiscoveryAgent._probe_timeout_seconds(_DummyProbe("doc25_angle")) == 150.0
    assert DiscoveryAgent._probe_timeout_seconds(_DummyProbe("catalog_drift")) == 90.0
    assert DiscoveryAgent._probe_timeout_seconds(_DummyProbe("mesh_audit")) == 120.0
    assert DiscoveryAgent._probe_timeout_seconds(_DummyProbe("audit")) == 60.0