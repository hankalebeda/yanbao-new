from scripts import benchmark_api


def test_benchmark_api_bypasses_env_proxies_for_loopback_hosts():
    for base_url in ("http://localhost:8010", "http://127.0.0.1:8010", "http://[::1]:8010"):
        session = benchmark_api.build_session(base_url)
        try:
            assert session.trust_env is False
        finally:
            session.close()


def test_benchmark_api_keeps_env_proxies_for_non_loopback_hosts():
    session = benchmark_api.build_session("https://api.example.com")
    try:
        assert session.trust_env is True
    finally:
        session.close()
