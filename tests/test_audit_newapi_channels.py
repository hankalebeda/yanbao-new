from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


def _load_module(module_name: str, relative_path: str):
    module_path = Path(__file__).resolve().parents[1] / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sync_newapi_channels = _load_module('sync_newapi_channels_for_audit_tests', 'ai-api/codex/sync_newapi_channels.py')
sys.modules['sync_newapi_channels'] = sync_newapi_channels
audit_newapi_channels = _load_module('audit_newapi_channels_under_test', 'ai-api/codex/audit_newapi_channels.py')


def test_determine_test_model_prefers_existing_test_model():
    channel = {'test_model': 'gpt-5.4', 'models': 'gpt-5.2'}
    assert audit_newapi_channels.determine_test_model(channel) == 'gpt-5.4'


def test_determine_test_model_falls_back_from_models_and_mapping():
    channel = {
        'test_model': '',
        'models': 'gpt-5.3-codex,gpt-5.2',
        'model_mapping': '{"gpt-5.4": "gpt-5.3-codex"}',
    }
    assert audit_newapi_channels.determine_test_model(channel) == 'gpt-5.4'


def test_prioritize_gpt_models_keeps_only_gpt_family_and_prefers_requested_model():
    models = ['claude-sonnet-4', 'gpt-5.2', 'gpt-5.4', 'gpt-5.3-codex', 'kimi-k2.5']
    ordered = audit_newapi_channels.prioritize_gpt_models(models, preferred='gpt-5.3-codex')
    assert ordered == ['gpt-5.3-codex', 'gpt-5.4', 'gpt-5.2']


def test_classify_channel_marks_disabled_supplier_usable_as_candidate_reactivate():
    classification, action = audit_newapi_channels.classify_channel(
        is_active=False,
        channel_test_ok=False,
        upstream_probe_ok=True,
        risk_types=[],
        sibling_conflict=False,
        test_model_used='gpt-5.4',
        model_list_ok=True,
        gpt_related_models=['gpt-5.4', 'gpt-5.3-codex'],
    )
    assert classification == 'usable_disabled'
    assert action == 'candidate_reactivate'


def test_classify_channel_marks_active_drift_when_active_channel_fails_probe():
    classification, action = audit_newapi_channels.classify_channel(
        is_active=True,
        channel_test_ok=True,
        upstream_probe_ok=False,
        risk_types=['503 / service temporarily unavailable'],
        sibling_conflict=False,
        test_model_used='gpt-5.4',
    )
    assert classification == 'active_drift'
    assert action == 'disable_keep'


def test_run_upstream_probe_fetches_models_first_and_only_uses_gpt_candidates(monkeypatch):
    seen = {}

    def fake_fetch_provider_models(*, base_url, api_key, timeout, proxy_url=None):
        seen['proxy_url'] = proxy_url
        return True, ['claude-sonnet-4', 'gpt-5.3-codex', 'gpt-5.4'], '', 'proxy'

    def fake_probe(source, *, candidate_models, proxy_url=None, **kwargs):
        seen['candidate_models'] = tuple(candidate_models)
        seen['probe_proxy_url'] = proxy_url
        return True, '', candidate_models[0], 'proxy'

    monkeypatch.setattr(audit_newapi_channels, 'fetch_provider_models', fake_fetch_provider_models)
    monkeypatch.setattr(audit_newapi_channels, 'probe_upstream_responses_audit', fake_probe)

    channel = {
        'id': 99,
        'base_url': 'https://example.com',
        'test_model': 'gpt-5.4',
        'model_mapping': '{"gpt-5.4": "gpt-5.3-codex"}',
        'name': 'example',
        'status': 2,
    }
    result = audit_newapi_channels.run_upstream_probe(
        channel,
        key_map={99: 'stub'},
        probe_timeout=1,
        probe_attempts=1,
        required_successes=1,
        proxy_url='http://127.0.0.1:10808',
    )
    assert result['upstream_probe_ok'] is True
    assert result['model_list_ok'] is True
    assert result['gpt_related_models'] == ['gpt-5.4', 'gpt-5.3-codex']
    assert seen['candidate_models'] == ('gpt-5.4', 'gpt-5.3-codex')
    assert seen['proxy_url'] == 'http://127.0.0.1:10808'
    assert seen['probe_proxy_url'] == 'http://127.0.0.1:10808'


def test_summarize_results_distinguishes_strict_and_supplier_usable_states():
    results = [
        audit_newapi_channels.ChannelAuditResult(
            channel_id=1,
            name='alpha',
            base_url='https://alpha.example',
            current_status=1,
            current_weight=100,
            current_priority=30,
            models='gpt-5.4',
            test_model_used='gpt-5.4',
            channel_test_ok=True,
            channel_test_latency_s=1.2,
            channel_test_message='',
            upstream_probe_ok=False,
            upstream_probe_model=None,
            upstream_probe_latency_s=2.1,
            upstream_probe_message='service temporarily unavailable',
            classification='active_drift',
            recommended_action='disable_keep',
            sibling_group='alpha.example',
            sibling_count=1,
            risk_types=['503 / service temporarily unavailable'],
            current_group='default',
            current_tag=None,
            current_remark=None,
            model_list_ok=True,
            provider_models=['gpt-5.4'],
            gpt_related_models=['gpt-5.4'],
            supplier_usable=False,
        ),
        audit_newapi_channels.ChannelAuditResult(
            channel_id=2,
            name='beta',
            base_url='https://beta.example',
            current_status=2,
            current_weight=0,
            current_priority=10,
            models='gpt-5.4',
            test_model_used='gpt-5.4',
            channel_test_ok=False,
            channel_test_latency_s=1.0,
            channel_test_message='',
            upstream_probe_ok=True,
            upstream_probe_model='gpt-5.4',
            upstream_probe_latency_s=1.1,
            upstream_probe_message='',
            classification='usable_disabled',
            recommended_action='candidate_reactivate',
            sibling_group='beta.example',
            sibling_count=1,
            risk_types=[],
            current_group='default',
            current_tag=None,
            current_remark=None,
            model_list_ok=True,
            provider_models=['gpt-5.4', 'gpt-5.3-codex'],
            gpt_related_models=['gpt-5.4', 'gpt-5.3-codex'],
            supplier_usable=True,
            codex_usable=True,
        ),
        audit_newapi_channels.ChannelAuditResult(
            channel_id=3,
            name='gamma',
            base_url='https://gamma.example',
            current_status=2,
            current_weight=0,
            current_priority=10,
            models='gpt-5.2',
            test_model_used='gpt-5.2',
            channel_test_ok=False,
            channel_test_latency_s=None,
            channel_test_message='model_not_found',
            upstream_probe_ok=False,
            upstream_probe_model=None,
            upstream_probe_latency_s=None,
            upstream_probe_message='model_not_found',
            classification='broken',
            recommended_action='retire_candidate_but_do_not_delete',
            sibling_group='gamma.example',
            sibling_count=1,
            risk_types=['model_not_found'],
            current_group='default',
            current_tag=None,
            current_remark=None,
            model_list_ok=False,
            provider_models=[],
            gpt_related_models=[],
            supplier_usable=False,
        ),
    ]
    summary = audit_newapi_channels.summarize_results(results)
    assert summary['status_summary']['active_count'] == 1
    assert summary['status_summary']['disabled_count'] == 2
    assert summary['status_summary']['active_fail_count'] == 1
    assert summary['status_summary']['disabled_recoverable_count'] == 1
    assert summary['status_summary']['active_usable_count'] == 0
    assert summary['model_summary']['supplier_model_list_ok_count'] == 2
    assert summary['model_summary']['supplier_usable_count'] == 1
    assert summary['risk_summary'] == {
        '503 / service temporarily unavailable': 1,
        'model_not_found': 1,
    }
    assert summary['next_steps']['active_unhealthy'] == ['alpha']
    assert summary['next_steps']['disabled_recoverable'] == ['beta']
    assert summary['next_steps']['supplier_usable'] == ['beta']
