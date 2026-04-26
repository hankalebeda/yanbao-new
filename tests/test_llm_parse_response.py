"""Tests for _parse_llm_response and _fix_triple_quotes in report_generation_ssot."""
import pytest
from app.services.report_generation_ssot import _parse_llm_response, _fix_triple_quotes


class TestFixTripleQuotes:
    def test_converts_triple_quotes_to_json_string(self):
        text = '{"key": """hello\nworld"""}'
        result = _fix_triple_quotes(text)
        assert '"""' not in result
        assert '"hello\\nworld"' in result

    def test_preserves_text_without_triple_quotes(self):
        text = '{"key": "normal string"}'
        assert _fix_triple_quotes(text) == text

    def test_handles_inner_double_quotes(self):
        text = '{"key": """say "hi" please"""}'
        result = _fix_triple_quotes(text)
        assert '"""' not in result
        assert '\\"hi\\"' in result


class TestParseLLMResponse:
    def test_standard_json_in_code_block(self):
        raw = '```json\n{"recommendation": "BUY", "confidence": 0.72, "conclusion_text": "stock is great", "reasoning_chain_md": "some reasoning"}\n```'
        result = _parse_llm_response(raw)
        assert result is not None
        assert result["recommendation"] == "BUY"
        assert result["confidence"] == 0.72

    def test_triple_quoted_reasoning(self):
        raw = '```json\n{\n  "recommendation": "HOLD",\n  "confidence": 0.65,\n  "conclusion_text": "neutral outlook for this stock",\n  "reasoning_chain_md": """\n- **point1**: data\n- **point2**: more data\n"""\n}\n```'
        result = _parse_llm_response(raw)
        assert result is not None
        assert result["recommendation"] == "HOLD"
        assert result["confidence"] == 0.65
        assert "point1" in result["reasoning_chain_md"]

    def test_json_without_code_block(self):
        raw = '{"recommendation": "SELL", "confidence": 0.55, "conclusion_text": "bearish signal", "reasoning_chain_md": "chain"}'
        result = _parse_llm_response(raw)
        assert result is not None
        assert result["recommendation"] == "SELL"

    def test_regex_fallback_extraction(self):
        """When JSON is malformed but key fields are extractable via regex."""
        raw = 'Here is my analysis: {"recommendation": "BUY", "confidence": 0.78, "conclusion_text": "good entry point for this stock", "reasoning_chain_md": badly_formatted}'
        result = _parse_llm_response(raw)
        assert result is not None
        assert result["recommendation"] == "BUY"
        assert result["confidence"] == 0.78

    def test_returns_none_for_garbage(self):
        result = _parse_llm_response("This is not JSON at all")
        assert result is None

    def test_returns_none_for_empty(self):
        result = _parse_llm_response("")
        assert result is None

    def test_multiline_conclusion_in_json(self):
        raw = '```json\n{"recommendation": "HOLD", "confidence": 0.60, "conclusion_text": "line1\\nline2\\nline3 with data", "reasoning_chain_md": "step1\\nstep2"}\n```'
        result = _parse_llm_response(raw)
        assert result is not None
        assert result["recommendation"] == "HOLD"
