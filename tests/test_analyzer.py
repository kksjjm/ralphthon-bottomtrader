import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from src.core.analyzer import (
    _build_user_prompt,
    _parse_response,
    analyze_drops,
    analyze_macro_crash,
    analyze_single_drop,
)


class TestParseResponse:
    def test_valid_json(self):
        text = json.dumps({"cause": "test", "recovery_likelihood": "높음", "sources": ["url"]})
        result = _parse_response(text)
        assert result["cause"] == "test"
        assert result["recovery_likelihood"] == "높음"

    def test_legacy_confidence_mapped(self):
        text = json.dumps({"cause": "test", "confidence": "HIGH", "sources": []})
        result = _parse_response(text)
        assert result["recovery_likelihood"] == "높음"

    def test_json_with_surrounding_text(self):
        text = 'Here is the response: {"cause": "drop", "recovery_likelihood": "낮음", "sources": []} end'
        result = _parse_response(text)
        assert result["cause"] == "drop"
        assert result["recovery_likelihood"] == "낮음"

    def test_invalid_json_fallback(self):
        text = "This is not JSON at all"
        result = _parse_response(text)
        assert result["cause"] == text
        assert result["recovery_likelihood"] == "보통"
        assert result["sources"] == []


class TestBuildUserPrompt:
    def test_with_news(self):
        news = [
            {"title": "NVDA drops 10%", "link": "https://ex.com", "source": "Reuters"},
        ]
        prompt = _build_user_prompt("NVDA", -10.0, news)
        assert "NVDA" in prompt
        assert "-10.0%" in prompt
        assert "NVDA drops 10%" in prompt
        # URLs are no longer sent to the LLM
        assert "https://ex.com" not in prompt

    def test_without_news(self):
        prompt = _build_user_prompt("TSLA", -5.5, [])
        assert "TSLA" in prompt
        assert "뉴스: 없음" in prompt

    def test_with_avg_drop(self):
        prompt = _build_user_prompt("AAPL", -3.0, [], avg_drop_pct=-12.5)
        assert "이동평균 대비" in prompt
        assert "-12.5%" in prompt


@pytest.mark.asyncio
async def test_analyze_single_drop_success():
    mock_client = AsyncMock()
    mock_response = MagicMock()
    mock_response.choices = [
        MagicMock(message=MagicMock(content=json.dumps({
            "cause": "EU 조사",
            "recovery_likelihood": "높음",
            "sources": ["EU probe"],
        })))
    ]
    mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

    result = await analyze_single_drop(
        mock_client, "NVDA", -10.0, [{"title": "EU probe", "source": "Reuters", "link": "https://reuters.com"}]
    )
    assert result["ticker"] == "NVDA"
    assert result["recovery_likelihood"] == "높음"
    assert "EU" in result["cause"]


@pytest.mark.asyncio
async def test_analyze_single_drop_api_failure():
    mock_client = AsyncMock()
    mock_client.chat.completions.create = AsyncMock(side_effect=Exception("API down"))

    result = await analyze_single_drop(
        mock_client, "NVDA", -10.0, [{"title": "News", "link": "", "source": ""}]
    )
    assert result["ticker"] == "NVDA"
    assert result["recovery_likelihood"] == "보통"
    assert "실패" in result["cause"]


@pytest.mark.asyncio
async def test_analyze_drops_parallel():
    with patch("src.core.analyzer.AsyncOpenAI") as MockOpenAI:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content=json.dumps({
                "cause": "테스트 원인",
                "recovery_likelihood": "보통",
                "sources": [],
            })))
        ]
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)
        MockOpenAI.return_value = mock_client

        drops = [
            {"ticker": "AAPL", "daily_drop_pct": -5.5, "news": []},
            {"ticker": "NVDA", "daily_drop_pct": -10.0, "news": [{"title": "news", "source": "", "link": ""}]},
        ]
        results = await analyze_drops(drops)
        assert len(results) == 2
        assert {r["ticker"] for r in results} == {"AAPL", "NVDA"}


@pytest.mark.asyncio
async def test_analyze_macro_crash():
    with patch("src.core.analyzer.AsyncOpenAI") as MockOpenAI:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content=json.dumps({
                "cause": "Fed 금리 인상 발표",
                "recovery_likelihood": "낮음",
                "sources": [],
            })))
        ]
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)
        MockOpenAI.return_value = mock_client

        result = await analyze_macro_crash(50, [{"title": "Fed hikes", "source": "Reuters"}])
        assert result["recovery_likelihood"] == "낮음"
        assert "Fed" in result["cause"]
