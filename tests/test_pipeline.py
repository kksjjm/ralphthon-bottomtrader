"""Integration tests for the pipeline."""
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import numpy as np
import pytest

from src.pipeline import _merge_drops, _format_alert_message, _format_macro_message, run_pipeline


class TestMergeDrops:
    def test_merge_no_overlap(self):
        daily = [{"ticker": "AAPL", "daily_drop_pct": -6.0, "close": 150.0}]
        avg = [{"ticker": "NVDA", "avg_drop_pct": -12.0, "close": 130.0}]
        result = _merge_drops(daily, avg)
        assert len(result) == 2
        tickers = {d["ticker"] for d in result}
        assert tickers == {"AAPL", "NVDA"}

    def test_merge_with_overlap(self):
        daily = [{"ticker": "NVDA", "daily_drop_pct": -8.0, "close": 135.0}]
        avg = [{"ticker": "NVDA", "avg_drop_pct": -15.0, "close": 135.0, "avg_price": 158.0}]
        result = _merge_drops(daily, avg)
        assert len(result) == 1
        assert result[0]["daily_drop_pct"] == -8.0
        assert result[0]["avg_drop_pct"] == -15.0

    def test_merge_avg_only_preserves_daily(self):
        # Avg-only tickers now carry the real daily change, not 0.
        avg = [{
            "ticker": "AAPL", "avg_drop_pct": -12.0, "close": 150.0,
            "avg_price": 170.0, "daily_drop_pct": -1.5,
        }]
        result = _merge_drops([], avg)
        assert len(result) == 1
        assert result[0]["daily_drop_pct"] == -1.5

    def test_merge_empty(self):
        assert _merge_drops([], []) == []


class TestFormatMessages:
    def test_format_alert_message(self):
        analysis = {
            "ticker": "NVDA",
            "cause": "EU 반독점 조사",
            "recovery_likelihood": "높음",
            "sources": ["뉴스 제목"],
        }
        drop = {"daily_drop_pct": -7.2, "avg_drop_pct": -12.3}
        msg = _format_alert_message(analysis, drop)
        assert "NVDA" in msg
        assert "-7.2%" in msg
        assert "EU 반독점" in msg
        assert "이동평균 복귀 가능성: 높음" in msg
        assert "/buy NVDA" in msg
        # URL/sources should no longer appear in the message
        assert "소스" not in msg
        assert "신뢰도" not in msg

    def test_format_alert_no_avg(self):
        analysis = {"ticker": "AAPL", "cause": "실적 미달",
                    "recovery_likelihood": "보통", "sources": []}
        drop = {"daily_drop_pct": -5.5}
        msg = _format_alert_message(analysis, drop)
        # "이동평균 복귀 가능성" is always shown, but "이동평균 대비" (the comparison line)
        # should not appear when avg_drop_pct is missing
        assert "이동평균 대비" not in msg
        assert "이동평균 복귀 가능성: 보통" in msg

    def test_format_alert_legacy_confidence(self):
        # Older alert rows may still carry English confidence values.
        analysis = {"ticker": "TSLA", "cause": "x", "confidence": "HIGH"}
        drop = {"daily_drop_pct": -6.0}
        msg = _format_alert_message(analysis, drop)
        assert "이동평균 복귀 가능성: 높음" in msg

    def test_format_macro_message(self):
        analysis = {"cause": "Fed 금리 인상", "recovery_likelihood": "낮음", "sources": []}
        msg = _format_macro_message(analysis, 50)
        assert "50개" in msg
        assert "Fed" in msg
        assert "회로 차단기" in msg
        assert "이동평균 복귀 가능성: 낮음" in msg
        assert "소스" not in msg


@pytest.mark.asyncio
@patch("src.pipeline.db")
@patch("src.pipeline.fetch_prices")
@patch("src.pipeline.load_tickers")
@patch("src.pipeline.Bot")
async def test_pipeline_no_drops(mock_bot_cls, mock_load, mock_fetch, mock_db):
    mock_load.return_value = ["AAPL"]
    mock_db.get_all_user_settings.return_value = []

    dates = pd.date_range(end=date.today(), periods=5, freq="B")
    data = pd.DataFrame({"Close": [100, 100, 100, 100, 100]}, index=dates)
    mock_fetch.return_value = data

    mock_bot = AsyncMock()
    mock_bot_cls.return_value = mock_bot

    await run_pipeline()
    # Should send either "no drops" or "market closed" message
    mock_bot.send_message.assert_called()


@pytest.mark.asyncio
@patch("src.pipeline.db")
@patch("src.pipeline.fetch_prices")
@patch("src.pipeline.load_tickers")
@patch("src.pipeline.Bot")
async def test_pipeline_fetch_failure(mock_bot_cls, mock_load, mock_fetch, mock_db):
    mock_load.return_value = ["AAPL"]
    mock_db.get_all_user_settings.return_value = []
    mock_fetch.side_effect = Exception("yfinance down")

    mock_bot = AsyncMock()
    mock_bot_cls.return_value = mock_bot

    await run_pipeline()
    call_text = mock_bot.send_message.call_args[1]["text"]
    assert "실패" in call_text


@pytest.mark.asyncio
@patch("src.pipeline.db")
@patch("src.pipeline.fetch_prices")
@patch("src.pipeline.load_tickers")
@patch("src.pipeline.Bot")
async def test_pipeline_empty_data(mock_bot_cls, mock_load, mock_fetch, mock_db):
    mock_load.return_value = ["AAPL"]
    mock_db.get_all_user_settings.return_value = []
    mock_fetch.return_value = pd.DataFrame()

    mock_bot = AsyncMock()
    mock_bot_cls.return_value = mock_bot

    await run_pipeline()
    call_text = mock_bot.send_message.call_args[1]["text"]
    assert "비어있습니다" in call_text
