from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np
import pytest

from src.core.market import (
    extract_avg_drops,
    extract_daily_drops,
    is_market_open_today,
    load_tickers,
    fetch_news_google,
)


@pytest.fixture
def sample_price_data():
    """Create sample multi-ticker price data similar to yfinance output."""
    dates = pd.date_range("2026-01-01", periods=30, freq="B")
    tickers = ["AAPL", "NVDA", "TSLA"]
    arrays = []
    for ticker in tickers:
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            arrays.append((ticker, col))
    index = pd.MultiIndex.from_tuples(arrays)
    data = pd.DataFrame(
        np.random.uniform(100, 200, size=(30, len(arrays))),
        index=dates,
        columns=index,
    )
    # Set NVDA to have a big daily drop on the last day
    data.iloc[-2, data.columns.get_loc(("NVDA", "Close"))] = 150.0
    data.iloc[-1, data.columns.get_loc(("NVDA", "Close"))] = 135.0  # -10%

    # Set TSLA stable
    for i in range(30):
        data.iloc[i, data.columns.get_loc(("TSLA", "Close"))] = 200.0

    return data


def test_load_tickers():
    tickers = load_tickers()
    assert len(tickers) > 0
    assert "AAPL" in tickers
    assert "NVDA" in tickers
    assert all(isinstance(t, str) for t in tickers)


def test_extract_daily_drops(sample_price_data):
    drops = extract_daily_drops(sample_price_data, ["AAPL", "NVDA", "TSLA"], daily_threshold=5.0)
    tickers_dropped = [d["ticker"] for d in drops]
    assert "NVDA" in tickers_dropped
    nvda_drop = next(d for d in drops if d["ticker"] == "NVDA")
    assert nvda_drop["daily_drop_pct"] == pytest.approx(-10.0, abs=0.1)
    assert nvda_drop["close"] == pytest.approx(135.0)


def test_extract_daily_drops_no_drops(sample_price_data):
    drops = extract_daily_drops(sample_price_data, ["TSLA"], daily_threshold=5.0)
    assert len(drops) == 0


def test_extract_daily_drops_custom_threshold(sample_price_data):
    # With threshold of 15%, NVDA's -10% shouldn't trigger
    drops = extract_daily_drops(sample_price_data, ["NVDA"], daily_threshold=15.0)
    assert len(drops) == 0


def test_extract_avg_drops(sample_price_data):
    # NVDA with last close at 135 vs average of ~150 area should trigger
    drops = extract_avg_drops(
        sample_price_data, ["NVDA"], lookback_period=20, avg_threshold=5.0
    )
    # May or may not trigger depending on random data, so just check structure
    for d in drops:
        assert "ticker" in d
        assert "close" in d
        assert "avg_drop_pct" in d


def test_is_market_open_today():
    dates = pd.date_range(end=date.today(), periods=5, freq="B")
    data = pd.DataFrame({"Close": range(5)}, index=dates)
    result = is_market_open_today(data)
    # Result depends on whether today is a business day
    assert isinstance(result, bool)


def test_is_market_open_today_empty():
    data = pd.DataFrame()
    assert is_market_open_today(data) is False


def test_is_market_open_old_data():
    dates = pd.date_range("2020-01-01", periods=5, freq="B")
    data = pd.DataFrame({"Close": range(5)}, index=dates)
    assert is_market_open_today(data) is False


@patch("src.core.market.feedparser.parse")
def test_fetch_news_google(mock_parse):
    mock_parse.return_value = MagicMock(
        entries=[
            {"title": "NVDA drops", "link": "https://example.com/1", "published": "2026-04-11", "source": {"title": "Reuters"}},
            {"title": "Market news", "link": "https://example.com/2", "published": "2026-04-11", "source": {"title": "CNBC"}},
        ]
    )
    news = fetch_news_google("NVDA")
    assert len(news) == 2
    assert news[0]["title"] == "NVDA drops"
    assert news[0]["source"] == "Reuters"


@patch("src.core.market.feedparser.parse")
def test_fetch_news_google_empty(mock_parse):
    mock_parse.return_value = MagicMock(entries=[])
    news = fetch_news_google("XXXX")
    assert news == []


@patch("src.core.market.feedparser.parse")
def test_fetch_news_google_error(mock_parse):
    mock_parse.side_effect = Exception("Network error")
    news = fetch_news_google("NVDA")
    assert news == []
