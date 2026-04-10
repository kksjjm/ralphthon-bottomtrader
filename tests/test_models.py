"""Tests for DB model operations (src/core/db.py).

These tests mock the Supabase client to test business logic without hitting a real DB.
"""
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from src.core import db


@pytest.fixture(autouse=True)
def reset_db_client():
    db.reset_client()
    yield
    db.reset_client()


@pytest.fixture
def mock_client():
    mock = MagicMock()
    with patch("src.core.db.get_client", return_value=mock):
        yield mock


class TestUserSettings:
    def test_get_user_settings_exists(self, mock_client):
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"user_id": 123, "lookback_period": 20, "drop_threshold": 10.0}]
        )
        result = db.get_user_settings(123)
        assert result is not None
        assert result["user_id"] == 123

    def test_get_user_settings_not_found(self, mock_client):
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
        result = db.get_user_settings(999)
        assert result is None

    def test_get_or_create_creates_new(self, mock_client):
        # First call: select returns empty (user doesn't exist)
        select_mock = MagicMock(data=[])
        insert_mock = MagicMock(data=[{"user_id": 456, "lookback_period": 20}])
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = select_mock
        mock_client.table.return_value.insert.return_value.execute.return_value = insert_mock

        result = db.get_or_create_user_settings(456)
        assert result["user_id"] == 456


class TestAlerts:
    def test_insert_alert(self, mock_client):
        mock_client.table.return_value.upsert.return_value.execute.return_value = MagicMock(
            data=[{"id": 1, "ticker": "NVDA", "run_date": "2026-04-11"}]
        )
        result = db.insert_alert(
            ticker="NVDA",
            run_date=date(2026, 4, 11),
            drop_pct=-10.0,
            alert_price=135.0,
            cause="EU probe",
            confidence="HIGH",
            sources=["https://reuters.com"],
        )
        assert result is not None
        assert result["ticker"] == "NVDA"

    def test_insert_alert_duplicate(self, mock_client):
        mock_client.table.return_value.upsert.return_value.execute.side_effect = Exception("duplicate")
        result = db.insert_alert("NVDA", date(2026, 4, 11), -10.0, 135.0)
        assert result is None

    def test_get_alerts_by_date(self, mock_client):
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[
                {"id": 1, "ticker": "NVDA", "run_date": "2026-04-11"},
                {"id": 2, "ticker": "TSLA", "run_date": "2026-04-11"},
            ]
        )
        alerts = db.get_alerts_by_date(date(2026, 4, 11))
        assert len(alerts) == 2

    def test_get_latest_alert_for_ticker(self, mock_client):
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[{"id": 5, "ticker": "NVDA", "run_date": "2026-04-11"}]
        )
        result = db.get_latest_alert_for_ticker("NVDA")
        assert result["id"] == 5


class TestTrades:
    def test_create_trade(self, mock_client):
        mock_client.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[{"id": 1, "ticker": "NVDA", "buy_price": 120.5, "status": "holding"}]
        )
        result = db.create_trade(user_id=123, alert_id=5, ticker="NVDA", buy_price=120.5)
        assert result["status"] == "holding"

    def test_close_trade(self, mock_client):
        # Mock the trade lookup
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": 1, "buy_price": "120.5", "buy_date": "2026-04-05T10:00:00"}]
        )
        # Mock the update
        mock_client.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": 1, "status": "closed", "return_pct": 12.03, "holding_days": 6}]
        )
        result = db.close_trade(1, 135.0)
        assert result["status"] == "closed"

    def test_close_trade_not_found(self, mock_client):
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
        with pytest.raises(ValueError, match="not found"):
            db.close_trade(999, 100.0)

    def test_find_holding_trade(self, mock_client):
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[{"id": 1, "ticker": "NVDA", "status": "holding"}]
        )
        result = db.find_holding_trade(123, "NVDA")
        assert result is not None
        assert result["ticker"] == "NVDA"

    def test_find_holding_trade_not_found(self, mock_client):
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[]
        )
        result = db.find_holding_trade(123, "XXXX")
        assert result is None


class TestWatchlist:
    def test_add_to_watchlist(self, mock_client):
        mock_client.table.return_value.upsert.return_value.execute.return_value = MagicMock(
            data=[{"user_id": 123, "ticker": "AAPL"}]
        )
        result = db.add_to_watchlist(123, "aapl")
        assert result is not None
        # Verify ticker is uppercased
        call_args = mock_client.table.return_value.upsert.call_args
        assert call_args[0][0]["ticker"] == "AAPL"

    def test_add_to_watchlist_duplicate(self, mock_client):
        mock_client.table.return_value.upsert.return_value.execute.side_effect = Exception("dup")
        result = db.add_to_watchlist(123, "AAPL")
        assert result is None

    def test_remove_from_watchlist(self, mock_client):
        mock_client.table.return_value.delete.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
        assert db.remove_from_watchlist(123, "AAPL") is True

    def test_get_watchlist(self, mock_client):
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"ticker": "AAPL"}, {"ticker": "NVDA"}]
        )
        wl = db.get_watchlist(123)
        assert len(wl) == 2
