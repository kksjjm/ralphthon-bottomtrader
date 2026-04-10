"""Tests for Telegram bot command handlers."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.bot import (
    cmd_buy,
    cmd_history,
    cmd_mode,
    cmd_portfolio,
    cmd_sell,
    cmd_set_daily_drop,
    cmd_set_drop,
    cmd_set_period,
    cmd_settings,
    cmd_start,
    cmd_watch,
    cmd_watchlist,
    cmd_unwatch,
    cmd_unknown,
    _validate_positive_number,
    _validate_threshold,
)


@pytest.fixture
def update():
    u = AsyncMock()
    u.effective_user.id = 12345
    u.message.reply_text = AsyncMock()
    return u


@pytest.fixture
def context():
    c = MagicMock()
    c.args = []
    return c


class TestValidation:
    def test_validate_positive_number_valid(self):
        assert _validate_positive_number("120.50") == 120.50
        assert _validate_positive_number("1") == 1.0

    def test_validate_positive_number_invalid(self):
        assert _validate_positive_number("0") is None
        assert _validate_positive_number("-5") is None
        assert _validate_positive_number("abc") is None

    def test_validate_threshold_valid(self):
        assert _validate_threshold("5") == 5.0
        assert _validate_threshold("99") == 99.0

    def test_validate_threshold_over_100(self):
        assert _validate_threshold("101") is None
        assert _validate_threshold("200") is None


@pytest.mark.asyncio
class TestStartCommand:
    @patch("src.bot.db")
    async def test_start(self, mock_db, update, context):
        mock_db.get_or_create_user_settings.return_value = {"user_id": 12345}
        await cmd_start(update, context)
        update.message.reply_text.assert_called_once()
        assert "BottomTrader" in update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
class TestSettingsCommand:
    @patch("src.bot.db")
    async def test_settings(self, mock_db, update, context):
        mock_db.get_or_create_user_settings.return_value = {
            "user_id": 12345,
            "lookback_period": 20,
            "drop_threshold": 10.0,
            "daily_drop_threshold": 5.0,
            "monitor_mode": "all",
        }
        mock_db.get_watchlist.return_value = [{"ticker": "AAPL"}]
        await cmd_settings(update, context)
        text = update.message.reply_text.call_args[0][0]
        assert "20일" in text
        assert "AAPL" in text


@pytest.mark.asyncio
class TestSetPeriodCommand:
    @patch("src.bot.db")
    async def test_set_period_valid(self, mock_db, update, context):
        context.args = ["30"]
        mock_db.get_or_create_user_settings.return_value = {}
        mock_db.update_user_settings.return_value = {}
        await cmd_set_period(update, context)
        mock_db.update_user_settings.assert_called_once_with(12345, lookback_period=30)

    @patch("src.bot.db")
    async def test_set_period_no_args(self, mock_db, update, context):
        context.args = []
        await cmd_set_period(update, context)
        assert "사용법" in update.message.reply_text.call_args[0][0]

    @patch("src.bot.db")
    async def test_set_period_invalid(self, mock_db, update, context):
        context.args = ["0"]
        await cmd_set_period(update, context)
        assert "2 이상" in update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
class TestSetDropCommand:
    @patch("src.bot.db")
    async def test_set_drop_valid(self, mock_db, update, context):
        context.args = ["15"]
        mock_db.get_or_create_user_settings.return_value = {}
        mock_db.update_user_settings.return_value = {}
        await cmd_set_drop(update, context)
        mock_db.update_user_settings.assert_called_once_with(12345, drop_threshold=15.0)

    @patch("src.bot.db")
    async def test_set_drop_over_100(self, mock_db, update, context):
        context.args = ["150"]
        await cmd_set_drop(update, context)
        assert "1~100" in update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
class TestSetDailyDropCommand:
    @patch("src.bot.db")
    async def test_set_daily_drop_valid(self, mock_db, update, context):
        context.args = ["7"]
        mock_db.get_or_create_user_settings.return_value = {}
        mock_db.update_user_settings.return_value = {}
        await cmd_set_daily_drop(update, context)
        mock_db.update_user_settings.assert_called_once_with(12345, daily_drop_threshold=7.0)


@pytest.mark.asyncio
class TestModeCommand:
    @patch("src.bot.db")
    async def test_mode_valid(self, mock_db, update, context):
        context.args = ["watchlist"]
        mock_db.get_or_create_user_settings.return_value = {}
        mock_db.update_user_settings.return_value = {}
        await cmd_mode(update, context)
        assert "워치리스트만" in update.message.reply_text.call_args[0][0]

    @patch("src.bot.db")
    async def test_mode_invalid(self, mock_db, update, context):
        context.args = ["invalid"]
        await cmd_mode(update, context)
        assert "사용법" in update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
class TestWatchCommand:
    @patch("src.bot.db")
    async def test_watch_tickers(self, mock_db, update, context):
        context.args = ["AAPL", "NVDA"]
        mock_db.get_or_create_user_settings.return_value = {}
        mock_db.add_to_watchlist.return_value = {"ticker": "AAPL"}
        await cmd_watch(update, context)
        assert mock_db.add_to_watchlist.call_count == 2

    @patch("src.bot.db")
    async def test_watch_no_args(self, mock_db, update, context):
        context.args = []
        await cmd_watch(update, context)
        assert "사용법" in update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
class TestUnwatchCommand:
    @patch("src.bot.db")
    async def test_unwatch_success(self, mock_db, update, context):
        context.args = ["AAPL"]
        mock_db.remove_from_watchlist.return_value = True
        await cmd_unwatch(update, context)
        assert "제거" in update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
class TestWatchlistCommand:
    @patch("src.bot.db")
    async def test_watchlist_empty(self, mock_db, update, context):
        mock_db.get_watchlist.return_value = []
        await cmd_watchlist(update, context)
        assert "비어있습니다" in update.message.reply_text.call_args[0][0]

    @patch("src.bot.db")
    async def test_watchlist_with_items(self, mock_db, update, context):
        mock_db.get_watchlist.return_value = [
            {"ticker": "AAPL", "custom_threshold": None},
            {"ticker": "NVDA", "custom_threshold": 8.0},
        ]
        await cmd_watchlist(update, context)
        text = update.message.reply_text.call_args[0][0]
        assert "AAPL" in text
        assert "NVDA" in text


@pytest.mark.asyncio
class TestBuyCommand:
    @patch("src.bot.db")
    async def test_buy_success(self, mock_db, update, context):
        context.args = ["NVDA", "120.50"]
        mock_db.find_holding_trade.return_value = None
        mock_db.get_latest_alert_for_ticker.return_value = {"id": 5, "alert_price": 130.0}
        mock_db.get_or_create_user_settings.return_value = {}
        mock_db.create_trade.return_value = {"id": 1}
        await cmd_buy(update, context)
        text = update.message.reply_text.call_args[0][0]
        assert "매수 기록 완료" in text

    @patch("src.bot.db")
    async def test_buy_already_holding(self, mock_db, update, context):
        context.args = ["NVDA", "120.50"]
        mock_db.find_holding_trade.return_value = {"id": 1, "ticker": "NVDA"}
        await cmd_buy(update, context)
        assert "이미 보유 중" in update.message.reply_text.call_args[0][0]

    @patch("src.bot.db")
    async def test_buy_invalid_price(self, mock_db, update, context):
        context.args = ["NVDA", "abc"]
        await cmd_buy(update, context)
        assert "유효한 가격" in update.message.reply_text.call_args[0][0]

    @patch("src.bot.db")
    async def test_buy_no_args(self, mock_db, update, context):
        context.args = []
        await cmd_buy(update, context)
        assert "사용법" in update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
class TestSellCommand:
    @patch("src.bot.db")
    async def test_sell_success(self, mock_db, update, context):
        context.args = ["NVDA", "135.00"]
        mock_db.find_holding_trade.return_value = {"id": 1, "ticker": "NVDA", "buy_price": "120.50"}
        mock_db.close_trade.return_value = {"return_pct": 12.03, "holding_days": 6}
        await cmd_sell(update, context)
        text = update.message.reply_text.call_args[0][0]
        assert "매도 완료" in text

    @patch("src.bot.db")
    async def test_sell_not_holding(self, mock_db, update, context):
        context.args = ["NVDA", "135.00"]
        mock_db.find_holding_trade.return_value = None
        await cmd_sell(update, context)
        assert "보유 포지션이 없습니다" in update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
class TestPortfolioCommand:
    @patch("src.bot.db")
    async def test_portfolio_empty(self, mock_db, update, context):
        mock_db.get_holding_trades.return_value = []
        await cmd_portfolio(update, context)
        assert "없습니다" in update.message.reply_text.call_args[0][0]

    @patch("src.bot.db")
    async def test_portfolio_with_holdings(self, mock_db, update, context):
        mock_db.get_holding_trades.return_value = [
            {"ticker": "NVDA", "buy_price": "120.50", "buy_date": "2026-04-05T10:00:00"},
        ]
        await cmd_portfolio(update, context)
        text = update.message.reply_text.call_args[0][0]
        assert "NVDA" in text
        assert "120.50" in text


@pytest.mark.asyncio
class TestHistoryCommand:
    @patch("src.bot.db")
    async def test_history_empty(self, mock_db, update, context):
        mock_db.get_closed_trades.return_value = []
        await cmd_history(update, context)
        assert "없습니다" in update.message.reply_text.call_args[0][0]

    @patch("src.bot.db")
    async def test_history_with_trades(self, mock_db, update, context):
        mock_db.get_closed_trades.return_value = [
            {"ticker": "NVDA", "return_pct": "12.03", "holding_days": 6},
            {"ticker": "TSLA", "return_pct": "-5.20", "holding_days": 3},
        ]
        await cmd_history(update, context)
        text = update.message.reply_text.call_args[0][0]
        assert "승률" in text
        assert "NVDA" in text


@pytest.mark.asyncio
class TestUnknownCommand:
    async def test_unknown(self, update, context):
        await cmd_unknown(update, context)
        assert "알 수 없는" in update.message.reply_text.call_args[0][0]
