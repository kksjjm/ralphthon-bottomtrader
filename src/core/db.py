from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import structlog
from supabase import Client, create_client
from tenacity import retry, stop_after_attempt, wait_exponential

from src.core.config import SUPABASE_KEY, SUPABASE_URL

logger = structlog.get_logger()

_client: Client | None = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


def reset_client() -> None:
    global _client
    _client = None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def _execute(table: str, operation: str, **kwargs: Any) -> Any:
    client = get_client()
    query = client.table(table)
    if operation == "select":
        q = query.select(kwargs.get("columns", "*"))
        if "filters" in kwargs:
            for col, val in kwargs["filters"].items():
                q = q.eq(col, val)
        return q.execute()
    if operation == "insert":
        return query.insert(kwargs["data"]).execute()
    if operation == "upsert":
        return query.upsert(kwargs["data"]).execute()
    if operation == "update":
        q = query.update(kwargs["data"])
        for col, val in kwargs["filters"].items():
            q = q.eq(col, val)
        return q.execute()
    if operation == "delete":
        q = query.delete()
        for col, val in kwargs["filters"].items():
            q = q.eq(col, val)
        return q.execute()
    raise ValueError(f"Unknown operation: {operation}")


# --- User Settings ---

def get_user_settings(user_id: int) -> dict | None:
    resp = _execute("user_settings", "select", filters={"user_id": user_id})
    if resp.data:
        return resp.data[0]
    return None


def get_or_create_user_settings(user_id: int) -> dict:
    settings = get_user_settings(user_id)
    if settings:
        return settings
    resp = _execute("user_settings", "insert", data={"user_id": user_id})
    return resp.data[0]


def update_user_settings(user_id: int, **fields: Any) -> dict:
    fields["updated_at"] = datetime.now(UTC).isoformat()
    resp = _execute("user_settings", "update", data=fields, filters={"user_id": user_id})
    return resp.data[0]


def get_all_user_settings() -> list[dict]:
    resp = _execute("user_settings", "select")
    return resp.data or []


# --- Alerts ---

def insert_alert(
    ticker: str,
    run_date: date,
    drop_pct: float,
    alert_price: float,
    avg_drop_pct: float | None = None,
    cause: str | None = None,
    confidence: str | None = None,
    sources: list[str] | None = None,
) -> dict | None:
    data = {
        "ticker": ticker,
        "run_date": run_date.isoformat(),
        "drop_pct": float(drop_pct),
        "alert_price": float(alert_price),
    }
    if avg_drop_pct is not None:
        data["avg_drop_pct"] = float(avg_drop_pct)
    if cause is not None:
        data["cause"] = cause
    if confidence is not None:
        data["confidence"] = confidence
    if sources is not None:
        data["sources"] = sources
    try:
        resp = _execute("alerts", "upsert", data=data)
        return resp.data[0] if resp.data else None
    except Exception:
        logger.warning("alert_insert_failed", ticker=ticker, run_date=str(run_date))
        return None


def get_alerts_by_date(run_date: date) -> list[dict]:
    resp = _execute("alerts", "select", filters={"run_date": run_date.isoformat()})
    return resp.data or []


def get_latest_alert_for_ticker(ticker: str) -> dict | None:
    client = get_client()
    resp = (
        client.table("alerts")
        .select("*")
        .eq("ticker", ticker)
        .order("run_date", desc=True)
        .limit(1)
        .execute()
    )
    if resp.data:
        return resp.data[0]
    return None


# --- Trades ---

def create_trade(user_id: int, alert_id: int, ticker: str, buy_price: float,
                  buy_ma_price: float | None = None, buy_ma_period: int | None = None) -> dict:
    data = {
        "user_id": user_id,
        "alert_id": alert_id,
        "ticker": ticker,
        "buy_price": float(buy_price),
        "buy_date": datetime.now(UTC).isoformat(),
        "status": "holding",
    }
    if buy_ma_price is not None:
        data["buy_ma_price"] = float(buy_ma_price)
    if buy_ma_period is not None:
        data["buy_ma_period"] = buy_ma_period
    resp = _execute("trades", "insert", data=data)
    return resp.data[0]


def close_trade(trade_id: int, sell_price: float) -> dict:
    client = get_client()
    trade_resp = client.table("trades").select("*").eq("id", trade_id).execute()
    if not trade_resp.data:
        raise ValueError(f"Trade {trade_id} not found")
    trade = trade_resp.data[0]
    buy_price = float(trade["buy_price"])
    return_pct = ((sell_price - buy_price) / buy_price) * 100
    buy_date = datetime.fromisoformat(trade["buy_date"])
    now = datetime.now(UTC)
    if buy_date.tzinfo is None:
        buy_date = buy_date.replace(tzinfo=UTC)
    holding_days = (now - buy_date).days
    data = {
        "sell_price": float(sell_price),
        "sell_date": datetime.now(UTC).isoformat(),
        "status": "closed",
        "return_pct": round(return_pct, 2),
        "holding_days": holding_days,
    }
    resp = _execute("trades", "update", data=data, filters={"id": trade_id})
    return resp.data[0]


def get_holding_trades(user_id: int) -> list[dict]:
    client = get_client()
    resp = (
        client.table("trades")
        .select("*")
        .eq("user_id", user_id)
        .eq("status", "holding")
        .execute()
    )
    return resp.data or []


def get_all_holding_trades() -> list[dict]:
    client = get_client()
    resp = client.table("trades").select("*").eq("status", "holding").execute()
    return resp.data or []


def get_closed_trades(user_id: int, limit: int = 20) -> list[dict]:
    client = get_client()
    resp = (
        client.table("trades")
        .select("*")
        .eq("user_id", user_id)
        .eq("status", "closed")
        .order("sell_date", desc=True)
        .limit(limit)
        .execute()
    )
    return resp.data or []


def find_holding_trade(user_id: int, ticker: str) -> dict | None:
    client = get_client()
    resp = (
        client.table("trades")
        .select("*")
        .eq("user_id", user_id)
        .eq("ticker", ticker.upper())
        .eq("status", "holding")
        .limit(1)
        .execute()
    )
    if resp.data:
        return resp.data[0]
    return None


# --- Trade Snapshots ---

def insert_snapshot(trade_id: int, snapshot_date: date, close_price: float,
                    return_from_alert: float | None, return_from_buy: float | None) -> dict:
    data = {
        "trade_id": trade_id,
        "snapshot_date": snapshot_date.isoformat(),
        "close_price": float(close_price),
    }
    if return_from_alert is not None:
        data["return_from_alert"] = round(return_from_alert, 2)
    if return_from_buy is not None:
        data["return_from_buy"] = round(return_from_buy, 2)
    resp = _execute("trade_snapshots", "insert", data=data)
    return resp.data[0]


# --- Watchlist ---

def add_to_watchlist(user_id: int, ticker: str, custom_threshold: float | None = None) -> dict | None:
    data: dict[str, Any] = {"user_id": user_id, "ticker": ticker.upper()}
    if custom_threshold is not None:
        data["custom_threshold"] = float(custom_threshold)
    try:
        resp = _execute("watchlist", "upsert", data=data)
        return resp.data[0] if resp.data else None
    except Exception:
        logger.warning("watchlist_add_failed", user_id=user_id, ticker=ticker)
        return None


def remove_from_watchlist(user_id: int, ticker: str) -> bool:
    try:
        client = get_client()
        client.table("watchlist").delete().eq("user_id", user_id).eq("ticker", ticker.upper()).execute()
        return True
    except Exception:
        return False


def get_watchlist(user_id: int) -> list[dict]:
    resp = _execute("watchlist", "select", filters={"user_id": user_id})
    return resp.data or []
