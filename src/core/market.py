from __future__ import annotations

import csv
import urllib.parse
from datetime import date, timedelta
from pathlib import Path

import feedparser
import pandas as pd
import structlog
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential

logger = structlog.get_logger()

TICKERS_CSV = Path(__file__).parent.parent.parent / "data" / "tickers.csv"


def load_tickers() -> list[str]:
    tickers = []
    with open(TICKERS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tickers.append(row["ticker"].strip())
    return sorted(set(tickers))


def load_ticker_info() -> list[dict]:
    """Load full ticker info including Korean names."""
    results = []
    seen = set()
    with open(TICKERS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row["ticker"].strip()
            if ticker in seen:
                continue
            seen.add(ticker)
            results.append({
                "ticker": ticker,
                "name": row.get("name", "").strip(),
                "name_kr": row.get("name_kr", "").strip(),
                "index": row.get("index", "").strip(),
            })
    return sorted(results, key=lambda x: x["ticker"])


def resolve_ticker(query: str) -> str | None:
    """Resolve a ticker or Korean name to a ticker symbol.

    Accepts: ticker (NVDA), English name (NVIDIA), Korean name (엔비디아).
    Returns the ticker symbol or None if not found.
    """
    query_upper = query.upper().strip()
    query_lower = query.lower().strip()
    with open(TICKERS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row["ticker"].strip().upper()
            name = row.get("name", "").strip().lower()
            name_kr = row.get("name_kr", "").strip()
            if query_upper == ticker:
                return ticker
            if query_lower == name:
                return ticker
            if query == name_kr:
                return ticker
    return None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def fetch_prices(tickers: list[str], period: str = "3mo") -> pd.DataFrame:
    """Batch download prices for all tickers. Returns DataFrame with Close prices."""
    logger.info("fetching_prices", count=len(tickers), period=period)
    try:
        data = yf.download(tickers, period=period, group_by="ticker", progress=False, threads=True)
        if data.empty:
            logger.warning("yfinance_empty_response")
            return pd.DataFrame()
        return data
    except Exception as e:
        logger.error("yfinance_failed", error=str(e))
        raise


def extract_daily_drops(
    data: pd.DataFrame,
    tickers: list[str],
    daily_threshold: float = 5.0,
) -> list[dict]:
    """Find tickers that dropped more than daily_threshold% from previous close."""
    drops = []
    for ticker in tickers:
        try:
            if len(tickers) == 1:
                close = data["Close"]
            else:
                close = data[(ticker, "Close")]
            if close.empty or len(close) < 2:
                continue
            last_close = float(close.iloc[-1])
            prev_close = float(close.iloc[-2])
            if pd.isna(last_close) or pd.isna(prev_close) or prev_close == 0:
                continue
            pct_change = ((last_close - prev_close) / prev_close) * 100
            if pct_change <= -daily_threshold:
                drops.append({
                    "ticker": ticker,
                    "close": last_close,
                    "prev_close": prev_close,
                    "daily_drop_pct": round(pct_change, 2),
                })
        except (KeyError, IndexError):
            continue
    return drops


def extract_avg_drops(
    data: pd.DataFrame,
    tickers: list[str],
    lookback_period: int = 20,
    avg_threshold: float = 10.0,
) -> list[dict]:
    """Find tickers that dropped more than avg_threshold% below N-day moving average."""
    drops = []
    for ticker in tickers:
        try:
            if len(tickers) == 1:
                close = data["Close"]
            else:
                close = data[(ticker, "Close")]
            if close.empty or len(close) < lookback_period + 1:
                continue
            last_close = float(close.iloc[-1])
            avg_price = float(close.iloc[-(lookback_period + 1):-1].mean())
            if pd.isna(last_close) or pd.isna(avg_price) or avg_price == 0:
                continue
            pct_from_avg = ((last_close - avg_price) / avg_price) * 100
            if pct_from_avg <= -avg_threshold:
                drops.append({
                    "ticker": ticker,
                    "close": last_close,
                    "avg_price": round(avg_price, 2),
                    "avg_drop_pct": round(pct_from_avg, 2),
                    "lookback_period": lookback_period,
                })
        except (KeyError, IndexError):
            continue
    return drops


def is_market_open_today(data: pd.DataFrame) -> bool:
    """Check if the most recent trading date is today."""
    if data.empty:
        return False
    last_date = data.index[-1]
    if hasattr(last_date, "date"):
        last_date = last_date.date()
    return last_date == date.today()


def fetch_news_google(ticker: str, company_name: str = "") -> list[dict]:
    """Fetch news from Google News RSS. Free, unlimited, but less reliable."""
    query = f"{ticker} stock"
    if company_name:
        query = f"{company_name} {ticker} stock"
    encoded = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
    try:
        feed = feedparser.parse(url)
        results = []
        for entry in feed.entries[:5]:
            results.append({
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "source": entry.get("source", {}).get("title", ""),
            })
        logger.info("news_fetched", ticker=ticker, count=len(results), source="google")
        return results
    except Exception as e:
        logger.warning("google_news_failed", ticker=ticker, error=str(e))
        return []


def fetch_news_finnhub(ticker: str, api_key: str) -> list[dict]:
    """Fetch news from Finnhub. High quality but rate-limited (60/day free)."""
    if not api_key:
        return []
    import urllib.request
    import json

    today = date.today()
    from_date = (today - timedelta(days=2)).isoformat()
    to_date = today.isoformat()
    url = f"https://finnhub.io/api/v1/company-news?symbol={ticker}&from={from_date}&to={to_date}&token={api_key}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        results = []
        for item in data[:5]:
            results.append({
                "title": item.get("headline", ""),
                "link": item.get("url", ""),
                "published": item.get("datetime", ""),
                "source": item.get("source", ""),
            })
        logger.info("news_fetched", ticker=ticker, count=len(results), source="finnhub")
        return results
    except Exception as e:
        logger.warning("finnhub_news_failed", ticker=ticker, error=str(e))
        return []
