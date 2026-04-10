"""
BottomTrader Pipeline — Cron job entry point.

Runs daily after US market close. Detects drops, analyzes causes, sends Telegram alerts.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime

import structlog
from telegram import Bot

from src.core import db
from src.core.analyzer import analyze_drops, analyze_macro_crash
from src.core.config import (
    CIRCUIT_BREAKER_THRESHOLD,
    DEFAULT_DAILY_DROP_THRESHOLD,
    DEFAULT_DROP_THRESHOLD,
    DEFAULT_LOOKBACK_PERIOD,
    FINNHUB_API_KEY,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHANNEL_ID,
)
from src.core.market import (
    extract_avg_drops,
    extract_daily_drops,
    fetch_news_google,
    fetch_prices,
    is_market_open_today,
    load_tickers,
)

logger = structlog.get_logger()

DISCLAIMER = "\n\n⚠️ 본 정보는 투자 조언이 아닙니다. 투자 결정은 본인의 판단과 책임 하에 이루어져야 합니다."


def _merge_drops(daily: list[dict], avg: list[dict]) -> list[dict]:
    """Merge daily drops and average drops, deduplicating by ticker."""
    by_ticker: dict[str, dict] = {}
    for d in daily:
        by_ticker[d["ticker"]] = {**d}
    for a in avg:
        t = a["ticker"]
        if t in by_ticker:
            by_ticker[t]["avg_drop_pct"] = a["avg_drop_pct"]
            by_ticker[t]["avg_price"] = a.get("avg_price")
        else:
            by_ticker[t] = {**a, "daily_drop_pct": 0}
    return list(by_ticker.values())


def _format_alert_message(analysis: dict, drop_info: dict) -> str:
    ticker = analysis["ticker"]
    daily_pct = drop_info.get("daily_drop_pct", 0)
    avg_pct = drop_info.get("avg_drop_pct")
    cause = analysis.get("cause", "분석 불가")
    confidence = analysis.get("confidence", "LOW")
    sources = analysis.get("sources", [])

    lines = [f"📉 {ticker} {daily_pct:+.1f}% (일일)"]
    if avg_pct:
        lines[0] += f" | 이동평균 대비 {avg_pct:+.1f}%"
    lines.append(f"원인: {cause}")
    lines.append(f"신뢰도: {confidence}")
    if sources:
        lines.append(f"소스: {sources[0]}")
    lines.append(f"→ /buy {ticker} [가격] 으로 매수 기록")
    return "\n".join(lines)


def _format_macro_message(analysis: dict, drop_count: int) -> str:
    cause = analysis.get("cause", "분석 불가")
    confidence = analysis.get("confidence", "LOW")
    sources = analysis.get("sources", [])
    lines = [
        f"🔻 시장 전체 급락 — {drop_count}개 종목 하락",
        "",
        f"원인: {cause}",
        f"신뢰도: {confidence}",
    ]
    if sources:
        lines.append(f"소스: {sources[0]}")
    lines.append("\n개별 종목 알림은 회로 차단기에 의해 생략되었습니다.")
    return "\n".join(lines)


async def _send_telegram(bot: Bot, text: str) -> None:
    """Send a message to the Telegram channel, splitting if needed."""
    max_len = 4000
    full_text = text + DISCLAIMER
    if len(full_text) <= max_len:
        await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=full_text)
        return
    parts = []
    current = ""
    for line in full_text.split("\n"):
        if len(current) + len(line) + 1 > max_len:
            parts.append(current)
            current = line
        else:
            current = current + "\n" + line if current else line
    if current:
        parts.append(current)
    for part in parts:
        await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=part)


async def update_position_snapshots(price_data, tickers: list[str], today: date) -> None:
    """Update daily snapshots for all active holding trades."""
    holdings = db.get_all_holding_trades()
    if not holdings:
        return
    for trade in holdings:
        ticker = trade["ticker"]
        try:
            if len(tickers) == 1:
                close_series = price_data["Close"]
            else:
                close_series = price_data[(ticker, "Close")]
            if close_series.empty:
                continue
            current_price = float(close_series.iloc[-1])

            alert = None
            if trade.get("alert_id"):
                alerts_resp = db.get_client().table("alerts").select("*").eq("id", trade["alert_id"]).execute()
                if alerts_resp.data:
                    alert = alerts_resp.data[0]

            return_from_alert = None
            if alert and alert.get("alert_price"):
                alert_price = float(alert["alert_price"])
                if alert_price > 0:
                    return_from_alert = ((current_price - alert_price) / alert_price) * 100

            return_from_buy = None
            if trade.get("buy_price"):
                buy_price = float(trade["buy_price"])
                if buy_price > 0:
                    return_from_buy = ((current_price - buy_price) / buy_price) * 100

            db.insert_snapshot(trade["id"], today, current_price, return_from_alert, return_from_buy)
            logger.info("snapshot_updated", trade_id=trade["id"], ticker=ticker, price=current_price)
        except (KeyError, IndexError) as e:
            logger.warning("snapshot_failed", trade_id=trade["id"], ticker=ticker, error=str(e))


async def run_pipeline() -> None:
    """Main pipeline: fetch prices, detect drops, analyze, send alerts."""
    logger.info("pipeline_started")
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    today = date.today()

    # Load tickers and user settings
    all_tickers = load_tickers()
    users = db.get_all_user_settings()

    # If no users, use defaults
    if not users:
        users = [{
            "user_id": 0,
            "lookback_period": DEFAULT_LOOKBACK_PERIOD,
            "drop_threshold": DEFAULT_DROP_THRESHOLD,
            "daily_drop_threshold": DEFAULT_DAILY_DROP_THRESHOLD,
            "monitor_mode": "all",
        }]

    # Determine tickers to fetch (union of all user watchlists + universe)
    tickers_to_fetch = set(all_tickers)
    for user in users:
        if user.get("monitor_mode") in ("watchlist", "both"):
            wl = db.get_watchlist(user["user_id"])
            for item in wl:
                tickers_to_fetch.add(item["ticker"])
    tickers_list = sorted(tickers_to_fetch)

    # Fetch price data (batch download)
    try:
        price_data = fetch_prices(tickers_list)
    except Exception as e:
        logger.error("price_fetch_failed", error=str(e))
        await _send_telegram(bot, "❌ 오늘 주가 데이터 수집에 실패했습니다.")
        return

    if price_data.empty:
        await _send_telegram(bot, "❌ 오늘 주가 데이터가 비어있습니다.")
        return

    # Check market holiday
    if not is_market_open_today(price_data):
        await _send_telegram(bot, "🏖️ 오늘은 미국 시장 휴장입니다.")
        logger.info("market_closed")
        return

    # Detect drops using the most aggressive thresholds across all users
    min_daily = min(float(u.get("daily_drop_threshold", DEFAULT_DAILY_DROP_THRESHOLD)) for u in users)
    daily_drops = extract_daily_drops(price_data, tickers_list, daily_threshold=min_daily)

    min_avg = min(float(u.get("drop_threshold", DEFAULT_DROP_THRESHOLD)) for u in users)
    max_lookback = max(int(u.get("lookback_period", DEFAULT_LOOKBACK_PERIOD)) for u in users)
    avg_drops = extract_avg_drops(price_data, tickers_list, lookback_period=max_lookback, avg_threshold=min_avg)

    all_drops = _merge_drops(daily_drops, avg_drops)

    if not all_drops:
        await _send_telegram(bot, f"📊 {today.isoformat()} — 오늘은 설정 기준에 맞는 급락 종목이 없습니다.")
        logger.info("no_drops_found")
        await update_position_snapshots(price_data, tickers_list, today)
        return

    logger.info("drops_found", count=len(all_drops))

    # Circuit breaker: market-wide crash
    if len(all_drops) > CIRCUIT_BREAKER_THRESHOLD:
        logger.warning("circuit_breaker_triggered", count=len(all_drops))
        sample_news = []
        for d in all_drops[:5]:
            news = fetch_news_google(d["ticker"])
            sample_news.extend(news)
        macro_analysis = await analyze_macro_crash(len(all_drops), sample_news)
        msg = _format_macro_message(macro_analysis, len(all_drops))
        await _send_telegram(bot, f"📉 급락 알림 ({today.isoformat()})\n\n{msg}")

        # Still store alerts for the top drops
        sorted_drops = sorted(all_drops, key=lambda x: x.get("daily_drop_pct", 0))
        for d in sorted_drops[:20]:
            db.insert_alert(
                ticker=d["ticker"],
                run_date=today,
                drop_pct=d.get("daily_drop_pct", 0),
                alert_price=d["close"],
                avg_drop_pct=d.get("avg_drop_pct"),
                cause=macro_analysis.get("cause"),
                confidence=macro_analysis.get("confidence"),
            )
        await update_position_snapshots(price_data, tickers_list, today)
        return

    # Fetch news for each drop (sequential, respecting rate limits)
    for drop in all_drops:
        drop["news"] = fetch_news_google(drop["ticker"])

    # Analyze drops (parallel LLM calls)
    analyses = await analyze_drops(all_drops)

    # Build analysis lookup
    analysis_map = {a["ticker"]: a for a in analyses}

    # Store alerts and send messages
    messages = [f"📉 급락 알림 ({today.isoformat()})"]
    for drop in sorted(all_drops, key=lambda x: x.get("daily_drop_pct", 0)):
        ticker = drop["ticker"]
        analysis = analysis_map.get(ticker, {
            "ticker": ticker, "cause": "분석 없음", "confidence": "LOW", "sources": []
        })

        # Store alert (idempotent via UNIQUE constraint)
        db.insert_alert(
            ticker=ticker,
            run_date=today,
            drop_pct=drop.get("daily_drop_pct", 0),
            alert_price=drop["close"],
            avg_drop_pct=drop.get("avg_drop_pct"),
            cause=analysis.get("cause"),
            confidence=analysis.get("confidence"),
            sources=analysis.get("sources"),
        )

        messages.append("")
        messages.append(_format_alert_message(analysis, drop))

    await _send_telegram(bot, "\n".join(messages))

    # Update position snapshots
    await update_position_snapshots(price_data, tickers_list, today)

    logger.info("pipeline_complete", alerts=len(all_drops))


def main() -> None:
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()
