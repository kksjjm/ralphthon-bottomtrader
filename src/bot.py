"""
BottomTrader Telegram Bot — Command handler for user interactions.

Runs on Render.com as a long-polling bot.
"""
from __future__ import annotations

import structlog
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from src.core import db
from src.core.config import TELEGRAM_BOT_TOKEN
from src.core.market import load_ticker_info, resolve_ticker

logger = structlog.get_logger()

VALID_MODES = ("all", "watchlist", "both")


def _validate_positive_number(value: str) -> float | None:
    try:
        n = float(value)
        if n <= 0:
            return None
        return n
    except ValueError:
        return None


def _validate_threshold(value: str) -> float | None:
    n = _validate_positive_number(value)
    if n is not None and n > 100:
        return None
    return n


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    db.get_or_create_user_settings(user_id)
    await update.message.reply_text(
        "BottomTrader에 오신 것을 환영합니다! 🎯\n\n"
        "사용 가능한 명령어:\n"
        "/settings — 현재 설정 확인\n"
        "/set_period [일수] — 이동평균 기간 설정\n"
        "/set_drop [%] — 이동평균 대비 하락 임계값\n"
        "/set_daily_drop [%] — 일일 하락 임계값\n"
        "/tickers — 등록된 종목 리스트 조회\n"
        "/tickers [검색어] — 종목 검색 (한글/영문)\n"
        "/watch [종목...] — 워치리스트에 추가 (한글 가능)\n"
        "/unwatch [종목] — 워치리스트에서 제거\n"
        "/watchlist — 워치리스트 확인\n"
        "/mode [all|watchlist|both] — 모니터링 모드 변경\n"
        "/buy [종목] [가격] — 매수 기록\n"
        "/sell [종목] [가격] — 매도 기록\n"
        "/portfolio — 보유 포지션 확인\n"
        "/history — 과거 매매 기록"
    )


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    s = db.get_or_create_user_settings(user_id)
    wl = db.get_watchlist(user_id)
    wl_text = ", ".join(w["ticker"] for w in wl) if wl else "없음"
    await update.message.reply_text(
        f"⚙️ 현재 설정\n"
        f"이동평균 기간: {s['lookback_period']}일\n"
        f"평균 대비 하락 임계값: {s['drop_threshold']}%\n"
        f"일일 하락 임계값: {s['daily_drop_threshold']}%\n"
        f"모니터링 모드: {s.get('monitor_mode', 'all')}\n"
        f"워치리스트: {wl_text}"
    )


async def cmd_set_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("사용법: /set_period [일수]\n예: /set_period 20")
        return
    val = _validate_positive_number(context.args[0])
    if val is None or val != int(val) or val < 2:
        await update.message.reply_text("2 이상의 정수를 입력하세요.")
        return
    db.get_or_create_user_settings(user_id)
    db.update_user_settings(user_id, lookback_period=int(val))
    await update.message.reply_text(f"✅ 이동평균 기간을 {int(val)}일로 변경했습니다.")


async def cmd_set_drop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("사용법: /set_drop [%]\n예: /set_drop 15")
        return
    val = _validate_threshold(context.args[0])
    if val is None:
        await update.message.reply_text("1~100 사이의 숫자를 입력하세요.")
        return
    db.get_or_create_user_settings(user_id)
    db.update_user_settings(user_id, drop_threshold=val)
    await update.message.reply_text(f"✅ 이동평균 대비 하락 임계값을 {val}%로 변경했습니다.")


async def cmd_set_daily_drop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("사용법: /set_daily_drop [%]\n예: /set_daily_drop 7")
        return
    val = _validate_threshold(context.args[0])
    if val is None:
        await update.message.reply_text("1~100 사이의 숫자를 입력하세요.")
        return
    db.get_or_create_user_settings(user_id)
    db.update_user_settings(user_id, daily_drop_threshold=val)
    await update.message.reply_text(f"✅ 일일 하락 임계값을 {val}%로 변경했습니다.")


async def cmd_tickers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    infos = load_ticker_info()
    if context.args:
        query = " ".join(context.args).strip().lower()
        infos = [
            i for i in infos
            if query in i["ticker"].lower()
            or query in i["name"].lower()
            or query in i["name_kr"]
        ]
        if not infos:
            await update.message.reply_text(f"'{query}' 검색 결과가 없습니다.")
            return

    # Paginate: Telegram message limit ~4096 chars
    page_size = 40
    page = 0
    if context.args and context.args[-1].isdigit():
        page = int(context.args[-1]) - 1
        infos_filtered = load_ticker_info()
        query_parts = context.args[:-1]
        if query_parts:
            q = " ".join(query_parts).strip().lower()
            infos = [
                i for i in infos_filtered
                if q in i["ticker"].lower()
                or q in i["name"].lower()
                or q in i["name_kr"]
            ]

    total = len(infos)
    total_pages = (total + page_size - 1) // page_size
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    end = start + page_size
    page_items = infos[start:end]

    lines = [f"📋 등록 종목 ({total}개) — 페이지 {page + 1}/{total_pages}\n"]
    for i in page_items:
        lines.append(f"  {i['ticker']:6s} {i['name_kr']:12s} {i['name']}")

    if total_pages > 1:
        lines.append(f"\n다음 페이지: /tickers {page + 2}" if page + 1 < total_pages else "")

    await update.message.reply_text("\n".join(lines))


async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not context.args or len(context.args) != 1 or context.args[0] not in VALID_MODES:
        await update.message.reply_text(f"사용법: /mode [all|watchlist|both]\n현재 유효한 모드: {', '.join(VALID_MODES)}")
        return
    mode = context.args[0]
    db.get_or_create_user_settings(user_id)
    db.update_user_settings(user_id, monitor_mode=mode)
    mode_desc = {"all": "전체 유니버스", "watchlist": "워치리스트만", "both": "전체 + 워치리스트"}
    await update.message.reply_text(f"✅ 모니터링 모드를 '{mode_desc[mode]}'로 변경했습니다.")


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("사용법: /watch AAPL MSFT GOOGL\n한글도 가능: /watch 엔비디아 테슬라 애플")
        return
    db.get_or_create_user_settings(user_id)
    added = []
    not_found = []
    for arg in context.args:
        arg = arg.strip()
        resolved = resolve_ticker(arg)
        if resolved:
            result = db.add_to_watchlist(user_id, resolved)
            if result:
                if arg.upper() != resolved:
                    added.append(f"{resolved} ({arg})")
                else:
                    added.append(resolved)
        else:
            not_found.append(arg)
    lines = []
    if added:
        lines.append(f"✅ 워치리스트에 추가: {', '.join(added)}")
    if not_found:
        lines.append(f"❌ 찾을 수 없음: {', '.join(not_found)}")
        lines.append("/tickers [검색어] 로 종목을 검색해보세요.")
    if not lines:
        lines.append("추가할 유효한 종목이 없습니다.")
    await update.message.reply_text("\n".join(lines))


async def cmd_unwatch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("사용법: /unwatch AAPL\n한글도 가능: /unwatch 애플")
        return
    raw_ticker = context.args[0].strip()
    ticker = resolve_ticker(raw_ticker) or raw_ticker.upper()
    if db.remove_from_watchlist(user_id, ticker):
        await update.message.reply_text(f"✅ {ticker}를 워치리스트에서 제거했습니다.")
    else:
        await update.message.reply_text(f"❌ {ticker} 제거 실패.")


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    wl = db.get_watchlist(user_id)
    if not wl:
        await update.message.reply_text("📋 워치리스트가 비어있습니다.\n/watch AAPL MSFT 로 추가하세요.")
        return
    lines = ["📋 워치리스트"]
    for item in wl:
        threshold = f" (임계값: {item['custom_threshold']}%)" if item.get("custom_threshold") else ""
        lines.append(f"  • {item['ticker']}{threshold}")
    await update.message.reply_text("\n".join(lines))


async def cmd_buy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not context.args or len(context.args) != 2:
        await update.message.reply_text("사용법: /buy NVDA 120.50\n한글도 가능: /buy 엔비디아 120.50")
        return
    raw_ticker = context.args[0].strip()
    ticker = resolve_ticker(raw_ticker) or raw_ticker.upper()
    price = _validate_positive_number(context.args[1])
    if price is None:
        await update.message.reply_text("유효한 가격을 입력하세요. 예: /buy NVDA 120.50")
        return

    # Check for existing holding
    existing = db.find_holding_trade(user_id, ticker)
    if existing:
        await update.message.reply_text(f"❌ {ticker}는 이미 보유 중입니다. 먼저 /sell {ticker} [가격]으로 매도하세요.")
        return

    # Find the most recent alert for this ticker
    alert = db.get_latest_alert_for_ticker(ticker)
    alert_id = alert["id"] if alert else None

    if alert_id is None:
        await update.message.reply_text(f"⚠️ {ticker}에 대한 최근 알림이 없습니다. 그래도 매수를 기록합니다.")

    db.get_or_create_user_settings(user_id)
    trade = db.create_trade(user_id, alert_id, ticker, price)
    msg = f"✅ {ticker} 매수 기록 완료\n매수가: ${price:.2f}"
    if alert:
        msg += f"\n알림 시점 가격: ${float(alert['alert_price']):.2f}"
    await update.message.reply_text(msg)


async def cmd_sell(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not context.args or len(context.args) != 2:
        await update.message.reply_text("사용법: /sell NVDA 135.00\n한글도 가능: /sell 엔비디아 135.00")
        return
    raw_ticker = context.args[0].strip()
    ticker = resolve_ticker(raw_ticker) or raw_ticker.upper()
    price = _validate_positive_number(context.args[1])
    if price is None:
        await update.message.reply_text("유효한 가격을 입력하세요.")
        return

    trade = db.find_holding_trade(user_id, ticker)
    if not trade:
        await update.message.reply_text(f"❌ {ticker} 보유 포지션이 없습니다.")
        return

    closed = db.close_trade(trade["id"], price)
    return_pct = closed.get("return_pct", 0)
    holding_days = closed.get("holding_days", 0)
    emoji = "📈" if return_pct > 0 else "📉"
    await update.message.reply_text(
        f"{emoji} {ticker} 매도 완료\n"
        f"매수가: ${float(trade['buy_price']):.2f}\n"
        f"매도가: ${price:.2f}\n"
        f"수익률: {return_pct:+.2f}%\n"
        f"보유 기간: {holding_days}일"
    )


async def cmd_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    holdings = db.get_holding_trades(user_id)
    if not holdings:
        await update.message.reply_text("📊 보유 포지션이 없습니다.")
        return
    lines = [f"📊 보유 포지션 ({len(holdings)}종목)"]
    for t in holdings:
        ticker = t["ticker"]
        buy_price = float(t["buy_price"])
        buy_date = t["buy_date"][:10] if t.get("buy_date") else "?"
        lines.append(f"  {ticker} | 매수: ${buy_price:.2f} ({buy_date})")
    await update.message.reply_text("\n".join(lines))


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    trades = db.get_closed_trades(user_id)
    if not trades:
        await update.message.reply_text("📜 매매 기록이 없습니다.")
        return
    lines = ["📜 최근 매매 기록"]
    wins = sum(1 for t in trades if float(t.get("return_pct", 0)) > 0)
    total = len(trades)
    avg_return = sum(float(t.get("return_pct", 0)) for t in trades) / total if total else 0
    lines.append(f"승률: {wins}/{total} ({wins/total*100:.0f}%) | 평균 수익률: {avg_return:+.1f}%\n")
    for t in trades[:10]:
        emoji = "📈" if float(t.get("return_pct", 0)) > 0 else "📉"
        lines.append(
            f"  {emoji} {t['ticker']} | {float(t.get('return_pct', 0)):+.1f}% | "
            f"{t.get('holding_days', '?')}일"
        )
    await update.message.reply_text("\n".join(lines))


async def cmd_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "알 수 없는 명령어입니다. /start 로 사용 가능한 명령어를 확인하세요."
    )


def create_app() -> Application:
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("set_period", cmd_set_period))
    app.add_handler(CommandHandler("set_drop", cmd_set_drop))
    app.add_handler(CommandHandler("set_daily_drop", cmd_set_daily_drop))
    app.add_handler(CommandHandler("tickers", cmd_tickers))
    app.add_handler(CommandHandler("mode", cmd_mode))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("unwatch", cmd_unwatch))
    app.add_handler(CommandHandler("watchlist", cmd_watchlist))
    app.add_handler(CommandHandler("buy", cmd_buy))
    app.add_handler(CommandHandler("sell", cmd_sell))
    app.add_handler(CommandHandler("portfolio", cmd_portfolio))
    app.add_handler(CommandHandler("history", cmd_history))
    return app


def main() -> None:
    logger.info("bot_starting")
    app = create_app()
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
