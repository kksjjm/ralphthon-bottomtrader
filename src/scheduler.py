"""
Built-in scheduler for running the pipeline at fixed times.
Runs inside the bot process so no separate cron service is needed.

Schedule (KST):
  06:00 - Fetch + analyze + save to DB (no Telegram send)
  08:00 - Read from DB + send Telegram (fast, data already ready)
  20:00 - Fetch + analyze + send Telegram (all-in-one)
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta

import structlog

logger = structlog.get_logger()

KST = timezone(timedelta(hours=9))

# (hour_kst, task_type)
SCHEDULE = [
    (6, "fetch_only"),
    (8, "send_only"),
    (20, "fetch_and_send"),
]


def _next_task() -> tuple[datetime, str]:
    """Calculate the next scheduled task time (UTC) and its type."""
    now_kst = datetime.now(KST)
    for hour, task_type in sorted(SCHEDULE, key=lambda x: x[0]):
        candidate = now_kst.replace(hour=hour, minute=0, second=0, microsecond=0)
        if candidate > now_kst:
            return candidate.astimezone(timezone.utc), task_type
    # All today's slots passed, schedule for tomorrow's first slot
    tomorrow = now_kst + timedelta(days=1)
    first_hour, first_type = min(SCHEDULE, key=lambda x: x[0])
    candidate = tomorrow.replace(hour=first_hour, minute=0, second=0, microsecond=0)
    return candidate.astimezone(timezone.utc), first_type


async def run_scheduler() -> None:
    """Loop forever, running tasks at scheduled times."""
    from src.pipeline import run_pipeline, send_cached_alerts

    schedule_desc = [f"{h}시({t})" for h, t in SCHEDULE]
    logger.info("scheduler_started", schedule_kst=schedule_desc)

    while True:
        next_run, task_type = _next_task()
        now = datetime.now(timezone.utc)
        wait_seconds = (next_run - now).total_seconds()

        next_kst = next_run.astimezone(KST)
        logger.info(
            "scheduler_waiting",
            next_run_kst=next_kst.strftime("%Y-%m-%d %H:%M KST"),
            task_type=task_type,
            wait_minutes=round(wait_seconds / 60, 1),
        )

        await asyncio.sleep(wait_seconds)

        logger.info("scheduler_triggering", task_type=task_type)
        try:
            if task_type == "fetch_only":
                await asyncio.wait_for(run_pipeline(send_telegram=False), timeout=600)
            elif task_type == "send_only":
                await asyncio.wait_for(send_cached_alerts(), timeout=120)
            elif task_type == "fetch_and_send":
                await asyncio.wait_for(run_pipeline(send_telegram=True), timeout=600)
            logger.info("scheduler_task_complete", task_type=task_type)
        except asyncio.TimeoutError:
            logger.error("scheduler_timeout", task_type=task_type)
        except Exception as e:
            logger.error("scheduler_error", task_type=task_type, error=str(e))
