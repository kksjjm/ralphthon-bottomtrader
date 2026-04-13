"""
Built-in scheduler for running the pipeline at fixed times.
Runs inside the bot process so no separate cron service is needed.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta

import structlog

logger = structlog.get_logger()

KST = timezone(timedelta(hours=9))

# Run at Korean 8:00 AM and 8:00 PM
SCHEDULE_HOURS_KST = [8, 20]


def _next_run_time() -> datetime:
    """Calculate the next scheduled run time in UTC."""
    now_kst = datetime.now(KST)
    for hour in sorted(SCHEDULE_HOURS_KST):
        candidate = now_kst.replace(hour=hour, minute=0, second=0, microsecond=0)
        if candidate > now_kst:
            return candidate.astimezone(timezone.utc)
    # All today's slots passed, schedule for tomorrow's first slot
    tomorrow = now_kst + timedelta(days=1)
    first_hour = min(SCHEDULE_HOURS_KST)
    candidate = tomorrow.replace(hour=first_hour, minute=0, second=0, microsecond=0)
    return candidate.astimezone(timezone.utc)


async def run_scheduler() -> None:
    """Loop forever, running the pipeline at scheduled times."""
    from src.pipeline import run_pipeline

    logger.info("scheduler_started", schedule_kst=SCHEDULE_HOURS_KST)

    while True:
        next_run = _next_run_time()
        now = datetime.now(timezone.utc)
        wait_seconds = (next_run - now).total_seconds()

        next_kst = next_run.astimezone(KST)
        logger.info(
            "scheduler_waiting",
            next_run_kst=next_kst.strftime("%Y-%m-%d %H:%M KST"),
            wait_minutes=round(wait_seconds / 60, 1),
        )

        await asyncio.sleep(wait_seconds)

        logger.info("scheduler_triggering_pipeline")
        try:
            await asyncio.wait_for(run_pipeline(), timeout=600)
            logger.info("scheduler_pipeline_complete")
        except asyncio.TimeoutError:
            logger.error("scheduler_pipeline_timeout", timeout_seconds=600)
        except Exception as e:
            logger.error("scheduler_pipeline_error", error=str(e))
