from __future__ import annotations

import asyncio
import json

import structlog
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from src.core.config import OPENAI_API_KEY

logger = structlog.get_logger()

SYSTEM_PROMPT = """너는 미국 주식 시장 분석가다. 주어진 주가 데이터와 뉴스를 바탕으로 주가 급락의 가장 가능성 높은 원인을 한국어로 2-3문장으로 설명하라.

반드시 JSON 형식으로 응답하라:
{
  "cause": "원인 설명 (한국어, 2-3문장)",
  "confidence": "HIGH 또는 MEDIUM 또는 LOW",
  "sources": ["소스 URL 목록"]
}

신뢰도 기준:
- HIGH: 24시간 이내 해당 기업에 대한 구체적 뉴스(실적 발표, FDA 결정, 소송, CEO 사임 등)가 주요 매체에 보도됨
- MEDIUM: 섹터/매크로 뉴스가 있지만 해당 기업 특이적이지 않음, 또는 인과관계 불분명
- LOW: 관련 뉴스를 찾을 수 없거나, 추측성 분석만 가능

뉴스가 없는 경우: "관련 뉴스를 찾을 수 없습니다. 기술적 매도 또는 시장 전반 하락의 영향일 수 있습니다."로 응답하고 confidence를 LOW로 설정."""

MACRO_SYSTEM_PROMPT = """너는 미국 주식 시장 분석가다. 오늘 시장 전체가 크게 하락했다. 급락 종목 수와 주요 뉴스를 바탕으로 시장 전체 하락의 원인을 한국어로 3-5문장으로 요약하라.

반드시 JSON 형식으로 응답하라:
{
  "cause": "시장 전체 하락 원인 요약 (한국어, 3-5문장)",
  "confidence": "HIGH 또는 MEDIUM 또는 LOW",
  "sources": ["소스 URL 목록"]
}"""


def _build_user_prompt(ticker: str, drop_pct: float, news: list[dict],
                       avg_drop_pct: float | None = None) -> str:
    lines = [f"종목: {ticker}", f"일일 하락률: {drop_pct:.1f}%"]
    if avg_drop_pct is not None:
        lines.append(f"이동평균 대비 하락률: {avg_drop_pct:.1f}%")
    if news:
        lines.append("\n관련 뉴스:")
        for i, item in enumerate(news[:5], 1):
            source = item.get("source", "")
            title = item.get("title", "")
            link = item.get("link", "")
            lines.append(f"{i}. [{source}] {title}")
            if link:
                lines.append(f"   URL: {link}")
    else:
        lines.append("\n관련 뉴스: 없음")
    return "\n".join(lines)


def _parse_response(text: str) -> dict:
    """Parse LLM response as JSON. Falls back to raw text if parsing fails."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            start = text.index("{")
            end = text.rindex("}") + 1
            return json.loads(text[start:end])
        except (ValueError, json.JSONDecodeError):
            return {
                "cause": text,
                "confidence": "LOW",
                "sources": [],
            }


async def analyze_single_drop(
    client: AsyncOpenAI,
    ticker: str,
    drop_pct: float,
    news: list[dict],
    avg_drop_pct: float | None = None,
) -> dict:
    """Analyze a single stock drop using the LLM."""
    user_prompt = _build_user_prompt(ticker, drop_pct, news, avg_drop_pct)
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.3,
            timeout=30,
        )
        text = response.choices[0].message.content or ""
        result = _parse_response(text)
        result["ticker"] = ticker
        logger.info("analysis_complete", ticker=ticker, confidence=result.get("confidence"))
        return result
    except Exception as e:
        logger.error("llm_analysis_failed", ticker=ticker, error=str(e))
        headlines = "; ".join(n.get("title", "") for n in news[:3]) if news else "뉴스 없음"
        return {
            "ticker": ticker,
            "cause": f"AI 분석 실패. 관련 뉴스: {headlines}",
            "confidence": "LOW",
            "sources": [n.get("link", "") for n in news if n.get("link")],
        }


async def analyze_drops(drops_with_news: list[dict]) -> list[dict]:
    """Analyze multiple drops in parallel using asyncio."""
    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    tasks = []
    for item in drops_with_news:
        tasks.append(
            analyze_single_drop(
                client,
                ticker=item["ticker"],
                drop_pct=item.get("daily_drop_pct", 0),
                news=item.get("news", []),
                avg_drop_pct=item.get("avg_drop_pct"),
            )
        )
    results = await asyncio.gather(*tasks, return_exceptions=True)
    analyses = []
    for r in results:
        if isinstance(r, Exception):
            logger.error("analysis_task_failed", error=str(r))
            continue
        analyses.append(r)
    return analyses


async def analyze_macro_crash(drop_count: int, sample_news: list[dict]) -> dict:
    """Analyze a market-wide crash event."""
    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    news_text = "\n".join(
        f"- [{n.get('source', '')}] {n.get('title', '')}" for n in sample_news[:10]
    )
    user_prompt = f"오늘 급락 종목 수: {drop_count}개\n\n주요 뉴스:\n{news_text}"
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": MACRO_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.3,
            timeout=30,
        )
        text = response.choices[0].message.content or ""
        return _parse_response(text)
    except Exception as e:
        logger.error("macro_analysis_failed", error=str(e))
        return {
            "cause": f"시장 전체 급락 (종목 {drop_count}개 하락). AI 분석 실패.",
            "confidence": "LOW",
            "sources": [],
        }
