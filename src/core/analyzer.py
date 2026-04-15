from __future__ import annotations

import asyncio
import json

import structlog
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from src.core.config import OPENAI_API_KEY

logger = structlog.get_logger()

RECOVERY_LEVELS = ("매우낮음", "낮음", "보통", "높음", "매우높음")

SYSTEM_PROMPT = """너는 미국 주식 시장 분석가다. 주어진 주가 데이터와 뉴스를 바탕으로 주가 급락의 가장 가능성 높은 원인을 한국어로 2-3문장으로 설명하라.

또한, 해당 종목이 단기(2~4주) 이내에 이동평균선 수준으로 복귀할 가능성을 다음 5단계 중 하나로 평가하라:
- 매우높음: 단기적/일회성 악재로 과매도, 펀더멘털 훼손 없음
- 높음: 종목 특이적 부정 뉴스지만 회복 여력 있음
- 보통: 신호 혼재, 불확실성 존재
- 낮음: 구조적/중기적 악재(실적 악화, 경쟁 심화, 수요 둔화 등)
- 매우낮음: 중대 이벤트(파산 우려, 대형 소송, 규제 직격, CEO 사임 등)로 회복 난망

반드시 JSON 형식으로 응답하라. 매번 입력된 하락률·뉴스 내용을 실제로 반영해 분석을 다르게 작성하라:
{
  "cause": "원인 설명 (한국어, 2-3문장, 입력된 하락률과 뉴스 제목을 구체적으로 반영)",
  "recovery_likelihood": "매우낮음|낮음|보통|높음|매우높음",
  "sources": ["참고한 뉴스 제목 목록"]
}

뉴스가 없는 경우: "관련 뉴스를 찾을 수 없습니다. 기술적 매도 또는 시장 전반 하락 영향일 수 있습니다."로 답하고 recovery_likelihood는 '보통'으로 설정."""

MACRO_SYSTEM_PROMPT = """너는 미국 주식 시장 분석가다. 오늘 시장 전체가 크게 하락했다. 급락 종목 수와 주요 뉴스를 바탕으로 시장 전체 하락의 원인을 한국어로 3-5문장으로 요약하라.

반드시 JSON 형식으로 응답하라:
{
  "cause": "시장 전체 하락 원인 요약 (한국어, 3-5문장)",
  "recovery_likelihood": "매우낮음|낮음|보통|높음|매우높음",
  "sources": ["참고한 뉴스 제목 목록"]
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
            lines.append(f"{i}. [{source}] {title}")
    else:
        lines.append("\n관련 뉴스: 없음")
    return "\n".join(lines)


def _normalize_recovery(value) -> str:
    """Normalize recovery_likelihood to one of the 5 Korean levels."""
    if not value:
        return "보통"
    s = str(value).strip()
    if s in RECOVERY_LEVELS:
        return s
    upper = s.upper()
    legacy = {"HIGH": "높음", "MEDIUM": "보통", "LOW": "낮음"}
    if upper in legacy:
        return legacy[upper]
    return "보통"


def _parse_response(text: str) -> dict:
    """Parse LLM response as JSON. Falls back to raw text if parsing fails."""
    parsed = None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            start = text.index("{")
            end = text.rindex("}") + 1
            parsed = json.loads(text[start:end])
        except (ValueError, json.JSONDecodeError):
            parsed = None

    if parsed is None:
        return {
            "cause": text,
            "recovery_likelihood": "보통",
            "sources": [],
        }

    # Normalize recovery_likelihood (accept legacy confidence field too)
    raw = parsed.get("recovery_likelihood") or parsed.get("confidence")
    parsed["recovery_likelihood"] = _normalize_recovery(raw)
    parsed.setdefault("cause", "")
    parsed.setdefault("sources", [])
    return parsed


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
        logger.info(
            "analysis_complete",
            ticker=ticker,
            recovery_likelihood=result.get("recovery_likelihood"),
            drop_pct=drop_pct,
            news_count=len(news),
        )
        return result
    except Exception as e:
        logger.error("llm_analysis_failed", ticker=ticker, error=str(e))
        headlines = "; ".join(n.get("title", "") for n in news[:3]) if news else "뉴스 없음"
        return {
            "ticker": ticker,
            "cause": f"AI 분석 실패. 관련 뉴스: {headlines}",
            "recovery_likelihood": "보통",
            "sources": [],
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
            "recovery_likelihood": "보통",
            "sources": [],
        }
