"""
LLM Analysis Quality Evaluation.

Run with: pytest tests/eval/ -v --no-header -k "eval"
Requires: OPENAI_API_KEY to be set (real API calls).

This file tests against known historical drops with verified causes.
Add more golden cases as you validate them manually.
"""
import json

import pytest

# Golden dataset: known historical drops with verified causes.
# Add cases after completing "The Assignment" (test 10 historical cases manually).
GOLDEN_CASES = [
    {
        "ticker": "NVDA",
        "drop_pct": -10.0,
        "news": [
            {"title": "NVIDIA faces EU antitrust probe over AI chip dominance", "source": "Reuters", "link": "https://example.com"},
        ],
        "expected_keywords": ["EU", "반독점", "조사"],
        "expected_confidence": "HIGH",
    },
    {
        "ticker": "META",
        "drop_pct": -26.0,
        "news": [
            {"title": "Meta reports Q4 revenue miss, user growth stalls", "source": "CNBC", "link": "https://example.com"},
        ],
        "expected_keywords": ["실적", "매출", "성장"],
        "expected_confidence": "HIGH",
    },
    {
        "ticker": "AAPL",
        "drop_pct": -5.0,
        "news": [],
        "expected_keywords": [],
        "expected_confidence": "LOW",
    },
]


@pytest.mark.skipif(
    not pytest.importorskip("openai", reason="OpenAI not installed"),
    reason="Requires real API key"
)
@pytest.mark.parametrize("case", GOLDEN_CASES, ids=[c["ticker"] for c in GOLDEN_CASES])
def test_analysis_quality(case):
    """
    Placeholder for LLM quality evaluation.

    To run actual eval:
    1. Set OPENAI_API_KEY environment variable
    2. Uncomment the code below
    3. Run: pytest tests/eval/ -v
    """
    # This test validates the golden dataset structure
    assert "ticker" in case
    assert "drop_pct" in case
    assert "news" in case
    assert "expected_confidence" in case
    assert case["expected_confidence"] in ("HIGH", "MEDIUM", "LOW")

    # Uncomment below for actual LLM evaluation:
    # import asyncio
    # from src.core.analyzer import analyze_single_drop
    # from openai import AsyncOpenAI
    # from src.core.config import OPENAI_API_KEY
    #
    # async def _run():
    #     client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    #     result = await analyze_single_drop(
    #         client, case["ticker"], case["drop_pct"], case["news"]
    #     )
    #     # Check confidence
    #     assert result["confidence"] == case["expected_confidence"], \
    #         f"Expected {case['expected_confidence']}, got {result['confidence']}"
    #     # Check keywords in cause
    #     for kw in case["expected_keywords"]:
    #         assert kw in result["cause"], f"Missing keyword '{kw}' in: {result['cause']}"
    #     return result
    #
    # asyncio.run(_run())
