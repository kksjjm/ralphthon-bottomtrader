import os

import pytest

# Set test environment variables before any imports
os.environ.setdefault("OPENAI_API_KEY", "test-key")
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:ABC-TEST")
os.environ.setdefault("TELEGRAM_CHANNEL_ID", "-100test")
os.environ.setdefault("FINNHUB_API_KEY", "test-finnhub")
