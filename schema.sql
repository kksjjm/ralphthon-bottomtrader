-- BottomTrader Database Schema
-- Run this in Supabase SQL Editor to set up tables

CREATE TABLE user_settings (
  user_id BIGINT PRIMARY KEY,
  lookback_period INT DEFAULT 20,
  drop_threshold DECIMAL DEFAULT 10.0,
  daily_drop_threshold DECIMAL DEFAULT 5.0,
  monitor_mode VARCHAR(20) DEFAULT 'all',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE alerts (
  id SERIAL PRIMARY KEY,
  ticker VARCHAR(10) NOT NULL,
  run_date DATE NOT NULL,
  drop_pct DECIMAL NOT NULL,
  avg_drop_pct DECIMAL,
  alert_price DECIMAL NOT NULL,
  cause TEXT,
  confidence VARCHAR(10),
  sources TEXT[],
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(ticker, run_date)
);
CREATE INDEX idx_alerts_run_date ON alerts(run_date);
CREATE INDEX idx_alerts_ticker ON alerts(ticker);

CREATE TABLE trades (
  id SERIAL PRIMARY KEY,
  user_id BIGINT REFERENCES user_settings(user_id),
  alert_id INT REFERENCES alerts(id),
  ticker VARCHAR(10) NOT NULL,
  buy_price DECIMAL,
  buy_date TIMESTAMPTZ,
  sell_price DECIMAL,
  sell_date TIMESTAMPTZ,
  status VARCHAR(20) DEFAULT 'watching',
  return_pct DECIMAL,
  holding_days INT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_trades_user_status ON trades(user_id, status);
CREATE INDEX idx_trades_ticker ON trades(ticker);

CREATE TABLE trade_snapshots (
  id SERIAL PRIMARY KEY,
  trade_id INT REFERENCES trades(id),
  snapshot_date DATE NOT NULL,
  close_price DECIMAL NOT NULL,
  return_from_alert DECIMAL,
  return_from_buy DECIMAL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_snapshots_trade_date ON trade_snapshots(trade_id, snapshot_date);

CREATE TABLE watchlist (
  id SERIAL PRIMARY KEY,
  user_id BIGINT REFERENCES user_settings(user_id),
  ticker VARCHAR(10) NOT NULL,
  custom_threshold DECIMAL,
  added_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(user_id, ticker)
);
CREATE INDEX idx_watchlist_user ON watchlist(user_id);
