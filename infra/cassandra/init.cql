CREATE KEYSPACE IF NOT EXISTS market_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
  AND durable_writes = true;

USE market_analytics;

-- ── Raw trades ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trades (
    symbol       TEXT,
    trade_time   TIMESTAMP,
    price        DOUBLE,
    volume       BIGINT,
    notional     DOUBLE,
    conditions   LIST<TEXT>,
    is_dark_pool BOOLEAN,
    PRIMARY KEY ((symbol), trade_time)
) WITH CLUSTERING ORDER BY (trade_time DESC)
  AND default_time_to_live = 604800;

-- ── Raw quotes ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS quotes (
    symbol     TEXT,
    quote_time TIMESTAMP,
    bid        DOUBLE,
    ask        DOUBLE,
    bid_size   BIGINT,
    ask_size   BIGINT,
    spread     DOUBLE,
    spread_pct DOUBLE,
    PRIMARY KEY ((symbol), quote_time)
) WITH CLUSTERING ORDER BY (quote_time DESC)
  AND default_time_to_live = 604800;

-- ── Streaming: Market Momentum (1-minute windows) ─────────────────────────
CREATE TABLE IF NOT EXISTS market_momentum (
    symbol          TEXT,
    window_start    TIMESTAMP,
    current_price   DOUBLE,
    prev_price      DOUBLE,
    price_change_pct DOUBLE,
    volume          BIGINT,
    notional        DOUBLE,
    buy_volume      BIGINT,
    sell_volume     BIGINT,
    trade_count     INT,
    vwap            DOUBLE,
    PRIMARY KEY ((symbol), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 86400;

-- ── Streaming alerts: Whale Alerts ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS whale_alerts (
    symbol           TEXT,
    alert_time       TIMESTAMP,
    trade_size       BIGINT,
    notional         DOUBLE,
    threshold_95p    DOUBLE,
    price            DOUBLE,
    deviation_percent DOUBLE,
    is_dark_pool     BOOLEAN,
    PRIMARY KEY ((symbol), alert_time)
) WITH CLUSTERING ORDER BY (alert_time DESC)
  AND default_time_to_live = 2592000;

-- ── Company News ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS company_news (
    symbol       TEXT,
    published_at TIMESTAMP,
    news_id      BIGINT,
    headline     TEXT,
    summary      TEXT,
    source       TEXT,
    url          TEXT,
    category     TEXT,
    related      TEXT,
    PRIMARY KEY ((symbol), published_at, news_id)
) WITH CLUSTERING ORDER BY (published_at DESC, news_id DESC)
  AND default_time_to_live = 604800;

-- ── Batch: Hourly Trading Report ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hourly_reports (
    symbol          TEXT,
    hour_start      TIMESTAMP,
    trade_count     BIGINT,
    total_volume    BIGINT,
    total_notional  DOUBLE,
    min_price       DOUBLE,
    max_price       DOUBLE,
    avg_price       DOUBLE,
    vwap            DOUBLE,
    volatility      DOUBLE,
    buy_volume      BIGINT,
    sell_volume     BIGINT,
    dominant_side   TEXT,
    PRIMARY KEY ((symbol), hour_start)
) WITH CLUSTERING ORDER BY (hour_start DESC);

-- ── Batch: Trading Patterns ─────────────────────────────
CREATE TABLE IF NOT EXISTS trading_patterns (
    symbol          TEXT,
    hour_of_day     INT,
    avg_trade_count DOUBLE,
    avg_volume      DOUBLE,
    avg_volatility  DOUBLE,
    avg_spread_pct  DOUBLE,
    sample_days     INT,
    updated_at      TIMESTAMP,
    PRIMARY KEY ((symbol), hour_of_day)
);

-- ── Batch: Large Trades Analysis ───────────────────────────
CREATE TABLE IF NOT EXISTS large_trades_analysis (
    symbol            TEXT,
    analysis_time     TIMESTAMP,
    trade_time        TIMESTAMP,
    trade_price       DOUBLE,
    trade_size        BIGINT,
    trade_notional    DOUBLE,
    price_5m_after    DOUBLE,
    price_impact_pct  DOUBLE,
    is_dark_pool      BOOLEAN,
    percentile_rank   DOUBLE,
    PRIMARY KEY ((symbol), analysis_time)
) WITH CLUSTERING ORDER BY (analysis_time DESC);
