# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Methodology

This project follows the **CRISP-DM** (Cross-Industry Standard Process for Data Mining) cycle:
1. **Business Understanding** — define objectives and requirements (documented in README.md)
2. **Data Understanding** — explore Finnhub trade/quote/news data quality and coverage
3. **Data Preparation** — ingestion, cleaning, feature extraction via Kafka and Spark
4. **Modelling** — Analytics Agent (trend indicators) and News Agent (NLP sentiment/impact)
5. **Evaluation** — per-component metrics and end-to-end recommendation quality
6. **Deployment** — Dockerised, single-command startup

When making changes, consider which CRISP-DM phase the work falls under. Evaluation is required for every component — each must have measurable quality metrics.

## Project Overview

Stock price awareness assistant that ingests real-time US market data from Finnhub, analyses price trends via a periodic **Analytics Agent**, retrieves and interprets relevant news via a **News Agent**, and combines both signals into buy/hold/sell recommendations. Served through a FastAPI REST API. All services run in Docker.

The system has been narrowed from a broader real-time analytics platform to focus on the two-agent recommendation pipeline. Spread and volatility streaming jobs have been removed; whale alert and market momentum remain.

## Commands

### Full system startup
```bash
docker compose up -d
```

### Individual service logs
```bash
docker compose logs -f streaming
docker compose logs -f api
docker compose logs -f ingestion
docker compose logs -f batch
```

### Cassandra shell
```bash
docker exec -it cassandra cqlsh
# USE market_analytics; DESCRIBE TABLES;
```

### Manually trigger streaming jobs
```bash
docker exec -it streaming bash run_jobs.sh
```

### Run a single batch job manually
```bash
docker exec batch spark-submit --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  /app/src/hourly_report.py
```

### API docs (Swagger UI)
http://localhost:8000/docs

### No test suite or linter is currently configured.

## Architecture

**Data flow:** Finnhub → Ingestion → Kafka → Spark Streaming/Batch → Cassandra → FastAPI

### Services (Docker Compose)

- **ingestion** — Python service with three threads: a WebSocket client for live trades (`finnhub_trades` topic), a REST poller for quotes every 5s (`finnhub_quotes` topic), and a REST poller for company news every 5min (`finnhub_news` topic). Reconnects with exponential backoff.
- **streaming** — Two Spark Structured Streaming jobs launched by `run_jobs.sh`:
  - `whale_alert.py` — detects trades above 95th percentile volume over 60-min window (10-min trigger)
  - `market_momentum.py` — 1-min price momentum snapshots (60s trigger)
- **batch** — APScheduler running three Spark batch jobs:
  - `hourly_report.py` at `:00` — hourly OHLCV summaries
  - `trading_patterns.py` at `:30` — hour-of-day pattern analysis
  - `large_trades_analysis.py` every 4h — whale trade impact metrics
- **api** — FastAPI on port 8000. Routes in `api/src/routers/`: `price.py`, `trades.py`, `analytics.py`, `reports.py`. Cassandra connection pool in `api/src/db/cassandra.py`.

### Kafka Topics

| Topic | Producer | Consumer |
|-------|----------|----------|
| `finnhub_trades` | Ingestion (WebSocket) | Whale alert, Market momentum |
| `finnhub_quotes` | Ingestion (REST poller) | — |
| `finnhub_news` | Ingestion (REST poller) | — |
| `alerts_whale_trades` | Whale alert job | — |

### Cassandra (keyspace: `market_analytics`)

Raw data tables (`trades`, `quotes`) have 7-day TTL. Streaming results (`whale_alerts`, `market_momentum`) have TTLs of 30 days and 1 day respectively. `company_news` has 7-day TTL. Batch results (`hourly_reports`, `trading_patterns`, `large_trades_analysis`) have no TTL. Schema defined in `infra/cassandra/init.cql`.

### Service startup order

Kafka → kafka-init (topic creation) → Cassandra → cassandra-init (schema) → Spark master → Spark worker → ingestion → streaming → batch → api. Health checks gate each dependency.

## Environment Variables

Configured in `.env` at project root. Key variables:
- `FINNHUB_API_KEY` — required, Finnhub API token
- `SYMBOLS` — comma-separated stock tickers (e.g., `AAPL,MSFT,NVDA`)
- `KAFKA_BOOTSTRAP_SERVERS`, `CASSANDRA_HOST`, `CASSANDRA_KEYSPACE` — infrastructure addresses
- `WHALE_ALERT_TRIGGER_SECS` — whale alert trigger interval (default 600)
- `NEWS_POLL_INTERVAL` — news polling interval in seconds (default 300)

## Finnhub Data Formats

Three data streams are relevant. Full details and quality notes in README.md section 5.

### Trade ticks (WebSocket → `finnhub_trades` Kafka topic)
Normalised by `ingestion/src/finnhub_client.py:_normalise_trade`:
`{ symbol, price, volume, notional, timestamp (ISO-8601), conditions, is_dark_pool }`

### Quotes (REST polling → `finnhub_quotes` Kafka topic)
Normalised by `ingestion/src/finnhub_client.py:_fetch_quote`:
`{ symbol, bid, ask, bid_size, ask_size, spread, spread_pct, timestamp }`
**Known issue:** only the `c` (current price) field is used from Finnhub; `d`, `dp`, `h`, `l`, `o`, `pc` are discarded. Bid/ask is synthetic (0.02% spread). These extra fields need to be preserved for the Analytics Agent.

### Company news (REST polling → `finnhub_news` Kafka topic)
Normalised by `ingestion/src/finnhub_client.py:_normalise_news`:
`{ symbol, news_id, headline, summary, source, url, category, published_at (ISO-8601), related }`
Polled every 5 minutes (configurable). Deduplication by `news_id`. Stored in `company_news` Cassandra table.
No sentiment label — the News Agent must infer sentiment from headline + summary via LLM.

### News sentiment (REST, optional complement)
`GET /api/v1/news-sentiment?symbol={sym}` returns pre-aggregated weekly buzz and bullish/bearish percentages. Coarse granularity; useful as a cross-check, not a primary signal.

## Key Conventions

- Python 3.11 throughout; all application code is Python
- Spark jobs use `spark-cassandra-connector` and `spark-sql-kafka` packages (version 3.5.0)
- Streaming checkpoints stored at `/tmp/checkpoints/{job_name}`
- Dark pool trades identified by Finnhub conditions "41" or "8"
- Spark driver memory: 512MB; executor memory: 1GB; Cassandra heap: 2GB
- Keep symbol list small (3–5 tickers) due to free-tier rate limits (~60 req/min)
- Trade WebSocket is active during US market hours (09:30–16:00 ET); data is sparse off-hours
