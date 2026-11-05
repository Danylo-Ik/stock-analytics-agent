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

The system is being narrowed from a broader real-time analytics platform to focus specifically on the two-agent recommendation pipeline. Some existing Spark streaming jobs may be removed or simplified as the scope tightens.

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

- **ingestion** — Python service with two threads: a WebSocket client for live trades (`finnhub_trades` topic) and a REST poller for quotes every 5s (`finnhub_quotes` topic). Reconnects with exponential backoff.
- **streaming** — Four Spark Structured Streaming jobs launched by `run_jobs.sh` with staggered start offsets:
  - `whale_alert.py` — detects trades above 95th percentile volume (30s trigger)
  - `volatility_monitor.py` — tracks 5-min price volatility, alerts on 2x spikes (60s trigger)
  - `market_momentum.py` — 1-min price momentum snapshots (60s trigger)
  - `spread_monitor.py` — bid-ask spread tracking
- **batch** — APScheduler running three Spark batch jobs:
  - `hourly_report.py` at `:00` — hourly OHLCV summaries
  - `trading_patterns.py` at `:30` — hour-of-day pattern analysis
  - `large_trades_analysis.py` every 4h — whale trade impact metrics
- **api** — FastAPI on port 8000. Routes in `api/src/routers/`: `price.py`, `trades.py`, `analytics.py`, `reports.py`. Cassandra connection pool in `api/src/db/cassandra.py`.

### Kafka Topics

| Topic | Producer | Consumer |
|-------|----------|----------|
| `finnhub_trades` | Ingestion (WebSocket) | All 4 streaming jobs |
| `finnhub_quotes` | Ingestion (REST poller) | Spread monitor |
| `alerts_whale_trades` | Whale alert job | — |
| `alerts_volatility` | Volatility monitor | — |
| `alerts_spread` | Spread monitor | — |

### Cassandra (keyspace: `market_analytics`)

Raw data tables (`trades`, `quotes`) have 7-day TTL. Streaming results (`whale_alerts`, `volatility_alerts`, `volatility_windows`, `market_momentum`, `spread_history`) have TTLs ranging from 1 hour to 30 days. Batch results (`hourly_reports`, `trading_patterns`, `large_trades_analysis`) have no TTL. Schema defined in `infra/cassandra/init.cql`.

### Service startup order

Kafka → kafka-init (topic creation) → Cassandra → cassandra-init (schema) → Spark master → Spark worker → ingestion → streaming → batch → api. Health checks gate each dependency.

## Environment Variables

Configured in `.env` at project root. Key variables:
- `FINNHUB_API_KEY` — required, Finnhub API token
- `SYMBOLS` — comma-separated stock tickers (e.g., `AAPL,MSFT,NVDA`)
- `KAFKA_BOOTSTRAP_SERVERS`, `CASSANDRA_HOST`, `CASSANDRA_KEYSPACE` — infrastructure addresses
- `WHALE_PERCENTILE`, `VOLATILITY_WINDOW_MINUTES`, `SPREAD_ALERT_MULTIPLIER` — tunable alert thresholds

## Finnhub Data Formats

Three data streams are relevant. Full details and quality notes in README.md section 5.

### Trade ticks (WebSocket → `finnhub_trades` Kafka topic)
Normalised by `ingestion/src/finnhub_client.py:_normalise_trade`:
`{ symbol, price, volume, notional, timestamp (ISO-8601), conditions, is_dark_pool }`

### Quotes (REST polling → `finnhub_quotes` Kafka topic)
Normalised by `ingestion/src/finnhub_client.py:_fetch_quote`:
`{ symbol, bid, ask, bid_size, ask_size, spread, spread_pct, timestamp }`
**Known issue:** only the `c` (current price) field is used from Finnhub; `d`, `dp`, `h`, `l`, `o`, `pc` are discarded. Bid/ask is synthetic (0.02% spread). These extra fields need to be preserved for the Analytics Agent.

### Company news (REST, not yet ingested)
`GET /api/v1/company-news?symbol={sym}&from={date}&to={date}` returns:
`[{ id, headline, summary, source, url, image, category, datetime (UNIX), related }]`
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
