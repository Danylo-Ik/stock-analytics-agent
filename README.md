# Stock Price Awareness Assistant

An NLP-driven system that monitors company stock prices and relevant news in real time, analyses market trends, and provides informed buy/hold/sell recommendations. Built as an NLP course assignment following the **CRISP-DM** development methodology.

---

## 1. Business Understanding

### Business Objective

Retail investors and analysts need timely, consolidated insight into stock movements and the news events that drive them. Manually tracking price feeds, reading financial news, and correlating the two is slow and error-prone. The goal of this system is to **automate the monitoring and interpretation loop**: continuously ingest live trading data for a set of companies, periodically analyse price trends, pull relevant news, and synthesise both data streams into actionable recommendations.

### Business Requirements

| ID | Requirement |
|----|-------------|
| BR-1 | The system must continuously ingest real-time trade data for a configurable set of stock tickers from the Finnhub API. |
| BR-2 | An **Analytics Agent** must run on a set schedule, processing recent trade data to compute key market indicators (price movement, volume trends, momentum). |
| BR-3 | A **News Agent** must retrieve recent news articles relevant to the tracked companies and assess their potential market impact using NLP. |
| BR-4 | The system must combine analytics and news signals to produce an informed recommendation (e.g., bullish / bearish / neutral with a confidence score). |
| BR-5 | Results must be accessible through a REST API so that downstream consumers (dashboards, alerts, further agents) can retrieve them. |
| BR-6 | Each component must be individually evaluable with appropriate metrics (data quality, retrieval relevance, recommendation accuracy, etc.). |

### Success Criteria

- The analytics agent produces accurate trend summaries that reflect actual price movements.
- The news agent retrieves articles relevant to the tracked tickers and correctly assesses sentiment/impact.
- Combined recommendations are coherent — they logically follow from the underlying data and news signals.
- The system runs end-to-end in Docker with a single `docker compose up -d`.

---

## 2. Tech Stack

| Layer | Technology |
|-------|-----------|
| Data source | Finnhub (WebSocket for trades, REST for quotes & news) |
| Message broker | Apache Kafka |
| Stream / batch processing | Apache Spark (PySpark) |
| Storage | Apache Cassandra |
| API | FastAPI |
| NLP / LLM | TBD (agent-based analysis and news summarisation) |
| Orchestration | Docker Compose |

---

## 3. Quick Start

### Prerequisites

- **Docker** and **Docker Compose** installed
- A free API key from [Finnhub](https://finnhub.io/)

### Environment setup

Create a `.env` file in the project root:

```env
FINNHUB_API_KEY=your_finnhub_key
SYMBOLS=AAPL,MSFT,NVDA
```

### Launch

```bash
docker compose up -d
```

On first start, Cassandra and Kafka initialisation may need a couple of minutes. Streaming jobs begin producing results once enough data accumulates in micro-batches.

### API access

After containers are running, open Swagger UI at: http://localhost:8000/docs

### Useful commands

```bash
# Container status
docker compose ps

# Follow streaming logs
docker compose logs -f streaming

# Cassandra shell
docker exec -it cassandra cqlsh
# USE market_analytics; DESCRIBE TABLES;

# Stop (preserves data)
docker compose stop

# Tear down containers
docker compose down
```

---

## 4. CRISP-DM Roadmap

| Phase | Status | Notes |
|-------|--------|-------|
| Business Understanding | Done | This section of the README |
| Data Understanding | Done | See section 5 below |
| Data Preparation | Planned | Kafka ingestion, Spark cleaning and feature extraction |
| Modelling | Planned | Analytics agent (trend indicators), News agent (NLP sentiment/impact) |
| Evaluation | Planned | Per-component metrics and end-to-end recommendation quality |
| Deployment | Planned | Dockerised, single-command startup |

---

## 5. Data Understanding

The system consumes three data streams from the Finnhub API. This section documents each stream's format, quality characteristics, and relevance to our business requirements.

### 5.1 Trade Data (WebSocket) — used by Analytics Agent

**Source:** `wss://ws.finnhub.io` (real-time WebSocket)
**Kafka topic:** `finnhub_trades`

Each tick from Finnhub arrives as:

| Raw field | Type | Description |
|-----------|------|-------------|
| `s` | string | Ticker symbol |
| `p` | float | Trade price |
| `v` | int | Trade volume (shares) |
| `t` | int | Epoch milliseconds |
| `c` | list[string] | Trade condition codes |

The ingestion service normalises this into:

| Normalised field | Type | Derivation |
|------------------|------|------------|
| `symbol` | string | Direct from `s` |
| `price` | float | Direct from `p` |
| `volume` | int | Direct from `v` |
| `notional` | float | `price * volume` |
| `timestamp` | ISO-8601 | `t` converted to UTC datetime |
| `conditions` | list[string] | Direct from `c` |
| `is_dark_pool` | bool | `true` if conditions contain `"41"` or `"8"` |

**Quality notes:**
- High-frequency stream during US market hours (09:30–16:00 ET); sparse or silent outside those hours and on weekends/holidays.
- Volume is per-tick, not aggregated — a single large order may appear as many small ticks.
- Free-tier Finnhub WebSocket delivers data with a slight delay (not true real-time).
- Condition codes are exchange-specific; only dark-pool detection (`"41"`, `"8"`) is currently used.

**Cassandra table:** `trades` (TTL 7 days, partitioned by symbol, clustered by trade_time DESC).

### 5.2 Quote Data (REST polling) — used by Analytics Agent

**Source:** `GET https://finnhub.io/api/v1/quote?symbol={symbol}`
**Kafka topic:** `finnhub_quotes`
**Poll interval:** every ~5 seconds per symbol (rate-limited to Finnhub's 60 calls/min free tier)

Raw Finnhub response:

| Field | Type | Description |
|-------|------|-------------|
| `c` | float | Current price |
| `d` | float | Price change (absolute) |
| `dp` | float | Price change (percent) |
| `h` | float | Day high |
| `l` | float | Day low |
| `o` | float | Day open |
| `pc` | float | Previous close |

The ingestion service currently uses **only the `c` field** and fabricates a synthetic spread (0.02% of price). The `d`, `dp`, `h`, `l`, `o`, `pc` fields are available but unused — these are valuable for the Analytics Agent (day range, open-to-current change).

Normalised message written to Kafka:

| Field | Type | Notes |
|-------|------|-------|
| `symbol` | string | Ticker |
| `bid` | float | `c - spread/2` (synthetic) |
| `ask` | float | `c + spread/2` (synthetic) |
| `bid_size` | int | Always 0 (not available from quote endpoint) |
| `ask_size` | int | Always 0 |
| `spread` | float | `c * 0.0002` (synthetic) |
| `spread_pct` | float | Fixed ~0.02% |
| `timestamp` | ISO-8601 | Time of fetch (not exchange time) |

**Quality notes:**
- Bid/ask and sizes are synthetic — not real order-book data. Spread-based analytics built on this data are estimates only.
- The `d`, `dp`, `h`, `l`, `o`, `pc` fields from Finnhub are discarded during normalisation. These should be preserved for the Analytics Agent.
- Polling frequency is constrained by Finnhub free-tier rate limits (~60 req/min).

**Cassandra table:** `quotes` (TTL 7 days, partitioned by symbol, clustered by quote_time DESC).

### 5.3 Company News (REST) — used by News Agent

**Source:** `GET https://finnhub.io/api/v1/company-news?symbol={symbol}&from={date}&to={date}`
**Kafka topic:** `finnhub_news`
**Poll interval:** every ~5 minutes per cycle (configurable via `NEWS_POLL_INTERVAL`)

Response is an array of articles:

| Field | Type | Description |
|-------|------|-------------|
| `id` | int | Unique article ID |
| `headline` | string | Article title |
| `summary` | string | Short description / lead paragraph |
| `source` | string | Publisher name |
| `url` | string | Link to full article |
| `image` | string | Thumbnail URL |
| `category` | string | News category |
| `datetime` | int | Published time (UNIX timestamp) |
| `related` | string | Comma-separated related tickers |

**Quality notes:**
- Free tier returns recent articles (last ~1 year) filtered by symbol.
- `summary` length and quality vary by source — some are one sentence, others a full paragraph.
- `related` field may list additional tickers the article mentions, useful for cross-company impact detection.
- No built-in sentiment label — sentiment must be inferred by LLM from `headline` + `summary`.

Normalised message written to Kafka:

| Field | Type | Derivation |
|-------|------|------------|
| `symbol` | string | Ticker queried |
| `news_id` | int | Direct from `id` |
| `headline` | string | Direct from `headline` |
| `summary` | string | Direct from `summary` |
| `source` | string | Direct from `source` |
| `url` | string | Direct from `url` |
| `category` | string | Direct from `category` |
| `published_at` | ISO-8601 | `datetime` converted to UTC |
| `related` | string | Direct from `related` |

**Cassandra table:** `company_news` (TTL 7 days, partitioned by symbol, clustered by published_at DESC, news_id DESC).

### 5.4 News Sentiment (REST) — potential complement

**Source:** `GET https://finnhub.io/api/v1/news-sentiment?symbol={symbol}`

Returns pre-computed aggregate sentiment for a company:

| Field | Type | Description |
|-------|------|-------------|
| `buzz.articlesInLastWeek` | int | Number of articles in past week |
| `buzz.weeklyAverage` | float | Average articles per week |
| `buzz.buzz` | float | Buzz score |
| `companyNewsScore` | float | Overall news score |
| `sectorAverageBullishPercent` | float | Sector bullish benchmark |
| `sectorAverageNewsScore` | float | Sector news score benchmark |
| `sentiment.bullishPercent` | float | Bullish sentiment ratio |
| `sentiment.bearishPercent` | float | Bearish sentiment ratio |

**Quality notes:**
- Pre-aggregated by Finnhub — useful as a cross-check against the News Agent's own LLM-derived sentiment, not as a replacement.
- Coarse granularity (weekly aggregates), not per-article.

### 5.5 Data Gaps and Observations

| Issue | Impact | Planned action |
|-------|--------|----------------|
| Quote normalisation discards `d`, `dp`, `h`, `l`, `o`, `pc` | Analytics Agent lacks day-range and open/close context | Extend normalisation to preserve these fields |
| Bid/ask is synthetic (0.02% spread) | Spread analytics are meaningless estimates | Acknowledge limitation; do not base recommendations on spread data |
| ~~No news ingestion yet~~ | ~~News Agent has no data~~ | **Resolved** — company-news poller added to ingestion service, writes to `finnhub_news` topic and `company_news` table |
| No market hours awareness | System processes stale/empty data off-hours | Consider adding market-hours gating or at least flagging |
| Free-tier latency and rate limits | Data is delayed; quote polling is slow with many symbols | Keep symbol list small (3–5 tickers) |
