"""
Stock Analytics Platform - REST API

  Ad-hoc queries
    GET /api/price/{symbol}?from=&to=&interval=1m|5m|1h
    GET /api/trades?symbol=&min_size=&dark_pool=&from=&to=&limit=
    GET /api/spread/{symbol}?from=&to=
    GET /api/momentum?symbol=&limit=

  Batch reports
    GET /api/reports/hourly?symbol=&hours=

  Analytics (batch-computed)
    GET /api/analytics/trading-patterns?symbol=
    GET /api/analytics/whale-impact?symbol=&period=24h

  Alerts (streaming-generated)
    GET /api/alerts/whale?symbol=&limit=&hours=
    GET /api/alerts/volatility?symbol=&limit=&hours=

  Health
    GET /health
"""

import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import analytics, price, reports, trades

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

app = FastAPI(
    title="Stock Analytics Platform",
    description="Real-time and historical stock market analytics powered by Kafka, Spark & Cassandra.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(price.router)
app.include_router(trades.router)
app.include_router(reports.router)
app.include_router(analytics.router)


@app.get("/health", tags=["Health"])
def health_check():
    from .db.cassandra import get_session
    try:
        session = get_session()
        session.execute("SELECT now() FROM system.local")
        cassandra_ok = True
    except Exception as exc:
        cassandra_ok = False

    return {
        "status":    "ok" if cassandra_ok else "degraded",
        "cassandra": "connected" if cassandra_ok else "unreachable",
    }


@app.get("/", include_in_schema=False)
def root():
    return {"message": "Stock Analytics API — visit /docs"}
