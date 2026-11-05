from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Query

from ..db.cassandra import execute

router = APIRouter(tags=["Reports & Alerts"])


@router.get("/api/reports/hourly")
def get_hourly_report(
    symbol: str = Query(...),
    hours: int = Query(12, ge=1, le=168),
):
    symbol = symbol.upper()
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)

    rows = execute(
        """
        SELECT symbol, hour_start, trade_count, total_volume, total_notional,
               min_price, max_price, avg_price, vwap, volatility,
               buy_volume, sell_volume, dominant_side
        FROM hourly_reports
        WHERE symbol = %s AND hour_start >= %s
        """,
        (symbol, cutoff),
    )

    def _fmt(r):
        return {
            **r,
            "hour_start": r["hour_start"].isoformat() if r["hour_start"] else None,
        }

    return {
        "symbol": symbol,
        "hours":  hours,
        "count":  len(rows),
        "report": [_fmt(r) for r in rows],
    }


@router.get("/api/alerts/whale")
def get_whale_alerts(
    symbol: str = Query(...),
    limit: int = Query(50, ge=1, le=500),
    hours: int = Query(24, ge=1, le=168),
):
    symbol = symbol.upper()
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)

    rows = execute(
        """
        SELECT symbol, alert_time, trade_size, notional, threshold_95p,
               price, deviation_percent, is_dark_pool
        FROM whale_alerts
        WHERE symbol = %s AND alert_time >= %s
        LIMIT %s
        """,
        (symbol, cutoff, limit),
    )

    def _fmt(r):
        return {
            **r,
            "alert_time": r["alert_time"].isoformat() if r["alert_time"] else None,
        }

    return {"symbol": symbol, "count": len(rows), "alerts": [_fmt(r) for r in rows]}


@router.get("/api/alerts/volatility")
def get_volatility_alerts(
    symbol: str = Query(...),
    limit: int = Query(50, ge=1, le=500),
    hours: int = Query(24, ge=1, le=168),
):
    symbol = symbol.upper()
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)

    rows = execute(
        """
        SELECT symbol, alert_time, window_start, current_volatility,
               previous_volatility, ratio, price
        FROM volatility_alerts
        WHERE symbol = %s AND alert_time >= %s
        LIMIT %s
        """,
        (symbol, cutoff, limit),
    )

    def _fmt(r):
        return {
            **r,
            "alert_time":   r["alert_time"].isoformat() if r["alert_time"] else None,
            "window_start": r["window_start"].isoformat() if r.get("window_start") else None,
        }

    return {"symbol": symbol, "count": len(rows), "alerts": [_fmt(r) for r in rows]}
