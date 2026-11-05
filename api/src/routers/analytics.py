from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db.cassandra import execute

router = APIRouter(tags=["Analytics"])


@router.get("/api/analytics/trading-patterns")
def get_trading_patterns(symbol: str = Query(...)):
    symbol = symbol.upper()

    rows = execute(
        """
        SELECT symbol, hour_of_day, avg_trade_count, avg_volume,
               avg_volatility, avg_spread_pct, sample_days, updated_at
        FROM trading_patterns
        WHERE symbol = %s
        """,
        (symbol,),
    )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No trading pattern data for {symbol}. "
                   "The batch job needs at least a few hours of data.",
        )

    rows_sorted = sorted(rows, key=lambda r: r["hour_of_day"])

    peak_volume_hour = max(rows_sorted, key=lambda r: r["avg_volume"] or 0)
    peak_vol_hour = max(rows_sorted, key=lambda r: r["avg_volatility"] or 0)

    return {
        "symbol":            symbol,
        "updated_at":        rows_sorted[0].get("updated_at"),
        "peak_volume_hour":  peak_volume_hour["hour_of_day"],
        "peak_volatility_hour": peak_vol_hour["hour_of_day"],
        "by_hour":           rows_sorted,
    }


@router.get("/api/analytics/whale-impact")
def get_whale_impact(
    symbol: str = Query(...),
    period: str = Query("24h", description="Lookback period: 4h, 24h, 48h"),
):
    symbol = symbol.upper()

    hours_map = {"4h": 4, "24h": 24, "48h": 48}
    if period not in hours_map:
        raise HTTPException(status_code=400, detail=f"period must be one of {list(hours_map)}")

    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours_map[period])

    rows = execute(
        """
        SELECT symbol, analysis_time, trade_time, trade_price, trade_size,
               trade_notional, price_5m_after, price_impact_pct,
               is_dark_pool, percentile_rank
        FROM large_trades_analysis
        WHERE symbol = %s AND analysis_time >= %s
        """,
        (symbol, cutoff),
    )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No large-trade analysis for {symbol} in the last {period}.",
        )

    impacts = [r["price_impact_pct"] for r in rows if r["price_impact_pct"] is not None]
    avg_impact = round(sum(impacts) / len(impacts), 4) if impacts else None
    positive = sum(1 for x in impacts if x > 0)
    negative = sum(1 for x in impacts if x < 0)

    def _fmt(r):
        return {
            **r,
            "analysis_time": r["analysis_time"].isoformat() if r["analysis_time"] else None,
            "trade_time":    r["trade_time"].isoformat() if r["trade_time"] else None,
        }

    return {
        "symbol":            symbol,
        "period":            period,
        "large_trade_count": len(rows),
        "avg_price_impact_pct": avg_impact,
        "positive_impact_count": positive,
        "negative_impact_count": negative,
        "trades":            [_fmt(r) for r in rows],
    }


@router.get("/api/spread/{symbol}")
def get_spread_history(
    symbol: str,
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
):
    symbol = symbol.upper()
    try:
        now = datetime.now(tz=timezone.utc)
        ts_from = datetime.fromisoformat(from_ts) if from_ts else now - timedelta(hours=1)
        ts_to = datetime.fromisoformat(to_ts) if to_ts else now
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    rows = execute(
        """
        SELECT symbol, window_start, avg_spread, min_spread,
               max_spread, avg_spread_pct, sample_count
        FROM spread_history
        WHERE symbol = %s AND window_start >= %s AND window_start <= %s
        """,
        (symbol, ts_from, ts_to),
    )

    def _fmt(r):
        return {**r, "window_start": r["window_start"].isoformat() if r["window_start"] else None}

    return {
        "symbol": symbol,
        "from":   ts_from.isoformat(),
        "to":     ts_to.isoformat(),
        "count":  len(rows),
        "spread": [_fmt(r) for r in rows],
    }


@router.get("/api/momentum")
def get_momentum(
    symbol: str = Query(...),
    limit: int = Query(60, ge=1, le=1440),
):
    symbol = symbol.upper()

    rows = execute(
        """
        SELECT symbol, window_start, current_price, prev_price,
               price_change_pct, volume, notional, buy_volume,
               sell_volume, trade_count, vwap
        FROM market_momentum
        WHERE symbol = %s
        LIMIT %s
        """,
        (symbol, limit),
    )

    def _fmt(r):
        return {**r, "window_start": r["window_start"].isoformat() if r["window_start"] else None}

    return {"symbol": symbol, "count": len(rows), "momentum": [_fmt(r) for r in rows]}
