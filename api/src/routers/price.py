from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from ..db.cassandra import execute

router = APIRouter(prefix="/api/price", tags=["Price History"])

VALID_INTERVALS = {"1m": 1, "5m": 5, "1h": 60}


@router.get("/{symbol}")
def get_price_history(
    symbol: str,
    interval: Literal["1m", "5m", "1h"] = "1m",
    from_ts: str = Query(
        default=None,
        alias="from",
        description="ISO-8601 start timestamp (default: 1 hour ago)",
    ),
    to_ts: str = Query(
        default=None,
        alias="to",
        description="ISO-8601 end timestamp (default: now)",
    ),
):
    symbol = symbol.upper()
    bucket_minutes = VALID_INTERVALS[interval]

    try:
        now = datetime.now(tz=timezone.utc)
        ts_from = datetime.fromisoformat(from_ts) if from_ts else now - timedelta(hours=1)
        ts_to = datetime.fromisoformat(to_ts) if to_ts else now
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {exc}")

    if ts_from >= ts_to:
        raise HTTPException(status_code=400, detail="'from' must be before 'to'")

    rows = execute(
        """
        SELECT trade_time, price, volume, notional
        FROM trades
        WHERE symbol = %s
          AND trade_time >= %s
          AND trade_time <= %s
        """,
        (symbol, ts_from, ts_to),
    )

    if not rows:
        return {"symbol": symbol, "interval": interval, "candles": []}

    from collections import defaultdict
    import math

    buckets: dict[datetime, list] = defaultdict(list)
    for row in rows:
        t: datetime = row["trade_time"]
        epoch = t.timestamp()
        bucket_epoch = math.floor(epoch / (bucket_minutes * 60)) * (bucket_minutes * 60)
        bucket_dt = datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)
        buckets[bucket_dt].append(row)

    candles = []
    for bucket_dt in sorted(buckets):
        bucket_rows = buckets[bucket_dt]
        prices = [r["price"] for r in bucket_rows]
        volumes = [r["volume"] for r in bucket_rows]
        notionals = [r["notional"] for r in bucket_rows]
        total_vol = sum(volumes)
        vwap = sum(notionals) / total_vol if total_vol else None

        candles.append({
            "time":   bucket_dt.isoformat(),
            "open":   prices[0],
            "high":   max(prices),
            "low":    min(prices),
            "close":  prices[-1],
            "volume": total_vol,
            "vwap":   round(vwap, 4) if vwap else None,
        })

    return {
        "symbol":   symbol,
        "interval": interval,
        "from":     ts_from.isoformat(),
        "to":       ts_to.isoformat(),
        "count":    len(candles),
        "candles":  candles,
    }
