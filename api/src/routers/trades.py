from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query

from ..db.cassandra import execute

router = APIRouter(prefix="/api/trades", tags=["Trade Lookup"])


@router.get("")
def get_trades(
    symbol: str = Query(..., description="Ticker symbol, e.g. AAPL"),
    min_size: Optional[int] = Query(None, description="Minimum trade volume (shares)"),
    min_notional: Optional[float] = Query(None, description="Minimum notional (USD)"),
    dark_pool: Optional[bool] = Query(None, description="Filter by dark-pool flag"),
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    limit: int = Query(100, ge=1, le=1000),
):
    symbol = symbol.upper()

    try:
        now = datetime.now(tz=timezone.utc)
        ts_from = datetime.fromisoformat(from_ts) if from_ts else now - timedelta(hours=1)
        ts_to = datetime.fromisoformat(to_ts) if to_ts else now
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {exc}")

    rows = execute(
        """
        SELECT symbol, trade_time, price, volume, notional,
               conditions, is_dark_pool
        FROM trades
        WHERE symbol = %s
          AND trade_time >= %s
          AND trade_time <= %s
        LIMIT 10000
        """,
        (symbol, ts_from, ts_to),
    )

    if min_size is not None:
        rows = [r for r in rows if r["volume"] >= min_size]
    if min_notional is not None:
        rows = [r for r in rows if (r["notional"] or 0) >= min_notional]
    if dark_pool is not None:
        rows = [r for r in rows if bool(r.get("is_dark_pool")) == dark_pool]

    prev_price: Optional[float] = None
    enriched = []
    for r in rows[:limit]:
        price = r["price"]
        if prev_price is None:
            side = "Buy"
        elif price > prev_price:
            side = "Buy"
        elif price < prev_price:
            side = "Sell"
        else:
            side = enriched[-1]["side"] if enriched else "Buy"
        prev_price = price

        enriched.append({
            "symbol":      r["symbol"],
            "trade_time":  r["trade_time"].isoformat() if r["trade_time"] else None,
            "price":       r["price"],
            "volume":      r["volume"],
            "notional":    r["notional"],
            "side":        side,
            "conditions":  r.get("conditions") or [],
            "is_dark_pool": bool(r.get("is_dark_pool")),
        })

    return {
        "symbol":  symbol,
        "from":    ts_from.isoformat(),
        "to":      ts_to.isoformat(),
        "count":   len(enriched),
        "trades":  enriched,
    }
