import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import List

import requests
import websocket
from tenacity import retry, stop_after_attempt, wait_exponential

from kafka_producer import StockKafkaProducer

logger = logging.getLogger(__name__)

FINNHUB_WS_URL = "wss://ws.finnhub.io"
FINNHUB_REST_URL = "https://finnhub.io/api/v1"


class FinnhubClient:

    def __init__(
        self,
        api_key: str,
        symbols: List[str],
        producer: StockKafkaProducer,
        quote_poll_interval: int = 5,
    ):
        self.api_key = api_key
        self.symbols = symbols
        self.producer = producer
        self.quote_poll_interval = quote_poll_interval
        self._ws: websocket.WebSocketApp | None = None
        self._stop_event = threading.Event()


    def _on_open(self, ws):
        logger.info("WebSocket connected; subscribing to %d symbols", len(self.symbols))
        for symbol in self.symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

    def _on_message(self, ws, raw: str):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        if msg.get("type") != "trade":
            return

        for tick in msg.get("data", []):
            trade = self._normalise_trade(tick)
            if trade:
                self.producer.send_trade(trade)

    def _on_error(self, ws, error):
        logger.error("WebSocket error: %s", error)

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning("WebSocket closed (%s): %s", close_status_code, close_msg)

    @staticmethod
    def _normalise_trade(tick: dict) -> dict | None:
        """
        Finnhub trade tick:
          { "p": price, "s": symbol, "t": epoch_ms, "v": volume, "c": [conditions] }
        """
        try:
            price = float(tick["p"])
            volume = int(tick["v"])
            conditions = tick.get("c") or []
            is_dark_pool = isinstance(conditions, list) and (
                "41" in conditions or "8" in conditions
            )
            return {
                "symbol":      tick["s"],
                "price":       price,
                "volume":      volume,
                "notional":    round(price * volume, 4),
                "timestamp":   datetime.fromtimestamp(
                    tick["t"] / 1000, tz=timezone.utc
                ).isoformat(),
                "conditions":  [str(c) for c in conditions],
                "is_dark_pool": is_dark_pool,
            }
        except (KeyError, TypeError, ValueError) as exc:
            logger.debug("Could not normalise tick %s: %s", tick, exc)
            return None

    @retry(
        stop=stop_after_attempt(20),
        wait=wait_exponential(multiplier=2, min=3, max=60),
    )
    def _connect_ws(self):
        url = f"{FINNHUB_WS_URL}?token={self.api_key}"
        self._ws = websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        logger.info("Starting WebSocket run-loop")
        self._ws.run_forever(ping_interval=30, ping_timeout=10)
        if not self._stop_event.is_set():
            raise ConnectionError("WebSocket disconnected; retrying")

    def run_websocket(self):

        self._connect_ws()


    def _fetch_quote(self, symbol: str) -> dict | None:
        try:
            resp = requests.get(
                f"{FINNHUB_REST_URL}/quote",
                params={"symbol": symbol, "token": self.api_key},
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            price = float(data.get("c", 0))
            if price == 0:
                return None
            spread_est = round(price * 0.0002, 4)
            return {
                "symbol":      symbol,
                "bid":         round(price - spread_est / 2, 4),
                "ask":         round(price + spread_est / 2, 4),
                "bid_size":    0,
                "ask_size":    0,
                "spread":      spread_est,
                "spread_pct":  round(spread_est / price * 100, 6),
                "timestamp":   datetime.now(tz=timezone.utc).isoformat(),
            }
        except Exception as exc:
            logger.warning("Quote fetch failed for %s: %s", symbol, exc)
            return None

    def run_quote_poller(self):
        min_gap = max(
            self.quote_poll_interval,
            len(self.symbols) / 55,
        )
        logger.info(
            "Quote poller started: %d symbols, %.1fs gap", len(self.symbols), min_gap
        )
        while not self._stop_event.is_set():
            for symbol in self.symbols:
                if self._stop_event.is_set():
                    break
                quote = self._fetch_quote(symbol)
                if quote:
                    self.producer.send_quote(quote)
                time.sleep(min_gap)

    def stop(self):
        self._stop_event.set()
        if self._ws:
            self._ws.close()
