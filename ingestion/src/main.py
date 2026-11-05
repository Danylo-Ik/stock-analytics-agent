"""
Finnhub WebSocket  -> trade events -> Kafka trades-raw
Finnhub REST poller -> quote snapshots -> Kafka quotes-raw
"""

import logging
import os
import signal
import threading

from dotenv import load_dotenv

from finnhub_client import FinnhubClient
from kafka_producer import StockKafkaProducer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("ingestion")


def main():
    api_key = os.environ["FINNHUB_API_KEY"]
    symbols = [s.strip() for s in os.environ.get("SYMBOLS", "AAPL,MSFT").split(",")]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    poll_interval = int(os.environ.get("QUOTE_POLL_INTERVAL", "5"))

    logger.info("Starting ingestion for symbols: %s", symbols)

    producer = StockKafkaProducer(bootstrap_servers=bootstrap)
    client = FinnhubClient(
        api_key=api_key,
        symbols=symbols,
        producer=producer,
        quote_poll_interval=poll_interval,
    )

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received")
        client.stop()
        producer.close()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    quote_thread = threading.Thread(
        target=client.run_quote_poller, name="quote-poller", daemon=True
    )
    quote_thread.start()

    client.run_websocket()
    producer.close()
    logger.info("Ingestion service stopped")


if __name__ == "__main__":
    main()
