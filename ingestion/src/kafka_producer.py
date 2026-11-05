import json
import logging
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logger = logging.getLogger(__name__)

class StockKafkaProducer:
    def __init__(self, bootstrap_servers: str):
        self.producer = self._connect(bootstrap_servers)

    def _connect(self, bootstrap_servers: str) -> KafkaProducer:
        retries = 10
        while retries > 0:
            try:
                logger.info(f"Connecting to Kafka at {bootstrap_servers}...")
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks=1
                )
                logger.info("Kafka connection successful.")
                return producer
            except NoBrokersAvailable:
                logger.warning("Retrying in 5 seconds...")
                time.sleep(5)
                retries -= 1
        raise Exception("Failed to connect to Kafka.")

    def send_trade(self, trade: dict):
        self.producer.send("finnhub_trades", value=trade)

    def send_quote(self, quote: dict):
        self.producer.send("finnhub_quotes", value=quote)

    def close(self):
        if self.producer:
            logger.info("Flushing and closing Kafka producer.")
            self.producer.flush()
            self.producer.close()