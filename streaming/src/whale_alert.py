"""
Streaming Job A1 - Whale Alert

Every 30 seconds, for each symbol compute the 95th percentile of trade volumes over the last 10 minutes.
An alert happens when a new trade has volume > 95th percentile of recent trades.

Result saved to cassandra whale_alerts table and also published to alerts_whale_trades Kafka topic.
"""

import os
import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, current_timestamp, from_json, lit,
    struct, to_json, to_timestamp,
)

from utils.spark_utils import (
    TRADE_SCHEMA, KEYSPACE,
    create_spark_session, read_kafka_stream,
    read_from_cassandra, write_to_cassandra,
    read_table_window
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("whale_alert")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRIGGER_SECS    = int(os.getenv("WHALE_ALERT_TRIGGER_SECS", "30"))
WINDOW_MINUTES  = 10
MIN_SAMPLE      = 20


def process_batch(spark):
    def _process(df, epoch_id):
        if df.rdd.isEmpty():
            return

        write_to_cassandra(
            df.select("symbol", "trade_time", "price", "volume",
                      "notional", "conditions", "is_dark_pool"),
            "trades",
        )

        symbols = [r.symbol for r in df.select("symbol").distinct().collect()]
        if not symbols:
            return

        cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=WINDOW_MINUTES)

        recent_all = read_table_window(
            spark, "trades", "trade_time", symbols, cutoff
        ).cache()

        thresholds = (
            recent_all.groupBy("symbol")
            .agg(
                F.count("*").alias("sample_count"),
                F.expr("percentile_approx(volume, 0.95)").alias("threshold_95p")
            )
            .filter(col("sample_count") >= MIN_SAMPLE)
        )

        alerts_df = (
            df.join(thresholds, on="symbol", how="inner")
            .filter(col("volume") > col("threshold_95p"))
            .withColumn(
                "deviation_percent",
                (col("volume").cast("double") - col("threshold_95p")) / col("threshold_95p") * 100.0
            )
            .withColumn("alert_time", current_timestamp())
        )

        if not alerts_df.rdd.isEmpty():
            write_to_cassandra(
                alerts_df.select(
                    "symbol", "alert_time",
                    col("volume").alias("trade_size"),
                    "notional", "threshold_95p",
                    "price", "deviation_percent", "is_dark_pool",
                ),
                "whale_alerts",
            )

            (
                alerts_df.select(
                    col("symbol").alias("key"),
                    to_json(struct(
                        col("alert_time"), col("symbol"),
                        col("volume").alias("trade_size"), col("notional"),
                        col("threshold_95p"), col("price"),
                        col("deviation_percent"), col("is_dark_pool"),
                    )).alias("value"),
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", "alerts_whale_trades")
                .save()
            )

            logger.info("Processed whale batch: %d alerts triggered.", alerts_df.count())

        recent_all.unpersist()

    return _process


def main():
    spark = create_spark_session("WhaleAlert")

    trades = (
        read_kafka_stream(spark, "finnhub_trades")
        .select(from_json(col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .select(
            col("d.symbol"),
            col("d.price"),
            col("d.volume"),
            col("d.notional"),
            to_timestamp(col("d.timestamp")).alias("trade_time"),
            col("d.conditions"),
            col("d.is_dark_pool"),
        )
        .filter(col("symbol").isNotNull())
    )

    query = (
        trades.writeStream
        .foreachBatch(process_batch(spark))
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .option("checkpointLocation", "/tmp/checkpoints/whale_alert")
        .start()
    )

    logger.info("Whale Alert streaming job started (trigger=%ds)", TRIGGER_SECS)
    query.awaitTermination()


if __name__ == "__main__":
    main()