"""
Streaming Job A2 - Volatility Monitor

Every 60 seconds, for each symbol compute the std dev of trade prices over the last 5 minutes.

An alert happens when the current 5-min volatility is more than 2x the
volatility of the previous window.
"""

import os
import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    avg, col, current_timestamp, from_json,
    lit, stddev, struct, to_json, to_timestamp,
    count
)

from utils.spark_utils import (
    TRADE_SCHEMA, KEYSPACE,
    create_spark_session, read_kafka_stream,
    read_from_cassandra, write_to_cassandra,
    read_table_window
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("volatility_monitor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRIGGER_SECS = int(os.getenv("VOLATILITY_TRIGGER_SECS", "60"))
WINDOW_MINUTES = 5
ALERT_RATIO = 2.0
MIN_SAMPLE = 10


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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

        now          = datetime.now(tz=timezone.utc)
        window_start = now.replace(second=0, microsecond=0) - timedelta(minutes=WINDOW_MINUTES)
        prev_start   = window_start - timedelta(minutes=WINDOW_MINUTES)

        all_recent = read_table_window(
            spark, "trades", "trade_time", symbols, window_start
        ).cache()

        current_stats = (
            all_recent.groupBy("symbol")
            .agg(
                stddev("price").alias("current_volatility"),
                avg("price").alias("avg_price"),
                count("*").alias("trade_count")
            )
            .filter((col("trade_count") >= MIN_SAMPLE) & (col("current_volatility") > 0))
        )

        windows_to_save = current_stats.select(
            "symbol", 
            lit(window_start).alias("window_start"),
            col("current_volatility").alias("volatility"),
            "avg_price", 
            "trade_count"
        )
        write_to_cassandra(windows_to_save, "volatility_windows")

        prev_windows = (
            read_from_cassandra(spark, "volatility_windows")
            .filter(
                col("symbol").isin(symbols)
                & (col("window_start") >= lit(prev_start))
                & (col("window_start") < lit(window_start))
            )
            .withColumn("rn", row_number().over(Window.partitionBy("symbol").orderBy(col("window_start").desc())))
            .filter((col("rn") == 1) & (col("volatility") > 0))
            .select("symbol", col("volatility").alias("previous_volatility"))
        )

        w_latest = Window.partitionBy("symbol").orderBy(col("trade_time").desc())
        latest_prices = (
            all_recent.withColumn("rn", row_number().over(w_latest))
            .filter(col("rn") == 1)
            .select("symbol", col("price").alias("current_price"))
        )

        alerts_df = (
            current_stats.join(prev_windows, on="symbol", how="inner")
            .withColumn("ratio", col("current_volatility") / col("previous_volatility"))
            .filter(col("ratio") >= ALERT_RATIO)
            .join(latest_prices, on="symbol", how="left")
            .select(
                col("symbol"),
                current_timestamp().alias("alert_time"),
                lit(window_start).alias("window_start"),
                col("current_volatility"),
                col("previous_volatility"),
                col("ratio"),
                col("current_price").alias("price")
            )
        )

        if not alerts_df.rdd.isEmpty():
            write_to_cassandra(alerts_df, "volatility_alerts")

            (
                alerts_df
                .select(
                    col("symbol").alias("key"),
                    to_json(struct("*")).alias("value"),
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", "alerts_volatility")
                .save()
            )
            
            logger.info("Processed volatility batch: %d alerts triggered.", alerts_df.count())

        all_recent.unpersist()

    return _process


def main():
    spark = create_spark_session("VolatilityMonitor")

    raw = read_kafka_stream(spark, "finnhub_trades")

    trades = (
        raw
        .select(from_json(col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .select(
            col("d.symbol"),
            col("d.price"),
            col("d.volume"),
            col("d.notional"),
            col("d.conditions"),
            col("d.is_dark_pool"),
            to_timestamp(col("d.timestamp")).alias("trade_time"),
        )
        .filter(col("symbol").isNotNull())
    )

    query = (
        trades.writeStream
        .foreachBatch(process_batch(spark))
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .option("checkpointLocation", "/tmp/checkpoints/volatility_monitor")
        .start()
    )

    logger.info("Volatility Monitor streaming job started (trigger=%ds)", TRIGGER_SECS)
    query.awaitTermination()


if __name__ == "__main__":
    main()
