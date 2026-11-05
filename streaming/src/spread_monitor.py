"""
Streaming Job A4 - Spread Monitor

Reads the finnhub_quotes Kafka topic produced by the REST poller in the
ingestion service.

Every 30 seconds save raw quotes to Cassandra quotes table, 
compute 1-minute aggregated spread metrics and save to spread_history table,
alert when current average spread > 3x the 10-minute baseline
"""

import os
import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql.functions import (
    avg, col, count, from_json, lit, max as spark_max,
    min as spark_min, to_timestamp,
)

from utils.spark_utils import (
    QUOTE_SCHEMA,
    create_spark_session, read_kafka_stream,
    read_table_window, write_to_cassandra,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spread_monitor")

TRIGGER_SECS = int(os.getenv("SPREAD_TRIGGER_SECS", "30"))
BASELINE_MINUTES = 10
ALERT_MULTIPLIER = 3.0


def process_batch(spark):
    def _process(df, epoch_id):
        if df.rdd.isEmpty():
            return

        write_to_cassandra(
            df.select("symbol", "quote_time", "bid", "ask",
                      "bid_size", "ask_size", "spread", "spread_pct"),
            "quotes",
        )

        symbols = [r.symbol for r in df.select("symbol").distinct().collect()]
        if not symbols:
            return

        now             = datetime.now(tz=timezone.utc)
        window_start    = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        baseline_cutoff = now - timedelta(minutes=BASELINE_MINUTES)

        recent_quotes_all = read_table_window(
            spark, "quotes", "quote_time", symbols, window_start
        ).cache()

        baseline_spreads_all = read_table_window(
            spark, "spread_history", "window_start", symbols, baseline_cutoff,
            to_dt=window_start,
        ).cache()

        current_spreads = (
            recent_quotes_all.groupBy("symbol")
            .agg(
                avg("spread").alias("avg_spread"),
                spark_min("spread").alias("min_spread"),
                spark_max("spread").alias("max_spread"),
                avg("spread_pct").alias("avg_spread_pct"),
                count("*").alias("sample_count"),
            )
            .filter(col("avg_spread") > 0)
        )

        if current_spreads.rdd.isEmpty():
            recent_quotes_all.unpersist()
            baseline_spreads_all.unpersist()
            return

        window_df = current_spreads.select(
            "symbol",
            lit(window_start).alias("window_start"),
            "avg_spread", "min_spread", "max_spread",
            "avg_spread_pct", "sample_count"
        )
        write_to_cassandra(window_df, "spread_history")

        baseline_aggs = (
            baseline_spreads_all.groupBy("symbol")
            .agg(avg("avg_spread").alias("baseline"))
            .filter(col("baseline") > 0)
        )

        alerts = (
            current_spreads.join(baseline_aggs, on="symbol", how="inner")
            .withColumn("ratio", col("avg_spread") / col("baseline"))
            .filter(col("ratio") >= ALERT_MULTIPLIER)
            .collect() # Only collect the actual alerts that triggered
        )

        for row in alerts:
            logger.warning(
                "SPREAD ALERT %s: avg_spread=%.4f is %.1f× baseline=%.4f",
                row["symbol"], row["avg_spread"], row["ratio"], row["baseline"]
            )

        recent_quotes_all.unpersist()
        baseline_spreads_all.unpersist()

    return _process


def main():
    spark = create_spark_session("SpreadMonitor")

    raw = read_kafka_stream(spark, "finnhub_quotes")

    quotes = (
        raw
        .select(from_json(col("value").cast("string"), QUOTE_SCHEMA).alias("d"))
        .select(
            col("d.symbol"),
            col("d.bid"),
            col("d.ask"),
            col("d.bid_size"),
            col("d.ask_size"),
            col("d.spread"),
            col("d.spread_pct"),
            to_timestamp(col("d.timestamp")).alias("quote_time"),
        )
        .filter(col("symbol").isNotNull())
    )

    query = (
        quotes.writeStream
        .foreachBatch(process_batch(spark))
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .option("checkpointLocation", "/tmp/checkpoints/spread_monitor")
        .start()
    )

    logger.info("Spread Monitor streaming job started (trigger=%ds)", TRIGGER_SECS)
    query.awaitTermination()


if __name__ == "__main__":
    main()
