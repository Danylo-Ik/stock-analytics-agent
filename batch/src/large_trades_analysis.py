"""
Batch Job B3 — Large Trades Analysis

For a past window (default 24 h):
find all trades above the 90th percentile by volume for each symbol,
for each large trade, look up the average price 5 minutes after the trade and compute the price impact percentage,
write results to Cassandra large_trades_analysis.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg, col, lag, lit, udf, when,
)
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("large_trades_analysis")

KEYSPACE = "market_analytics"
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
LOOKBACK_HOURS = int(os.getenv("LARGE_TRADES_LOOKBACK_HOURS", "24"))
IMPACT_WINDOW_MINUTES = 5
PERCENTILE_THRESHOLD = 0.90


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("LargeTradesAnalysis")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_table(spark, table):
    return (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=KEYSPACE)
        .load()
    )


def write_table(df, table):
    (
        df.write.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=KEYSPACE)
        .mode("append")
        .save()
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    now = datetime.now(tz=timezone.utc)
    lookback_start = now - timedelta(hours=LOOKBACK_HOURS)

    logger.info(
        "Large trades analysis: window [%s, %s]",
        lookback_start.isoformat(), now.isoformat(),
    )

    trades = read_table(spark, "trades").filter(
        col("trade_time") >= lit(lookback_start.isoformat())
    ).cache()

    if trades.rdd.isEmpty():
        logger.warning("No trades in lookback window — skipping.")
        spark.stop()
        return

    symbols = [r.symbol for r in trades.select("symbol").distinct().collect()]
    all_results = []

    for symbol in symbols:
        sym_trades = trades.filter(col("symbol") == symbol)
        n = sym_trades.count()
        if n < 20:
            logger.info("%s: too few trades (%d) — skipping", symbol, n)
            continue

        threshold = sym_trades.approxQuantile("volume", [PERCENTILE_THRESHOLD], 0.05)[0]
        large = sym_trades.filter(col("volume") >= lit(threshold))

        large_rows = large.select(
            "symbol", "trade_time", "price", "volume", "notional", "is_dark_pool"
        ).collect()

        logger.info(
            "%s: threshold=%.0f, large_trades=%d / %d",
            symbol, threshold, len(large_rows), n,
        )

        for row in large_rows:
            trade_time = row["trade_time"]
            if trade_time is None:
                continue

            impact_start = trade_time
            impact_end = trade_time + timedelta(minutes=IMPACT_WINDOW_MINUTES)

            post_trades = (
                sym_trades
                .filter(
                    (col("trade_time") > lit(impact_start.isoformat()))
                    & (col("trade_time") <= lit(impact_end.isoformat()))
                )
                .agg(avg("price").alias("avg_post_price"))
                .collect()
            )

            avg_post = (
                float(post_trades[0]["avg_post_price"])
                if post_trades and post_trades[0]["avg_post_price"]
                else None
            )

            trade_price = float(row["price"])
            price_impact_pct = (
                round((avg_post - trade_price) / trade_price * 100.0, 6)
                if avg_post and trade_price
                else None
            )

            all_results.append((
                symbol,
                datetime.now(tz=timezone.utc),
                trade_time,
                trade_price,
                int(row["volume"]),
                float(row["notional"]),
                avg_post,
                price_impact_pct,
                bool(row["is_dark_pool"]) if row["is_dark_pool"] is not None else False,
                round(PERCENTILE_THRESHOLD * 100, 0),
            ))

    if not all_results:
        logger.warning("No large trades found across all symbols.")
        spark.stop()
        return

    result_df = spark.createDataFrame(
        all_results,
        [
            "symbol", "analysis_time", "trade_time",
            "trade_price", "trade_size", "trade_notional",
            "price_5m_after", "price_impact_pct",
            "is_dark_pool", "percentile_rank",
        ],
    )

    write_table(result_df, "large_trades_analysis")
    logger.info("Large trades analysis written: %d rows", len(all_results))
    spark.stop()


if __name__ == "__main__":
    main()
