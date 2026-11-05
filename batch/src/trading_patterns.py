"""
Batch Job B2 — Trading Patterns Analysis

Analyses all accumulated hourly_reports data to build a
report of day behaviour, grouped by hour

avg_trade_count — typical activity level
avg_volume — typical volume
avg_volatility — typical price std dev
avg_spread_pct — typical bid-ask spread (from spread_history)
sample_days — how many days of data used
"""

import logging
import os
from datetime import timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, hour, lit, round as spark_round,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("trading_patterns")

KEYSPACE = "market_analytics"
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TradingPatterns")
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

    reports = read_table(spark, "hourly_reports")
    if reports.rdd.isEmpty():
        logger.warning("No hourly_reports data yet — skipping.")
        spark.stop()
        return

    reports = reports.withColumn("hour_of_day", hour(col("hour_start")))

    patterns = reports.groupBy("symbol", "hour_of_day").agg(
        avg("trade_count").alias("avg_trade_count"),
        avg("total_volume").alias("avg_volume"),
        avg("volatility").alias("avg_volatility"),
        count("*").alias("sample_days"),
    )

    spread_hist = read_table(spark, "spread_history")

    if not spread_hist.rdd.isEmpty():
        spread_by_hour = (
            spread_hist
            .withColumn("hour_of_day", hour(col("window_start")))
            .groupBy("symbol", "hour_of_day")
            .agg(avg("avg_spread_pct").alias("avg_spread_pct"))
        )
        patterns = patterns.join(spread_by_hour, on=["symbol", "hour_of_day"], how="left")
    else:
        from pyspark.sql.functions import lit as flit
        patterns = patterns.withColumn("avg_spread_pct", flit(None).cast("double"))

    from pyspark.sql.functions import current_timestamp
    patterns = (
        patterns
        .withColumn("avg_trade_count", spark_round("avg_trade_count", 2))
        .withColumn("avg_volume",      spark_round("avg_volume", 0))
        .withColumn("avg_volatility",  spark_round("avg_volatility", 6))
        .withColumn("avg_spread_pct",  spark_round("avg_spread_pct", 6))
        .withColumn("updated_at",      current_timestamp())
    )

    write_table(patterns, "trading_patterns")
    row_count = patterns.count()
    logger.info("Trading patterns written: %d rows", row_count)
    spark.stop()


if __name__ == "__main__":
    main()
