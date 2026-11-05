"""
Batch Job B1 — Hourly Trading Report

Runs every hour.  For each symbol and each complete hour in the last 12 hours,
computes the trading summary:
trade_count, total_volume, total_notional,
min/max/avg price, VWAP, volatility (std dev),
buy_volume, sell_volume, dominant_side

Results are overwritten into Cassandra hourly_reports.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg, col, count, lag, last, lit, max as spark_max,
    min as spark_min, stddev, sum as spark_sum, when,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("hourly_report")

KEYSPACE = "market_analytics"
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HourlyReport")
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
    report_end = now.replace(minute=0, second=0, microsecond=0)
    report_start = report_end - timedelta(hours=12)

    logger.info(
        "Generating hourly reports for window [%s, %s)",
        report_start.isoformat(), report_end.isoformat(),
    )

    trades = read_table(spark, "trades").filter(
        (col("trade_time") >= lit(report_start.isoformat()))
        & (col("trade_time") < lit(report_end.isoformat()))
    )

    row_count = trades.count()
    if row_count == 0:
        logger.warning("No trades found in window — skipping report.")
        spark.stop()
        return

    logger.info("Processing %d trades", row_count)

    w = Window.partitionBy("symbol").orderBy("trade_time")
    trades = (
        trades
        .withColumn("prev_price", lag("price", 1).over(w))
        .withColumn(
            "direction",
            when(col("prev_price").isNull(), lit("Buy"))
            .when(col("price") > col("prev_price"), lit("Buy"))
            .when(col("price") < col("prev_price"), lit("Sell"))
            .otherwise(lit("Buy")),
        )
    )

    from pyspark.sql.functions import date_trunc
    trades = trades.withColumn(
        "hour_start", date_trunc("hour", col("trade_time"))
    )

    agg = trades.groupBy("symbol", "hour_start").agg(
        count("*").alias("trade_count"),
        spark_sum("volume").alias("total_volume"),
        spark_sum("notional").alias("total_notional"),
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price"),
        avg("price").alias("avg_price"),
        (spark_sum(col("price") * col("volume")) / spark_sum("volume")).alias("vwap"),
        stddev("price").alias("volatility"),
        spark_sum(
            when(col("direction") == "Buy", col("volume")).otherwise(0)
        ).alias("buy_volume"),
        spark_sum(
            when(col("direction") == "Sell", col("volume")).otherwise(0)
        ).alias("sell_volume"),
    )

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    @udf(StringType())
    def dominant(buy, sell):
        if buy is None or sell is None:
            return "Unknown"
        return "Buy" if buy >= sell else "Sell"

    agg = agg.withColumn("dominant_side", dominant(col("buy_volume"), col("sell_volume")))

    write_table(agg, "hourly_reports")
    logger.info("Hourly report written: %d rows", agg.count())
    spark.stop()


if __name__ == "__main__":
    main()
