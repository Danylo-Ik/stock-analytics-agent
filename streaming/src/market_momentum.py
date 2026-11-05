"""
Streaming Job A3 — Market Momentum Dashboard

Every 60 seconds produce a per-symbol snapshot of:
current price (last trade),
price change % vs the previous 1-min window,
volume traded in the last minute,
buy / sell volume split,
VWAP for the window

price > prev_price = Buy
price < prev_price = Sell
price == prev_price = use previous direction

Result saved to cassandra market_momentum table.
"""

import os
import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    avg, col, count, from_json, lag, last, lit,
    row_number, round as spark_round,
    sum as spark_sum, to_timestamp, when,
)

from utils.spark_utils import (
    TRADE_SCHEMA, KEYSPACE,
    create_spark_session, read_kafka_stream,
    read_from_cassandra, write_to_cassandra,
    read_table_window
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("market_momentum")

TRIGGER_SECS = int(os.getenv("MOMENTUM_TRIGGER_SECS", "60"))
WINDOW_MINUTES = 1


def process_batch(spark):
    def _process(df, epoch_id):
        if df.isEmpty():
            return
        
        trades_to_save = df.select(
            "symbol", "trade_time", "price", "volume", 
            "notional", "conditions", "is_dark_pool"
        )
        write_to_cassandra(trades_to_save, "trades")

        now = datetime.now(tz=timezone.utc)
        window_start = now.replace(second=0, microsecond=0) - timedelta(minutes=WINDOW_MINUTES)
        prev_window_start = window_start - timedelta(minutes=WINDOW_MINUTES)

        symbols = [r.symbol for r in df.select("symbol").distinct().collect()]
        if not symbols:
            return

        all_current_trades = (
            read_from_cassandra(spark, "trades")
            .filter(
                col("symbol").isin(symbols) 
                & (col("trade_time") >= lit(window_start.isoformat()))
            )
        )

        if all_current_trades.isEmpty():
            return

        w = Window.partitionBy("symbol").orderBy("trade_time")
        with_direction = (
            all_current_trades
            .withColumn("prev_price", lag("price", 1).over(w))
            .withColumn(
                "direction",
                when(col("prev_price").isNull(), lit("Buy"))
                .when(col("price") > col("prev_price"), lit("Buy"))
                .when(col("price") < col("prev_price"), lit("Sell"))
                .otherwise(lit("Buy"))
            )
        )

        aggs = with_direction.groupBy("symbol").agg(
            last("price").alias("current_price"),
            spark_sum("volume").alias("volume"),
            spark_sum("notional").alias("notional"),
            count("*").alias("trade_count"),
            spark_sum(when(col("direction") == "Buy", col("volume")).otherwise(0)).alias("buy_volume"),
            spark_sum(when(col("direction") == "Sell", col("volume")).otherwise(0)).alias("sell_volume"),
            (spark_sum(col("price") * col("volume")) / spark_sum("volume")).alias("vwap")
        )

        all_prev_windows = (
            read_from_cassandra(spark, "market_momentum")
            .filter(
                col("symbol").isin(symbols)
                & (col("window_start") >= lit(prev_window_start.isoformat()))
                & (col("window_start") < lit(window_start.isoformat()))
            )
            .withColumn("rn", F.row_number().over(Window.partitionBy("symbol").orderBy(col("window_start").desc())))
            .filter(col("rn") == 1)
            .select("symbol", col("current_price").alias("prev_close_price"))
        )

        final_df = (
            aggs.join(all_prev_windows, on="symbol", how="left")
            .withColumn(
                "prev_price", 
                when(col("prev_close_price").isNotNull(), col("prev_close_price")).otherwise(col("current_price"))
            )
            .withColumn(
                "price_change_pct",
                when(col("prev_price") > 0, ((col("current_price") - col("prev_price")) / col("prev_price")) * 100.0)
                .otherwise(lit(0.0))
            )
            .select(
                col("symbol"),
                lit(window_start).alias("window_start"),
                col("current_price").cast("double"),
                col("prev_price").cast("double"),
                spark_round(col("price_change_pct"), 4).alias("price_change_pct"),
                col("volume").cast("long"),
                col("notional").cast("double"),
                col("buy_volume").cast("long"),
                col("sell_volume").cast("long"),
                col("trade_count").cast("int"),
                col("vwap").cast("double")
            )
        )

        write_to_cassandra(final_df, "market_momentum")
        logger.info("Market momentum batch written successfully for %d symbols.", len(symbols))

    return _process


def main():
    spark = create_spark_session("MarketMomentum")

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
        .option("checkpointLocation", "/tmp/checkpoints/market_momentum")
        .start()
    )

    logger.info("Market Momentum streaming job started (trigger=%ds)", TRIGGER_SECS)
    query.awaitTermination()


if __name__ == "__main__":
    main()
