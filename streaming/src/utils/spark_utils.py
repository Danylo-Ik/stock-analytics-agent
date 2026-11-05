import os
from datetime import datetime
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, LongType,
    StringType, StructField, StructType,
)

TRADE_SCHEMA = StructType([
    StructField("symbol",       StringType(),            nullable=False),
    StructField("price",        DoubleType(),            nullable=False),
    StructField("volume",       LongType(),              nullable=False),
    StructField("notional",     DoubleType(),            nullable=True),
    StructField("timestamp",    StringType(),            nullable=False),
    StructField("conditions",   ArrayType(StringType()), nullable=True),
    StructField("is_dark_pool", BooleanType(),           nullable=True),
])

QUOTE_SCHEMA = StructType([
    StructField("symbol",     StringType(), nullable=False),
    StructField("bid",        DoubleType(), nullable=True),
    StructField("ask",        DoubleType(), nullable=True),
    StructField("bid_size",   LongType(),   nullable=True),
    StructField("ask_size",   LongType(),   nullable=True),
    StructField("spread",     DoubleType(), nullable=True),
    StructField("spread_pct", DoubleType(), nullable=True),
    StructField("timestamp",  StringType(), nullable=False),
])

KEYSPACE = "market_analytics"


def create_spark_session(app_name: str) -> SparkSession:
    cassandra_host = os.getenv("CASSANDRA_HOST", "cassandra")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.cassandra.connection.host", cassandra_host)
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.cassandra.connection.localDC", "datacenter1")
        .config("spark.cassandra.output.consistency.level", "LOCAL_ONE")
        .config("spark.cassandra.connection.timeoutMS", "10000")
        .config("spark.cassandra.read.timeoutMS", "30000")
        .config("spark.cassandra.connection.reconnectionDelayMS.min", "1000")
        .config("spark.cassandra.connection.reconnectionDelayMS.max", "10000")
        .config("spark.sql.streaming.checkpointLocation", f"/tmp/checkpoints/{app_name}")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )


def write_to_cassandra(df: DataFrame, table: str, mode: str = "append") -> None:
    (
        df.coalesce(1).write.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=KEYSPACE)
        .option("spark.cassandra.output.concurrent.writes", "1")
        .option("spark.cassandra.output.batch.size.rows", "25")
        .mode(mode)
        .save()
    )


def read_from_cassandra(spark: SparkSession, table: str) -> DataFrame:
    return (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=KEYSPACE)
        .load()
    )


def read_table_window(
    spark: SparkSession,
    table: str,
    time_col: str,
    symbols: List[str],
    from_dt: datetime,
    to_dt: Optional[datetime] = None,
) -> DataFrame:

    df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=KEYSPACE)
        .load()
        .filter(col("symbol").isin(symbols))
        .filter(col(time_col) >= lit(from_dt))
    )
    if to_dt is not None:
        df = df.filter(col(time_col) <= lit(to_dt))
    return df
