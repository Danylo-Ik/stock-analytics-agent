#!/bin/bash
echo "Waiting 30s for Kafka and Cassandra..."
sleep 30

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"

SUBMIT="spark-submit \
  --master spark://spark-master:7077 \
  --packages $PACKAGES \
  --conf spark.cassandra.connection.host=cassandra \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.streaming.kafka.consumer.cache.enabled=false \
  --driver-memory 512m \
  --executor-memory 1g \
  --total-executor-cores 1"

echo "Submitting streaming jobs..."

$SUBMIT /app/src/whale_alert.py &
sleep 30

$SUBMIT /app/src/volatility_monitor.py &
sleep 15

$SUBMIT /app/src/market_momentum.py &
sleep 15

$SUBMIT /app/src/spread_monitor.py &

wait