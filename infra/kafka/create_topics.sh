#!/bin/bash

echo "Creating Kafka topics..."

kafka-topics.sh --create --topic finnhub_trades --bootstrap-server kafka:9092 --if-not-exists
kafka-topics.sh --create --topic finnhub_quotes --bootstrap-server kafka:9092 --if-not-exists
kafka-topics.sh --create --topic finnhub_news --bootstrap-server kafka:9092 --if-not-exists

kafka-topics.sh --create --topic alerts_whale_trades --bootstrap-server kafka:9092 --if-not-exists
kafka-topics.sh --create --topic alerts_volatility --bootstrap-server kafka:9092 --if-not-exists
kafka-topics.sh --create --topic alerts_spread --bootstrap-server kafka:9092 --if-not-exists

echo "All Kafka topics created successfully."