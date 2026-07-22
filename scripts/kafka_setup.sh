#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# kafka_setup.sh
# Helper script: start Zookeeper + Kafka, then create the topic.
# Run this ONCE before starting the producer or Spark job.
# Usage: bash scripts/kafka_setup.sh
# ─────────────────────────────────────────────────────────────────────────────

set -e

KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka}"
TOPIC="twitter-topic"

if [ ! -d "$KAFKA_HOME" ]; then
  echo "[ERROR] KAFKA_HOME not found at $KAFKA_HOME"
  echo "        Set the KAFKA_HOME env variable or edit this script."
  exit 1
fi

echo "============================================"
echo "  Starting Zookeeper ..."
echo "============================================"
# Run in background; logs go to /tmp/zookeeper.log
"$KAFKA_HOME/bin/zookeeper-server-start.sh" \
  "$KAFKA_HOME/config/zookeeper.properties" \
  > /tmp/zookeeper.log 2>&1 &
ZK_PID=$!
echo "[✓] Zookeeper PID: $ZK_PID"

sleep 5   # give ZK time to bind

echo ""
echo "============================================"
echo "  Starting Kafka Broker ..."
echo "============================================"
"$KAFKA_HOME/bin/kafka-server-start.sh" \
  "$KAFKA_HOME/config/server.properties" \
  > /tmp/kafka-broker.log 2>&1 &
KAFKA_PID=$!
echo "[✓] Kafka Broker PID: $KAFKA_PID"

sleep 8   # give broker time to start

echo ""
echo "============================================"
echo "  Creating topic: $TOPIC"
echo "============================================"
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --create \
  --topic "$TOPIC" \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "[✓] Topic '$TOPIC' is ready."
echo ""
echo "Logs:  /tmp/zookeeper.log   /tmp/kafka-broker.log"
echo "Stop:  kill $ZK_PID $KAFKA_PID"
echo ""
echo "Next steps:"
echo "  Terminal 2 → python3 producer/tweet_producer.py"
echo "  Terminal 3 → cd spark-streaming && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 --class sentiment.SentimentAnalysis target/scala-2.12/TwitterSentimentAnalysis-assembly-1.0.jar"
