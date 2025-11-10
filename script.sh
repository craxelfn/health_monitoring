#!/bin/bash

KAFKA_CONTAINER=kafka
TOPICS=("sensor_raw" "processed_data" "anomaly_alert")

for topic in "${TOPICS[@]}"; do
  docker exec -it $KAFKA_CONTAINER kafka-topics --create \
    --topic $topic \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 4
  echo "Created topic $topic"
done
