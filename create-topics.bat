@echo off
setlocal enabledelayedexpansion

set KAFKA_CONTAINER=kafka
set TOPICS=sensor_raw processed_data anomaly_alert

for %%t in (%TOPICS%) do (
  docker exec -i %KAFKA_CONTAINER% kafka-topics --create ^
    --topic %%t ^
    --bootstrap-server kafka:9092 ^
    --replication-factor 1 ^
    --partitions 4
  echo Created topic %%t
)

echo All topics created successfully!
pause