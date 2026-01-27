#!/bin/bash

echo "🚀 Flink Job Auto-Submit Script Started"
echo "⏳ Waiting for Flink Job Manager to be ready..."

# Wait for Flink to be fully started
sleep 20

echo "⏳ Waiting for Kafka to be available..."

# Wait for Kafka to be ready (try for max 60 seconds)
COUNTER=0
until docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:9092 > /dev/null 2>&1; do
  echo "⏳ Kafka not ready yet, waiting 5 seconds... (attempt $COUNTER)"
  sleep 5
  COUNTER=$((COUNTER + 1))
  if [ $COUNTER -eq 12 ]; then
    echo "❌ Kafka failed to start after 60 seconds"
    exit 1
  fi
done

echo "✅ Kafka is ready!"
echo "📤 Submitting Flink whale detector job..."

# Submit the Flink job (file is already in the image at /opt/flink/whale_detector.py)
docker exec flink-jobmanager flink run -py /opt/flink/whale_detector.py

if [ $? -eq 0 ]; then
  echo "✅ Flink job submitted successfully!"
else
  echo "❌ Failed to submit Flink job"
  exit 1
fi

echo "🎉 Auto-submit completed!"



