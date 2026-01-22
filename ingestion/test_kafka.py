from kafka import KafkaProducer
import json
import time

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert dict to JSON
)

print("🚀 Testing Kafka connection...")

# Send a test message
test_message = {
    'symbol': 'BTCUSDT',
    'price': 97450.00,
    'quantity': 0.025,
    'value': 2436.25,
    'timestamp': int(time.time() * 1000),
    'test': True
}

try:
    # Send to 'crypto-trades' topic
    future = producer.send('crypto-trades', value=test_message)

    # Wait for confirmation
    result = future.get(timeout=10)

    print(f"✅ Message sent successfully!")
    print(f"   Topic: {result.topic}")
    print(f"   Partition: {result.partition}")
    print(f"   Offset: {result.offset}")

except Exception as e:
    print(f"❌ Error sending message: {e}")

finally:
    producer.close()
    print("🔌 Producer closed")