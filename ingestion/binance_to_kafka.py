import json
import websocket
from kafka import KafkaProducer
import time
import os

# Get Kafka broker from environment variable (defaults to kafka:9092 for Docker)
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')

# Initialize Kafka Producer
producer = KafkaProducer(
     bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # Wait for all replicas to acknowledge
    retries=3    # Retry failed sends
)

# Statistics tracking
stats = {
    'total_trades': 0,
    'whale_trades': 0,
    'last_print': time.time()
}

def on_message(ws, message):
    """Process incoming trade from Binance and send to Kafka"""
    try:
        parsed = json.loads(message)

        # Combined streams wrap data in 'data' field
        if 'data' in parsed:
            data = parsed['data']
        else:
            data = parsed

        # Extract trade information
        symbol = data['s']
        price = float(data['p'])
        quantity = float(data['q'])
        trade_time = data['T']
        is_buyer_maker = data['m']

        # Calculate trade value
        trade_value = price * quantity

        # Determine trade side
        side = 'SELL' if is_buyer_maker else 'BUY'

        # Create enriched message for Kafka
        kafka_message = {
            'symbol': symbol,
            'price': price,
            'quantity': quantity,
            'value': trade_value,
            'side': side,
            'timestamp': trade_time,
            'is_whale': trade_value > 500000  # Flag whale trades
        }

        # Send to Kafka
        producer.send('crypto-trades', value=kafka_message)

        # Update statistics
        stats['total_trades'] += 1
        if trade_value > 500000:
            stats['whale_trades'] += 1
            print(f"🐋 WHALE DETECTED! {symbol} ${trade_value:,.2f}")

        # Print stats every 10 seconds
        current_time = time.time()
        if current_time - stats['last_print'] >= 10:
            print(f"📊 Stats: {stats['total_trades']} trades | {stats['whale_trades']} whales | {stats['total_trades']/10:.1f} trades/sec")
            stats['last_print'] = current_time

    except Exception as e:
        print(f"❌ Error processing message: {e}")

def on_error(ws, error):
    print(f"❌ WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("🔌 WebSocket connection closed")
    producer.close()

def on_open(ws):
    print("✅ Connected to Binance WebSocket")
    print("✅ Kafka Producer ready")
    print("🚀 Streaming trades to Kafka topic: crypto-trades")
    print("=" * 60)

if __name__ == "__main__":
    # Monitor BTC, ETH, BNB
    symbols = ["btcusdt", "ethusdt", "bnbusdt"]
    streams = "/".join([f"{symbol}@trade" for symbol in symbols])
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    print("🚀 Starting Binance → Kafka Pipeline")
    print(f"📊 Monitoring: {', '.join([s.upper() for s in symbols])}")
    print(f"🎯 Whale threshold: $500,000")

    ws = websocket.WebSocketApp(
        socket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()