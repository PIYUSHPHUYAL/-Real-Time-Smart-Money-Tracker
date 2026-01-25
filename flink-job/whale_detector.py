import json
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy
import psycopg2
from datetime import datetime


# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'crypto_tracker'),
    'user': os.getenv('POSTGRES_USER', 'crypto_admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'crypto_secure_pass_123')
}

# Whale threshold
WHALE_THRESHOLD = float(os.getenv('WHALE_THRESHOLD', '500000'))

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)

def write_alert_to_db(alert):
    """Write alert to PostgreSQL"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO alerts (symbol, alert_type, price, trade_value, message, details)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            alert['symbol'],
            alert['alert_type'],
            alert['price'],
            alert['trade_value'],
            alert['message'],
            json.dumps(alert['details'])
        ))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Alert saved: {alert['message']}")
        return True

    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def process_trade(trade_json):
    """Process incoming trade and detect whales"""
    try:
        trade = json.loads(trade_json)

        # Check if it's a whale trade
        if trade['value'] >= WHALE_THRESHOLD:
            alert = {
                'symbol': trade['symbol'],
                'alert_type': 'WHALE_TRADE',
                'price': trade['price'],
                'trade_value': trade['value'],
                'message': f"🐋 Whale {trade['side']}: {trade['symbol']} ${trade['value']:,.2f}",
                'details': {
                    'quantity': trade['quantity'],
                    'side': trade['side'],
                    'timestamp': trade['timestamp']
                }
            }

            # Write to database
            write_alert_to_db(alert)

    except Exception as e:
        print(f"❌ Error processing trade: {e}")

def main():
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    print("🚀 Starting Flink Whale Detector...")
    print(f"🎯 Whale threshold: ${WHALE_THRESHOLD:,.0f}")

    # Create Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(os.getenv('KAFKA_BROKER', 'kafka:9092')) \
        .set_topics('crypto-trades') \
        .set_group_id('flink-whale-detector') \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Create stream from Kafka
    trade_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name='KafkaSource'
    )

    # Process each trade
    # Process each trade
    trade_stream.map(lambda x: process_trade(x))

    print("✅ Flink job configured")
    print("📊 Listening for whale trades on Kafka topic: crypto-trades")
    print("=" * 60)

    # Execute the job
    env.execute('Whale Trade Detector')

if __name__ == '__main__':
    main()