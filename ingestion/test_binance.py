import json
import websocket

def on_message(ws, message):
    """Called when we receive a message from Binance"""
    parsed = json.loads(message)

    # Combined streams wrap data in a 'data' field
    if 'data' in parsed:
        data = parsed['data']
    else:
        data = parsed

    # Extract the important fields
    symbol = data['s']      # e.g., "BTCUSDT"
    price = data['p']       # Trade price
    quantity = data['q']    # Trade quantity
    trade_time = data['T']  # Timestamp

    # Calculate trade value in USD
    trade_value = float(price) * float(quantity)

    # Add whale emoji if trade > $100K
    whale_flag = "🐋" if trade_value > 100000 else ""

    print(f"🔔 {symbol:10} | ${float(price):>10,.2f} | Qty: {quantity:>10} | Value: ${trade_value:>12,.2f} {whale_flag}")

def on_error(ws, error):
    print(f"❌ Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("🔌 Connection closed")

def on_open(ws):
    print("✅ Connected to Binance - monitoring BTC, ETH, BNB!")

if __name__ == "__main__":
    # Monitor multiple symbols using combined streams
    symbols = ["btcusdt", "ethusdt", "bnbusdt"]
    streams = "/".join([f"{symbol}@trade" for symbol in symbols])
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    print(f"🚀 Connecting to Binance WebSocket...")
    print(f"📊 Monitoring: {', '.join([s.upper() for s in symbols])}")

    ws = websocket.WebSocketApp(
        socket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()