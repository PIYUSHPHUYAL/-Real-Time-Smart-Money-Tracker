FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first (for Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY binance_to_kafka.py .

# Run the ingestion service
CMD ["python", "-u", "binance_to_kafka.py"]