FROM flink:1.18-scala_2.12-java11

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/flink/usrlib

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the Flink job
COPY whale_detector.py .

# The job will be submitted manually or via docker-compose command