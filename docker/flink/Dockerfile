FROM flink:1.18-scala_2.12-java11

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip3 install --no-cache-dir \
    apache-flink==1.18.0 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9

# Download Kafka connector JARs
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.2-1.18/flink-connector-kafka-3.0.2-1.18.jar && \
    wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

# Copy whale detector script
COPY ../../flink-job/whale_detector.py /opt/flink/whale_detector.py