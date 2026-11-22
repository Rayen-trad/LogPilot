# Spark 3.5 (local mode) for the streaming detector and the batch jobs.
FROM apache/spark:3.5.3-python3

USER root
# Kafka connector baked in, so jobs don't download jars at startup.
ARG MVN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && curl -fsSLO $MVN/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
 && curl -fsSLO $MVN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
 && curl -fsSLO $MVN/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSLO $MVN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /state && chown spark:spark /state
USER spark
WORKDIR /app
ENV PYTHONPATH=/app PYSPARK_PYTHON=python3
