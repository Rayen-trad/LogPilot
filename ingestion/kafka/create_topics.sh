#!/usr/bin/env bash
# ==============================================================================
# LogPilot — Kafka Topic Provisioning Script
#
# Creates the 5 core streaming topics according to the architecture specification:
#  - logs.raw          (partitions: 6, retention: 24h)
#  - logs.parsed       (partitions: 6, retention: 24h)
#  - metrics.windows   (partitions: 3, retention: 24h)
#  - anomalies         (partitions: 3, retention: 7d)
#  - incidents.events  (partitions: 3, retention: 7d)
#
# Usage:
#   # From host (requires local kafka-topics CLI or via docker exec):
#   docker exec -i logpilot-kafka /bin/bash < ingestion/kafka/create_topics.sh
#   # Or directly inside the kafka container:
#   bash ingestion/kafka/create_topics.sh
# ==============================================================================

set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"

# Retention definitions in milliseconds:
# 24 hours = 24 * 60 * 60 * 1000 = 86400000 ms
# 7 days    = 7 * 24 * 60 * 60 * 1000 = 604800000 ms
RETENTION_24H=86400000
RETENTION_7D=604800000

echo "Connecting to Kafka cluster at: ${BOOTSTRAP_SERVER}"

# Wait for Kafka to become responsive
MAX_RETRIES=30
COUNT=0
until kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --list > /dev/null 2>&1; do
    COUNT=$((COUNT + 1))
    if [ "${COUNT}" -ge "${MAX_RETRIES}" ]; then
        echo "Error: Timed out waiting for Kafka broker at ${BOOTSTRAP_SERVER}" >&2
        exit 1
    fi
    echo "Kafka not ready yet (attempt ${COUNT}/${MAX_RETRIES}), retrying in 2s..."
    sleep 2
done

echo "Kafka broker is ready. Creating topics..."

create_topic() {
    local topic="$1"
    local partitions="$2"
    local retention_ms="$3"

    echo "--- Provisioning '${topic}' (partitions: ${partitions}, rep-factor: ${REPLICATION_FACTOR}, retention: ${retention_ms}ms) ---"
    kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create \
        --if-not-exists \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor "${REPLICATION_FACTOR}" \
        --config retention.ms="${retention_ms}"
}

# 1. logs.raw: Tail log lines shipped by Filebeat
create_topic "logs.raw" 6 "${RETENTION_24H}"

# 2. logs.parsed: Structured events parsed by Logstash
create_topic "logs.parsed" 6 "${RETENTION_24H}"

# 3. metrics.windows: Tumbling & sliding window aggregations from Spark
create_topic "metrics.windows" 3 "${RETENTION_24H}"

# 4. anomalies: Z-score & error pattern alerts from Spark
create_topic "anomalies" 3 "${RETENTION_7D}"

# 5. incidents.events: Correlated incident state transitions from Incident Manager
create_topic "incidents.events" 3 "${RETENTION_7D}"

echo ""
echo "=== Topic Verification ==="
kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --describe --topic "logs.raw,logs.parsed,metrics.windows,anomalies,incidents.events"

echo ""
echo "All LogPilot topics successfully verified."
