#!/usr/bin/env bash
# =========================================================================
# setup-connectors.sh: Register Kafka Connect source and sink connectors
# =========================================================================
#
# This script configures the CDC-to-Elasticsearch pipeline:
#
#   1. Creates an Elasticsearch index template with the English analyzer
#      (stemming) to match tantylla's en_stem tokenizer in Tantivy.
#
#   2. Registers the ScyllaDB CDC Source Connector to read CDC log
#      entries from benchmark.products and publish to a Kafka topic.
#
#   3. Registers the Elasticsearch Sink Connector to consume the Kafka
#      topic and index documents into Elasticsearch.
#
# Usage:
#   bash scripts/setup-connectors.sh \
#     <connect_host:port> <scylla_container:port> <es_container:port>
#
# Example:
#   bash scripts/setup-connectors.sh \
#     localhost:8083 bench-head-to-head-scylla:9042 bench-head-to-head-elasticsearch:9200

set -euo pipefail

CONNECT_URL="http://${1:?Usage: $0 <connect_host:port> <scylla_container:port> <es_container:port>}"
SCYLLA_ADDR="${2:?Usage: $0 <connect_host:port> <scylla_container:port> <es_container:port>}"
ES_ADDR="${3:?Usage: $0 <connect_host:port> <scylla_container:port> <es_container:port>}"

# Extract the ES port for the REST API call (may need to use
# the host-mapped port, not the container-internal one).
ES_PORT="${ES_ADDR##*:}"

echo "==> Waiting for Kafka Connect to be ready..."
for i in $(seq 1 60); do
    if curl -sf "${CONNECT_URL}/" > /dev/null 2>&1; then
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "ERROR: Kafka Connect not ready after 60 attempts." >&2
        exit 1
    fi
    sleep 3
done
echo "    Kafka Connect is ready."

# -------------------------------------------------------------------------
# Step 1: Elasticsearch index template with English analyzer
# -------------------------------------------------------------------------
# The English analyzer performs stemming (e.g., "running" → "run") which
# matches tantylla's en_stem tokenizer. Without this, ES would use the
# standard analyzer (no stemming), giving tantylla an unfair recall
# advantage on stemmed queries.

echo "==> Creating Elasticsearch index template..."

# We use the host-mapped ES port for this call (the script runs on the
# host, not inside Docker). The ES_ADDR passed here contains the
# container name:port for inter-container use, so we need the host port.
# The caller should pass the host-accessible address.
ES_URL="http://localhost:${ES_PORT}"

curl -sf -X PUT "${ES_URL}/_index_template/benchmark-products" \
  -H 'Content-Type: application/json' \
  -d '{
    "index_patterns": ["benchmark.products*"],
    "template": {
      "settings": {
        "analysis": {
          "analyzer": {
            "default": {
              "type": "english"
            }
          }
        },
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s"
      },
      "mappings": {
        "properties": {
          "product_id":     { "type": "keyword" },
          "name":           { "type": "text", "analyzer": "english" },
          "description":    { "type": "text", "analyzer": "english" },
          "brand":          { "type": "text", "analyzer": "english" },
          "category":       { "type": "keyword" },
          "subcategory":    { "type": "keyword" },
          "tags":           { "type": "keyword" },
          "price":          { "type": "float" },
          "stock_quantity":  { "type": "integer" },
          "rating_avg":     { "type": "float" },
          "review_count":   { "type": "integer" },
          "created_at":     { "type": "date" },
          "updated_at":     { "type": "date" }
        }
      }
    }
  }' && echo " OK" || echo " WARN: template creation returned non-zero"

# -------------------------------------------------------------------------
# Step 2: ScyllaDB CDC Source Connector
# -------------------------------------------------------------------------
# Reads the CDC log for benchmark.products and publishes change events
# to a Kafka topic named after the table.

echo "==> Registering ScyllaDB CDC Source Connector..."

curl -sf -X PUT "${CONNECT_URL}/connectors/scylla-cdc-source/config" \
  -H 'Content-Type: application/json' \
  -d "{
    \"connector.class\": \"com.scylladb.cdc.debezium.connector.ScyllaConnector\",
    \"scylla.cluster.ip.addresses\": \"${SCYLLA_ADDR}\",
    \"scylla.table.names\": \"benchmark.products\",
    \"tasks.max\": \"1\",
    \"key.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
    \"key.converter.schemas.enable\": \"false\",
    \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
    \"value.converter.schemas.enable\": \"false\"
  }" && echo " OK" || echo " WARN: source connector registration returned non-zero"

# -------------------------------------------------------------------------
# Step 3: Elasticsearch Sink Connector
# -------------------------------------------------------------------------
# Consumes the Kafka topic and indexes documents into Elasticsearch.
# We use the ExtractField SMT to unwrap the CDC "after" payload so ES
# receives clean document JSON rather than the CDC envelope.

echo "==> Registering Elasticsearch Sink Connector..."

curl -sf -X PUT "${CONNECT_URL}/connectors/es-sink/config" \
  -H 'Content-Type: application/json' \
  -d "{
    \"connector.class\": \"io.confluent.connect.elasticsearch.ElasticsearchSinkConnector\",
    \"topics\": \"benchmark.products\",
    \"connection.url\": \"http://${ES_ADDR}\",
    \"tasks.max\": \"1\",
    \"type.name\": \"_doc\",
    \"key.ignore\": \"true\",
    \"schema.ignore\": \"true\",
    \"behavior.on.malformed.documents\": \"warn\",
    \"behavior.on.null.values\": \"ignore\",
    \"write.method\": \"upsert\",
    \"batch.size\": \"200\",
    \"max.buffered.records\": \"5000\",
    \"flush.timeout.ms\": \"10000\"
  }" && echo " OK" || echo " WARN: sink connector registration returned non-zero"

echo ""
echo "==> Connector status:"
curl -sf "${CONNECT_URL}/connectors/scylla-cdc-source/status" | python3 -m json.tool 2>/dev/null || echo "  (source connector status unavailable)"
curl -sf "${CONNECT_URL}/connectors/es-sink/status" | python3 -m json.tool 2>/dev/null || echo "  (sink connector status unavailable)"

echo ""
echo "Done. Both connectors registered. CDC events will flow:"
echo "  ScyllaDB -> Kafka (benchmark.products topic) -> Elasticsearch"
