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
# IMPORTANT: <es_container:port> must be the address reachable from
# *inside* the Docker network (i.e. the container name, not localhost).
# The script derives the host-side ES URL by extracting just the port
# and substituting localhost, so passing localhost here would break the
# Kafka Connect sink connector.
#
# Example:
#   bash scripts/setup-connectors.sh \
#     localhost:8083 bench-competitor-scylla:9042 bench-competitor-elasticsearch:9200

set -euo pipefail

CONNECT_URL="http://${1:?Usage: $0 <connect_host:port> <scylla_container:port> <es_container:port>}"
SCYLLA_ADDR="${2:?Usage: $0 <connect_host:port> <scylla_container:port> <es_container:port>}"
ES_ADDR="${3:?Usage: $0 <connect_host:port> <scylla_container:port> <es_container:port>}"

# The Scylla CDC connector (v2.0.0+) requires topic.prefix. The topic
# produced for keyspace.table becomes "${TOPIC_PREFIX}.keyspace.table",
# so the ES sink must subscribe to the same derived name.
TOPIC_PREFIX="scylla"
CDC_TOPIC="${TOPIC_PREFIX}.benchmark.products"

# Extract the ES port so we can build a host-accessible URL for the
# index template PUT (this script runs on the host, not inside Docker).
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

# Build the host-accessible ES URL from the extracted port. ES_ADDR is
# the container-internal address (used by Kafka Connect inside Docker);
# for calls made directly from this script on the host we always use
# localhost with the mapped port.
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
# Step 1b: Pre-create the CDC index
# -------------------------------------------------------------------------
# The Confluent kafka-connect-elasticsearch connector v15.x bundles the
# legacy ES 7 RestHighLevelClient. Its IndicesClient.exists() call appends
# "?include_type_name=false" to the HEAD request, which ES 8.x treats as an
# unknown parameter and rejects with HTTP 400. Pre-creating the index causes
# the connector to skip the existence-check codepath entirely.
#
# The index name matches the Kafka topic name (${TOPIC_PREFIX}.keyspace.table)
# because the connector uses the topic name as the index name by default.
# The index_template created above (matching "benchmark.products*") does NOT
# match this name, so we apply the same mappings directly here.

echo "==> Pre-creating CDC target index ${CDC_TOPIC}..."

curl -sf -X PUT "${ES_URL}/${CDC_TOPIC}" \
  -H 'Content-Type: application/json' \
  -d "{
    \"settings\": {
      \"analysis\": { \"analyzer\": { \"default\": { \"type\": \"english\" } } },
      \"number_of_shards\": 1,
      \"number_of_replicas\": 0,
      \"refresh_interval\": \"5s\"
    },
    \"mappings\": {
      \"properties\": {
        \"product_id\":     { \"type\": \"keyword\" },
        \"name\":           { \"type\": \"text\", \"analyzer\": \"english\" },
        \"description\":    { \"type\": \"text\", \"analyzer\": \"english\" },
        \"brand\":          { \"type\": \"text\", \"analyzer\": \"english\" },
        \"category\":       { \"type\": \"keyword\" },
        \"subcategory\":    { \"type\": \"keyword\" },
        \"tags\":           { \"type\": \"keyword\" },
        \"price\":          { \"type\": \"float\" },
        \"stock_quantity\": { \"type\": \"integer\" },
        \"rating_avg\":     { \"type\": \"float\" },
        \"review_count\":   { \"type\": \"integer\" },
        \"created_at\":     { \"type\": \"date\" },
        \"updated_at\":     { \"type\": \"date\" }
      }
    }
  }" && echo " OK" || echo " WARN: index pre-creation returned non-zero (may already exist)"

# -------------------------------------------------------------------------
# Step 2: ScyllaDB CDC Source Connector
# -------------------------------------------------------------------------
# Reads the CDC log for benchmark.products and publishes change events
# to the Kafka topic "${TOPIC_PREFIX}.benchmark.products".
#
# topic.prefix is mandatory in connector v2.0.0+ (it replaced the old
# database.server.name field). Without it the connector rejects the
# config with a validation error.

echo "==> Registering ScyllaDB CDC Source Connector..."

curl -sf -X PUT "${CONNECT_URL}/connectors/scylla-cdc-source/config" \
  -H 'Content-Type: application/json' \
  -d "{
    \"connector.class\": \"com.scylladb.cdc.debezium.connector.ScyllaConnector\",
    \"topic.prefix\": \"${TOPIC_PREFIX}\",
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
#
# connection.url must be the address reachable from *inside* Docker
# (ES_ADDR), not localhost. topics must match the topic name produced by
# the source connector above.

echo "==> Registering Elasticsearch Sink Connector..."

curl -sf -X PUT "${CONNECT_URL}/connectors/es-sink/config" \
  -H 'Content-Type: application/json' \
  -d "{
    \"connector.class\": \"io.confluent.connect.elasticsearch.ElasticsearchSinkConnector\",
    \"topics\": \"${CDC_TOPIC}\",
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
echo "  ScyllaDB -> Kafka (${CDC_TOPIC}) -> Elasticsearch"
