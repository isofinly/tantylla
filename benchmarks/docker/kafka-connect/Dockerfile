# =========================================================================
# Kafka Connect: ScyllaDB CDC Source + Elasticsearch Sink
# =========================================================================
#
# Extends the Confluent Kafka Connect base image with two connector
# plugins that form the competitor benchmark pipeline:
#
#   ScyllaDB (CDC log) --> Kafka Connect --> Kafka --> Kafka Connect --> Elasticsearch
#
# The ScyllaDB CDC Source Connector reads the CDC log tables directly
# (no Debezium involved) and produces change events to Kafka topics.
# The Elasticsearch Sink Connector consumes those topics and indexes
# documents into Elasticsearch.
#
# NOTE: The Confluent image may run under Rosetta 2 on Apple Silicon.
# This adds overhead to the competitor stack; tantylla runs natively.
# Both stacks share the same ScyllaDB instance so the CDC source is
# identical — the comparison measures the indexing + search pipeline.

FROM confluentinc/cp-kafka-connect:7.7.1

# Install ScyllaDB CDC Source Connector
RUN confluent-hub install --no-prompt \
    scylladb/scylla-cdc-source-connector:1.1.2

# Install Elasticsearch Sink Connector
RUN confluent-hub install --no-prompt \
    confluentinc/kafka-connect-elasticsearch:14.2.2
