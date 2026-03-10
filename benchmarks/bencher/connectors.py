"""connectors.py: Kafka Connect setup for the competitor (Elasticsearch) stack.

Replaces setup-connectors.sh. Configuration is expressed as Python dicts so
it is easy to adjust without embedded shell-escaped JSON strings.
"""

import time

import requests

TOPIC_PREFIX = "scylla"
CDC_TOPIC = f"{TOPIC_PREFIX}.benchmark.products"

# English stemming analyzer — matches Tantivy's en_stem tokenizer.
# Applied to both the index template and the pre-created CDC target index
# so the setting lives in exactly one place.
_ANALYZER_SETTINGS: dict = {
    "analysis": {"analyzer": {"default": {"type": "english"}}},
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "5s",
}

# Field mappings for benchmark.products.  Shared between the template and the
# pre-created index to prevent schema drift if the mappings are ever changed.
_FIELD_MAPPINGS: dict = {
    "properties": {
        "product_id": {"type": "keyword"},
        "name": {"type": "text", "analyzer": "english"},
        "description": {"type": "text", "analyzer": "english"},
        # keyword sub-field enables exact-term counts used by the ingest benchmark.
        "brand": {
            "type": "text",
            "analyzer": "english",
            "fields": {"keyword": {"type": "keyword"}},
        },
        "category": {"type": "keyword"},
        "subcategory": {"type": "keyword"},
        "tags": {"type": "keyword"},
        "price": {"type": "float"},
        "stock_quantity": {"type": "integer"},
        "rating_avg": {"type": "float"},
        "review_count": {"type": "integer"},
        "created_at": {"type": "date"},
        "updated_at": {"type": "date"},
    }
}


def _wait_for_connect(url: str, retries: int = 60, interval: float = 3.0) -> None:
    print("Waiting for Kafka Connect ", end="", flush=True)
    for _ in range(retries):
        try:
            if requests.get(url, timeout=5).ok:
                print(" OK")
                return
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(interval)
    print()
    raise RuntimeError("Kafka Connect did not become ready")


def _put(url: str, body: dict, label: str) -> None:
    resp = requests.put(url, json=body, timeout=30)
    if resp.ok:
        print(f"  {label}: OK")
    else:
        print(f"  WARN {label}: HTTP {resp.status_code} — {resp.text[:200]}")


def setup(connect_host: str, scylla_addr: str, es_addr: str) -> None:
    """Register the ScyllaDB CDC source and Elasticsearch sink connectors.

    connect_host : host:port of Kafka Connect REST API (host-accessible)
    scylla_addr  : host:port of ScyllaDB reachable from inside Docker
    es_addr      : host:port of Elasticsearch reachable from inside Docker
    """
    connect_url = f"http://{connect_host}"
    # This script runs on the host, so we build a localhost ES URL from the
    # mapped port — ES_ADDR is the container-internal address used by Kafka Connect.
    es_port = es_addr.split(":")[-1]
    es_url = f"http://localhost:{es_port}"

    _wait_for_connect(connect_url)

    # -------------------------------------------------------------------------
    # Step 1: Index template — applies English stemming to all future indices
    #         matching "benchmark.products*".
    # -------------------------------------------------------------------------
    print("Creating Elasticsearch index template...")
    _put(
        f"{es_url}/_index_template/benchmark-products",
        {
            "index_patterns": ["benchmark.products*"],
            "template": {"settings": _ANALYZER_SETTINGS, "mappings": _FIELD_MAPPINGS},
        },
        "index template",
    )

    # -------------------------------------------------------------------------
    # Step 1b: Pre-create the CDC target index.
    #
    # The Confluent ES connector v15 (ES7 RestHighLevelClient) appends
    # ?include_type_name=false to its HEAD index-exists request, which ES 8.x
    # rejects with 400.  Pre-creating the index causes the connector to skip
    # that existence-check codepath entirely.
    # -------------------------------------------------------------------------
    print(f"Pre-creating CDC index '{CDC_TOPIC}'...")
    _put(
        f"{es_url}/{CDC_TOPIC}",
        {"settings": _ANALYZER_SETTINGS, "mappings": _FIELD_MAPPINGS},
        f"index {CDC_TOPIC}",
    )

    # -------------------------------------------------------------------------
    # Step 2: ScyllaDB CDC Source Connector
    # -------------------------------------------------------------------------
    print("Registering ScyllaDB CDC Source Connector...")
    _put(
        f"{connect_url}/connectors/scylla-cdc-source/config",
        {
            "connector.class": "com.scylladb.cdc.debezium.connector.ScyllaConnector",
            "topic.prefix": TOPIC_PREFIX,
            "scylla.cluster.ip.addresses": scylla_addr,
            "scylla.table.names": "benchmark.products",
            "tasks.max": "1",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
        },
        "ScyllaDB CDC Source Connector",
    )

    # -------------------------------------------------------------------------
    # Step 3: Elasticsearch Sink Connector
    # -------------------------------------------------------------------------
    print("Registering Elasticsearch Sink Connector...")
    _put(
        f"{connect_url}/connectors/es-sink/config",
        {
            "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
            "topics": CDC_TOPIC,
            "connection.url": f"http://{es_addr}",
            "tasks.max": "1",
            "type.name": "_doc",
            "key.ignore": "true",
            "schema.ignore": "true",
            "behavior.on.malformed.documents": "warn",
            "behavior.on.null.values": "ignore",
            "write.method": "upsert",
            "batch.size": "200",
            "max.buffered.records": "5000",
            "flush.timeout.ms": "10000",
        },
        "Elasticsearch Sink Connector",
    )

    # Print connector status for quick confirmation.
    print()
    for label, name in [("source", "scylla-cdc-source"), ("sink", "es-sink")]:
        try:
            status = requests.get(
                f"{connect_url}/connectors/{name}/status", timeout=10
            ).json()
            print(f"  [{label}] {status}")
        except Exception as exc:
            print(f"  [{label}] status unavailable: {exc}")

    print(f"\nCDC pipeline ready: ScyllaDB → Kafka ({CDC_TOPIC}) → Elasticsearch")
