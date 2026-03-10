FROM confluentinc/cp-kafka-connect:8.2.0

USER root
RUN set -eux; \
    # Install only missing tooling to avoid conflicts with minimal curl packages
    if command -v microdnf >/dev/null 2>&1; then \
        if ! command -v curl >/dev/null 2>&1; then microdnf install -y curl; fi; \
        microdnf install -y unzip ca-certificates; \
        microdnf clean all; \
    elif command -v yum >/dev/null 2>&1; then \
        if ! command -v curl >/dev/null 2>&1; then yum install -y curl; fi; \
        yum install -y unzip ca-certificates; \
        yum clean all; \
    else \
        echo "no supported package manager found" >&2; \
        exit 127; \
    fi

RUN set -eux; \
    SCYLLA_VER=2.0.0; \
    MAVEN_JAR_URL="https://repo1.maven.org/maven2/com/scylladb/scylla-cdc-source-connector/${SCYLLA_VER}/scylla-cdc-source-connector-${SCYLLA_VER}-jar-with-dependencies.jar"; \
    dest_dir=/usr/share/java/scylla-cdc-source-connector; \
    mkdir -p "$dest_dir"; \
    echo "Downloading Scylla CDC connector ${SCYLLA_VER} from Maven Central"; \
    curl -fSL -o "$dest_dir/scylla-cdc-source-connector-${SCYLLA_VER}-jar-with-dependencies.jar" "$MAVEN_JAR_URL"; \
    # verify download
    if [ ! -s "$dest_dir/scylla-cdc-source-connector-${SCYLLA_VER}-jar-with-dependencies.jar" ]; then \
        echo "Failed to download valid Scylla CDC connector JAR from ${MAVEN_JAR_URL}" >&2; \
        exit 1; \
    fi

COPY assets /tmp/assets

RUN set -eux; \
    if ls /tmp/assets/kafka-connect-elasticsearch*.zip >/dev/null 2>&1; then \
        echo "Installing Elasticsearch connector from build-context asset"; \
        for f in /tmp/assets/kafka-connect-elasticsearch*.zip; do \
            unzip "$f" -d /usr/share/java/; \
        done; \
    else \
        echo "No pre-staged Elasticsearch connector found; attempting confluent-hub install"; \
        confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:latest || true; \
        if curl --output /dev/null --silent --head --fail "https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-elasticsearch/latest/download"; then \
            echo "Falling back to direct CDN download for elasticsearch connector"; \
            curl -L -o /tmp/kc-elastic.zip "https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-elasticsearch/latest/download"; \
            unzip /tmp/kc-elastic.zip -d /usr/share/java/; \
            rm -f /tmp/kc-elastic.zip; \
        else \
            echo "Elasticsearch connector not installed — continuing without it"; \
        fi; \
    fi
