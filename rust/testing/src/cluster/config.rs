// =========================================================================
// Test Cluster Configuration
// =========================================================================

#[derive(Clone, Debug)]
pub struct TopologyConfig {
    pub search_nodes: usize,
    pub ingestors: usize,
    pub gateways: usize,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            search_nodes: 1,
            ingestors: 1,
            gateways: 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScyllaConfig {
    pub contact_points: Vec<String>,
    // TODO: Replace contact points with a container-backed Scylla cluster for
    // full infrastructure topology and failure testing.
}

impl Default for ScyllaConfig {
    fn default() -> Self {
        Self {
            contact_points: vec!["127.0.0.1:9042".to_string()],
        }
    }
}

#[derive(Clone, Debug)]
pub struct SchemaConfig {
    statements: Vec<String>,
}

impl SchemaConfig {
    pub fn from_cql(cql: &str) -> Self {
        let mut statements = Vec::new();
        for statement in cql.split(';') {
            let trimmed = statement.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }
            statements.push(trimmed.to_string());
        }
        Self { statements }
    }

    pub fn default_schema() -> Self {
        Self::from_cql(
            "CREATE TABLE IF NOT EXISTS {{keyspace}}.documents (\
                doc_id text PRIMARY KEY,\
                title text,\
                body text,\
                updated_at timestamp\
            ) WITH cdc = {'enabled': true};",
        )
    }

    pub(super) fn render(&self, keyspace: &str) -> Vec<String> {
        self.statements
            .iter()
            .map(|statement| statement.replace("{{keyspace}}", keyspace))
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
pub struct InstrumentationConfig {
    pub enabled: bool,
    pub event_port: Option<u16>,
}
