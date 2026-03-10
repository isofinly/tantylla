fn main() {
    tonic_prost_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        // Allow omitting `default_fields` from JSON payloads for backward
        // compatibility with clients and tests that do not yet send the field.
        .field_attribute(
            "SearchRequest.default_fields",
            "#[serde(default, skip_serializing_if = \"Vec::is_empty\")]",
        )
        // Allow omitting `facet_fields` from JSON payloads when no facets
        // are requested, keeping the wire format compact.
        .field_attribute(
            "SearchRequest.facet_fields",
            "#[serde(default, skip_serializing_if = \"Vec::is_empty\")]",
        )
        // Allow omitting `boost_fields` from JSON payloads when no per-field
        // boosts are requested; empty means fall back to `default_fields` or
        // legacy behaviour.
        .field_attribute(
            "SearchRequest.boost_fields",
            "#[serde(default, skip_serializing_if = \"Vec::is_empty\")]",
        )
        // Allow omitting `group_by_partition` from JSON payloads; absence is
        // treated as `false` (no deduplication), preserving backward compat.
        .field_attribute("SearchRequest.group_by_partition", "#[serde(default)]")
        // Omit `facets` from the response JSON when no facets were requested
        // so that existing clients are unaffected.
        .field_attribute(
            "SearchResponse.facets",
            "#[serde(default, skip_serializing_if = \"Vec::is_empty\")]",
        )
        .compile_protos(&["proto/indexer/v1/service.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
