fn main() {
    tonic_prost_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .field_attribute(
            "SearchRequest.default_fields",
            "#[serde(default, skip_serializing_if = \"Vec::is_empty\")]",
        )
        .field_attribute(
            "SearchRequest.facet_fields",
            "#[serde(default, skip_serializing_if = \"Vec::is_empty\")]",
        )
        .field_attribute(
            "SearchRequest.boost_fields",
            "#[serde(default, skip_serializing_if = \"Vec::is_empty\")]",
        )
        .field_attribute("SearchRequest.group_by_partition", "#[serde(default)]")
        .field_attribute(
            "SearchResponse.facets",
            "#[serde(default, skip_serializing_if = \"Vec::is_empty\")]",
        )
        .compile_protos(&["proto/indexer/v1/service.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
