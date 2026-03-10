fn main() {
    tonic_prost_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        // Allow omitting `default_fields` from JSON payloads for backward
        // compatibility with clients and tests that do not yet send the field.
        .field_attribute(
            "SearchRequest.default_fields",
            "#[serde(default, skip_serializing_if = \"Vec::is_empty\")]",
        )
        .compile_protos(&["proto/indexer/v1/service.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
