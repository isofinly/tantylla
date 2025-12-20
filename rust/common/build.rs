fn main() {
    tonic_prost_build::compile_protos("proto/indexer/v1/service.proto")
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
