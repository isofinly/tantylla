{
  description = "Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        protoTools = with pkgs; [
          buf
          protobuf
          protoc-gen-rust
          protoc-gen-rust-grpc
        ];

        cql_lsp = pkgs.rustPlatform.buildRustPackage rec {
          pname = "cql_lsp";
          version = "1.0.3";

          src = pkgs.fetchCrate {
            inherit pname version;
            sha256 = "sha256-ebk9/Ja4a/iioDWEEa86wm3C4EQWd7oaK9MO4jT5aAo=";
          };

          cargoHash = "sha256-ea/t1WN4W1u2esV8K5uovwv3gWY+Y6mXy+2gF7hq5zg=";

          meta = with pkgs.lib; {
            description = "CQL (Cassandra Query Language) LSP";
            homepage = "https://crates.io/crates/cql_lsp";
            license = licenses.mit;
          };
        };
        # cql_lsp = pkgs.rustPlatform.buildRustPackage {
        #   pname = "cql_lsp";
        #   version = "1.0.1-fix-I25";

        #   src = pkgs.fetchurl {
        #     url = "https://github.com/Akzestia/cqlls/archive/refs/tags/v1.0.1-fix-I25.tar.gz";
        #     sha256 = "sha256-3JAWdsBfZ4YFwxdJrVQFBTM+8GWNy9HQGmWJDiI9bXU=";
        #   };

        #   cargoHash = "sha256-s5p4V2UoqwfDmLPXBQcqGGOlQSfUt6QBOHRqnkkmwyk=";

        #   meta = with pkgs.lib; {
        #     description = "CQL (Cassandra Query Language) LSP";
        #     homepage = "https://github.com/Akzestia/cqlls";
        #     license = licenses.mit;
        #   };
        # };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs =
            with pkgs;
            [
              (rust-bin.stable.latest.default.override {
                extensions = [
                  "rust-src"
                  "rust-analyzer"
                ];
              })
              openssl
              pkg-config
              cassandra
              cql_lsp
              cargo-deny
            ]
            ++ protoTools;

          env = {
            OUT_DIR="~/.cargo-target/proto";
          };

          shellHook = ''
            rustc --version
            cqlsh --version
            buf --version
          '';
        };
      }
    );
}
