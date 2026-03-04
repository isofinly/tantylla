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

        cqlls = pkgs.rustPlatform.buildRustPackage rec {
          pname = "cqlls";
          version = "1.0.0";

          src = pkgs.fetchCrate {
            inherit pname version;
            sha256 = "sha256-O6xU7gaNCZ9163Zk+4SJM7lNq1Dn3BQhILKZos7l3sI=";
          };

          doCheck = false;
          cargoHash = "sha256-fcrRCNEMTFatHvVdE9zPukuq84hD6RUkEYInrqtzFeg=";

          meta = with pkgs.lib; {
            description = "The best Language Server for CQL (Cassandra Query Language)";
            homepage = "https://crates.io/crates/cqlls";
            license = licenses.mit;
          };
        };
        terraformWrapper = pkgs.writeShellScriptBin "terraform" ''
          exec ${pkgs.opentofu}/bin/tofu "$@"
        '';
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
              cqlls
              cargo-deny
              opentofu
              terraformWrapper
            ]
            ++ protoTools;

          env = {
            OUT_DIR = "~/.cargo-target/proto";
          };

          shellHook = ''
            rustc --version
            cqlsh --version
            buf --version

            alias terraform=tofu
          '';
        };
      }
    );
}
