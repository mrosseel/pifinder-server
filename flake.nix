{
  description = "PiFinder server-side infrastructure: Attic binary cache + delta update server";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forSystems = f: nixpkgs.lib.genAttrs systems (s: f nixpkgs.legacyPackages.${s});
    in
    {
      # Consumed by the host's flake (nixos-config) as:
      #   inputs.pifinder-server.nixosModules.default
      # The host must also provide the HTTPS front (see docs/caddy.md) and,
      # for attic, the S3 credentials in /var/lib/atticd/env.
      nixosModules = {
        pifinder-differ = import ./modules/pifinder-differ.nix;
        attic = import ./modules/attic.nix;
        default = {
          imports = [
            ./modules/pifinder-differ.nix
            ./modules/attic.nix
          ];
        };
      };

      # Standalone build for development: nix build .#pifinder-differ
      packages = forSystems (pkgs: rec {
        pifinder-differ = pkgs.rustPlatform.buildRustPackage {
          pname = "pifinder-differ";
          version = "0.3.0";
          src = pkgs.lib.cleanSourceWith {
            src = ./differ;
            filter = path: _type: builtins.baseNameOf path != "target";
          };
          cargoLock.lockFile = ./differ/Cargo.lock;
        };
        default = pifinder-differ;
      });
    };
}
