{
  description = "PiFinder server-side infrastructure: Attic binary cache + delta update server";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forSystems = f: nixpkgs.lib.genAttrs systems (s: f nixpkgs.legacyPackages.${s});
    in
    {
      # Consumed by the host's flake as
      #   inputs.pifinder-server.nixosModules.default
      # and switched on with services.pifinder-attic.enable and
      # services.pifinder-differ.enable. examples/host.nix shows a full host.
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
          version = "0.4.0";
          src = pkgs.lib.cleanSourceWith {
            src = ./differ;
            filter = path: _type: builtins.baseNameOf path != "target";
          };
          cargoLock.lockFile = ./differ/Cargo.lock;
        };
        default = pifinder-differ;
      });

      # Evaluates examples/host.nix as a complete NixOS system:
      #   nix flake check
      checks = forSystems (pkgs:
        let
          example = nixpkgs.lib.nixosSystem {
            inherit (pkgs) system;
            modules = [
              self.nixosModules.default
              ./examples/host.nix
              {
                services.prometheus.enable = true;
                services.grafana.enable = true;
                services.grafana.settings.security.secret_key = "example-only";
                boot.loader.grub.enable = false;
                fileSystems."/" = { device = "none"; fsType = "tmpfs"; };
                system.stateVersion = "26.05";
              }
            ];
          };
          drv = builtins.unsafeDiscardStringContext
            example.config.system.build.toplevel.drvPath;
        in
        {
          example-host = pkgs.runCommand "example-host-eval" { } ''
            echo ${drv} > $out
          '';
        });
    };
}
