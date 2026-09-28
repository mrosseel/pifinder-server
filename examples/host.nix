# A host that runs the PiFinder cache and delta server.
#
# In the host's flake:
#   inputs.pifinder-server.url = "github:mrosseel/pifinder-server";
#   modules = [ inputs.pifinder-server.nixosModules.default ./pifinder.nix ];
#
# Before the first deploy:
#   - DNS A/AAAA records for both domains point to this host (Caddy gets the
#     certificates when they resolve).
#   - /var/lib/atticd/env holds AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
#     for the S3 bucket (mode 0600, owner root). The first start adds the JWT
#     secret to it.
# After the first deploy:
#   - sudo cat /var/lib/atticd/ci-token, and put it in the ATTIC_TOKEN
#     secret of the PiFinder repo.
{
  services.pifinder-attic = {
    enable = true;
    domain = "cache.example.org";
    storage = {
      type = "s3";
      region = "eu-west-1";
      bucket = "example-pifinder-cache";
    };
  };

  services.pifinder-differ = {
    enable = true;
    domain = "deltas.example.org";
    # The manifests this server makes patches for in advance.
    warm.manifests = [ "example/PiFinder/nixos-manifest/update-manifest.json" ];
    # Only if the host runs services.prometheus and services.grafana:
    monitoring.prometheus = true;
    monitoring.grafanaDashboard = true;
  };

  services.caddy.email = "admin@example.org";
  networking.firewall.allowedTCPPorts = [ 80 443 ];
}
