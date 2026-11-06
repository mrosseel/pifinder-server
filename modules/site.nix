# The PiFinder project's cache and delta server: cache.pifinder.io and
# deltas.pifinder.io. Every host that serves them imports this module and adds
# only its own machine settings.
#
# The .eu names are aliases. Devices in the field still use them.
{
  imports = [
    ./attic.nix
    ./pifinder-differ.nix
  ];

  services.pifinder-attic = {
    enable = true;
    domain = "cache.pifinder.io";
  };
  services.caddy.virtualHosts."cache.pifinder.io".serverAliases = [ "cache.pifinder.eu" ];

  services.pifinder-differ = {
    enable = true;
    domain = "deltas.pifinder.io";
  };
  services.caddy.virtualHosts."deltas.pifinder.io".serverAliases = [ "deltas.pifinder.eu" ];

  networking.firewall.allowedTCPPorts = [ 80 443 ];
}
