# The AWS host for cache.pifinder.io and deltas.pifinder.io. nixos-anywhere
# installs it on an EC2 Graviton instance. README.md has the install and the
# move from the current server.
{ lib, ... }:
let
  sshKeys = [
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOvpuaWhiyWISrRYXtOpBLo6Fo/+NzZ0812RHlorSuNF mike.rosseel@gmail.com"
  ];
in
{
  imports = [
    ../../modules/site.nix
    ./disk.nix
  ];

  networking.hostName = "pifinder-aws";
  time.timeZone = "UTC";

  # Graviton instances boot only with UEFI.
  boot.loader.systemd-boot.enable = true;
  boot.loader.efi.canTouchEfiVariables = false;
  boot.initrd.availableKernelModules = [ "nvme" "ena" ];
  # The EC2 serial console, and no I/O timeout on the EBS NVMe device.
  boot.kernelParams = [ "console=ttyS0,115200n8" "nvme_core.io_timeout=4294967295" ];

  swapDevices = [{ device = "/var/lib/swapfile"; size = 4096; }];

  services.openssh = {
    enable = true;
    settings = {
      PasswordAuthentication = false;
      KbdInteractiveAuthentication = false;
      PermitRootLogin = "prohibit-password";
    };
  };
  users.users.root.openssh.authorizedKeys.keys = sshKeys;
  users.users.mike = {
    isNormalUser = true;
    extraGroups = [ "wheel" ];
    openssh.authorizedKeys.keys = sshKeys;
  };
  # Key-only login, so sudo asks no password. nixos-rebuild --target-host
  # needs this.
  security.sudo.wheelNeedsPassword = false;

  nix.settings = {
    experimental-features = [ "nix-command" "flakes" ];
    trusted-users = [ "root" "@wheel" ];
  };
  nix.gc = {
    automatic = true;
    dates = "weekly";
    options = "--delete-older-than 30d";
  };

  # The differ README gives one patch job per free core. A t4g.large has two
  # cores, and one stays free.
  services.pifinder-differ.workers = 1;

  # The cache state (the database with the signing keys, and the JWT secret
  # in env) comes from the current server. Until it is in place, none of
  # these units start. On an empty disk the bootstrap units would make new
  # signing keys that no device trusts. Also, a second atticd must not run
  # its GC on the same S3 bucket.
  systemd.services = lib.genAttrs [
    "atticd"
    "atticd-bootstrap-secret"
    "atticd-bootstrap-cache"
    "atticd-bootstrap-cache-release"
    "atticd-bootstrap-token"
    "atticd-set-dev-retention"
    "pifinder-differ"
    "pifinder-differ-warm"
  ] (_: { unitConfig.ConditionPathExists = [ "/var/lib/private/atticd/server.db" ]; });

  system.stateVersion = "26.05";
}
