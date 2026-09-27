{ config, lib, pkgs, ... }:

# pifinder-differ — on-demand + self-warming zstd delta server for PiFinder
# NixOS updates. Decision record: docs/adr/0036-delta-updates-on-demand-differ.md
# in the PiFinder repo (nixos branch). Sits beside Attic (modules/attic.nix)
# and serves byte-level patches between store-path NARs:
#
#   POST /delta   device names a target + the bases it holds → 200/202/204
#   POST /warm    precompute all stem-paired deltas between two toplevels
#   GET  /pairs   every computed pair with sizes/ratios (observability)
#   GET  /status  queues, counters, warm-run progress
#   GET  /blobs/* the patch blobs
#
# v0.2+ works from the cache itself, not from a nix store: closures,
# references and chunk lists come from atticd's SQLite DB (read-only),
# candidate bases are ranked by FastCDC chunk overlap, and NAR bytes are
# fetched from atticd over loopback. Patches are NAR-to-NAR (device dumps
# its base with `nix-store --dump`), so GC holes in the cache degrade
# per-path instead of failing whole closures.
#
# Listens on loopback. The host's Caddy vhost (docs/caddy.md) exposes only
# /update-start, /delta, /blobs/* and /health.
#
# The server hosts other apps. All compute is idle-priority and one core is
# always left free: 2 workers on this 4-core / 6 GiB machine (zstd -19
# with a large window peaks ~800 MiB per job).

let
  pifinder-differ = pkgs.rustPlatform.buildRustPackage {
    pname = "pifinder-differ";
    version = "0.4.0";
    src = lib.cleanSourceWith {
      src = ../differ;
      filter = path: _type: builtins.baseNameOf path != "target";
    };
    cargoLock.lockFile = ../differ/Cargo.lock;
  };
in
{
  systemd.services.pifinder-differ = {
    description = "PiFinder delta (zstd --patch-from) server";
    wantedBy = [ "multi-user.target" ];
    after = [ "network-online.target" "atticd.service" ];
    wants = [ "network-online.target" ];

    # curl fetches NARs from loopback atticd and from cache.nixos.org, zstd
    # patches, xz/bzip2 decode upstream NARs, df disk guard.
    path = [ pkgs.curl pkgs.zstd pkgs.xz pkgs.bzip2 pkgs.coreutils pkgs.bash ];

    environment = {
      DIFFER_LISTEN = "127.0.0.1:8090";
      DIFFER_STATE_DIR = "/var/lib/pifinder-differ";
      DIFFER_WORKERS = "2";
      DIFFER_ATTIC_URL = "http://127.0.0.1:8080";
      DIFFER_ATTIC_DB = "/var/lib/atticd/server.db";
      DIFFER_CACHES = "pifinder pifinder-release";
      # `attic push` skips paths that cache.nixos.org has. The differ reads
      # those narinfos and NARs from there, so nixpkgs paths get patches too.
      DIFFER_UPSTREAM_URL = "https://cache.nixos.org";
      # Local LRU cache of decompressed NARs under the state dir. v0.2 was
      # S3-fetch-bound (~600 s for one 202 MiB pair); with the release lane's
      # working set (~1 GiB per toplevel diff) this holds ~10 diffs. Disk has
      # ~270 G free — raise if warm runs still show cold fetches.
      DIFFER_NAR_CACHE_BYTES = toString (10 * 1024 * 1024 * 1024);
    };

    serviceConfig = {
      ExecStart = "${pifinder-differ}/bin/pifinder-differ";
      # atticd's state directory holds server.db (see User below).
      StateDirectory = [ "pifinder-differ" "atticd" ];
      Restart = "on-failure";
      RestartSec = 5;

      # Runs as the atticd dynamic user. server.db and its WAL files belong
      # to that user (mode 0600, under the 0700 /var/lib/private), and a WAL
      # reader must write server.db-shm. systemd shares one dynamic user
      # between units with the same User=, and StateDirectory makes
      # /var/lib/atticd reachable here. So the differ needs no root and no
      # capability. DynamicUser also implies ProtectSystem=strict.
      # env holds the S3 keys and the JWT secret and belongs to atticd, so it
      # is hidden. The cache signing keys live inside server.db, so the differ
      # can still read them.
      DynamicUser = true;
      User = config.services.atticd.user;
      Group = config.services.atticd.group;
      CapabilityBoundingSet = "";
      AmbientCapabilities = "";
      NoNewPrivileges = true;
      InaccessiblePaths = [
        "-/var/lib/atticd/env"
        "-/var/lib/atticd/ci-token"
        "-/var/lib/atticd/.pifinder-vkem-keypair.bak"
      ];
      ProtectHome = true;
      PrivateTmp = true;
      PrivateDevices = true;
      ProtectKernelTunables = true;
      ProtectKernelModules = true;
      ProtectKernelLogs = true;
      ProtectControlGroups = true;
      ProtectClock = true;
      ProtectHostname = true;
      RestrictNamespaces = true;
      RestrictRealtime = true;
      RestrictSUIDSGID = true;
      LockPersonality = true;
      MemoryDenyWriteExecute = true;
      RestrictAddressFamilies = [ "AF_INET" "AF_INET6" "AF_UNIX" ];
      SystemCallArchitectures = "native";
      SystemCallFilter = [ "@system-service" ];

      # Never compete with the co-hosted services.
      Nice = 19;
      IOSchedulingClass = "idle";
      CPUWeight = 20;
      MemoryHigh = "2500M";
      MemoryMax = "3G";
    };
  };

  # Pre-warm: when a manifest entry moves to a new build (a PR or trunk
  # rebuild, or a new release), ask the differ on loopback to compute the
  # patches from the old build to the new one, so devices get them at once.
  # /warm stays loopback-only; this runs on the same host.
  systemd.services.pifinder-differ-warm = {
    description = "Pre-warm pifinder-differ for new PiFinder builds";
    after = [ "pifinder-differ.service" "network-online.target" ];
    wants = [ "network-online.target" ];
    environment = {
      WARM_DIFFER_URL = "http://127.0.0.1:8090";
      WARM_MANIFESTS = lib.concatStringsSep " " [
        "https://api.github.com/repos/brickbots/PiFinder/contents/update-manifest.json?ref=nixos-manifest"
        "https://api.github.com/repos/mrosseel/PiFinder/contents/update-manifest.json?ref=nixos-manifest"
      ];
    };
    serviceConfig = {
      Type = "oneshot";
      ExecStart = "${pkgs.python3}/bin/python3 ${./differ-warm.py}";
      DynamicUser = true;
      StateDirectory = "pifinder-differ-warm";
      CapabilityBoundingSet = "";
      NoNewPrivileges = true;
      PrivateDevices = true;
      ProtectHome = true;
      ProtectKernelTunables = true;
      ProtectKernelModules = true;
      ProtectControlGroups = true;
      RestrictAddressFamilies = [ "AF_INET" "AF_INET6" ];
      SystemCallFilter = [ "@system-service" ];
      Nice = 19;
    };
  };

  systemd.timers.pifinder-differ-warm = {
    wantedBy = [ "timers.target" ];
    timerConfig = {
      OnBootSec = "2min";
      OnUnitActiveSec = "1min";
    };
  };
}
