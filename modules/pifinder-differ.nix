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
# Listens on loopback. With caddy.enable the module writes the Caddy vhost,
# which exposes only /update-start, /delta, /deltas, /blobs/* and /health.
# docs/caddy.md shows the same vhost for a host with its own proxy.
#
# The server hosts other apps. All compute is idle-priority and one core is
# always left free: 2 workers on a 4-core / 6 GiB machine (zstd -19 with a
# large window peaks ~800 MiB per job).
#
# Needs services.pifinder-attic on the same host: the differ reads atticd's
# database and fetches NARs from atticd over loopback.

let
  cfg = config.services.pifinder-differ;
  attic = config.services.pifinder-attic;
  listen = "127.0.0.1:${toString cfg.port}";

  defaultPackage = pkgs.rustPlatform.buildRustPackage {
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
  options.services.pifinder-differ = {
    enable = lib.mkEnableOption "the PiFinder delta update server";

    package = lib.mkOption {
      type = lib.types.package;
      default = defaultPackage;
      defaultText = lib.literalMD "built from `differ/` in this repo";
      description = "The pifinder-differ package.";
    };

    domain = lib.mkOption {
      type = lib.types.str;
      default = "deltas.pifinder.eu";
      description = ''
        Public host name of the delta server. The PiFinder devices read it
        from PIFINDER_DELTA_URL.
      '';
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = 8090;
      description = "Loopback port. Never expose it: the rate limit trusts X-Forwarded-For.";
    };

    workers = lib.mkOption {
      type = lib.types.ints.positive;
      default = 2;
      description = "Patch jobs at the same time. Each zstd -19 job can use about 800 MiB.";
    };

    narCacheBytes = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 10 * 1024 * 1024 * 1024;
      description = "Size of the local cache of decompressed NARs under /var/lib/pifinder-differ.";
    };

    caches = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ "pifinder" "pifinder-release" ];
      description = "Attic caches that hold the PiFinder builds.";
    };

    upstreamUrl = lib.mkOption {
      type = lib.types.str;
      default = "https://cache.nixos.org";
      description = ''
        Binary cache for the paths that `attic push` skips because this
        cache has them. The differ makes patches for those paths too.
      '';
    };

    memoryHigh = lib.mkOption {
      type = lib.types.str;
      default = "2500M";
      description = "systemd MemoryHigh of the differ.";
    };

    memoryMax = lib.mkOption {
      type = lib.types.str;
      default = "3G";
      description = "systemd MemoryMax of the differ.";
    };

    warm = {
      manifests = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [
          "brickbots/PiFinder/nixos-manifest/update-manifest.json"
          "mrosseel/PiFinder/nixos-manifest/update-manifest.json"
        ];
        description = ''
          Update manifests to watch, as <owner>/<repo>/<branch>/<path>. When
          an entry moves to a new build, the warm service asks the differ for
          the patches from the old build to the new one.
        '';
      };

      hubs = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [ "PR#379" ];
        description = ''
          Manifest labels (without the commit suffix) that devices switch
          to and from often. The warm service also makes the patches
          between these builds and the other recent builds.
        '';
      };
    };

    caddy.enable = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Write the Caddy vhost for `domain` (and enable Caddy).";
    };

    monitoring = {
      prometheus = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = "Add a scrape job for the differ's /metrics to services.prometheus.";
      };

      grafanaDashboard = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = ''
          Provision the pifinder-differ dashboard in services.grafana. It
          reads the data source named "Prometheus".
        '';
      };
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [{
      assertion = attic.enable;
      message = "services.pifinder-differ needs services.pifinder-attic on the same host.";
    }];

    systemd.services.pifinder-differ = {
      description = "PiFinder delta (zstd --patch-from) server";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" "atticd.service" ];
      wants = [ "network-online.target" ];

      # curl fetches NARs from loopback atticd and from cache.nixos.org, zstd
      # patches, xz/bzip2 decode upstream NARs, df disk guard.
      path = [ pkgs.curl pkgs.zstd pkgs.xz pkgs.bzip2 pkgs.coreutils pkgs.bash ];

      environment = {
        DIFFER_LISTEN = listen;
        DIFFER_STATE_DIR = "/var/lib/pifinder-differ";
        DIFFER_WORKERS = toString cfg.workers;
        DIFFER_ATTIC_URL = "http://127.0.0.1:${toString attic.port}";
        DIFFER_ATTIC_DB = "/var/lib/atticd/server.db";
        DIFFER_CACHES = lib.concatStringsSep " " cfg.caches;
        DIFFER_UPSTREAM_URL = cfg.upstreamUrl;
        # The release lane's working set is about 1 GiB per toplevel diff, so
        # the 10 GiB default holds about 10 diffs.
        DIFFER_NAR_CACHE_BYTES = toString cfg.narCacheBytes;
      };

      serviceConfig = {
        ExecStart = "${cfg.package}/bin/pifinder-differ";
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
        MemoryHigh = cfg.memoryHigh;
        MemoryMax = cfg.memoryMax;
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
        WARM_DIFFER_URL = "http://${listen}";
        # <owner>/<repo>/<branch>/<path>, read with git ls-remote and
        # raw.githubusercontent.com by commit (differ-warm.py).
        WARM_MANIFESTS = lib.concatStringsSep " " cfg.warm.manifests;
        WARM_HUBS = lib.concatStringsSep " " cfg.warm.hubs;
        # git looks for its config in HOME.
        HOME = "/var/lib/pifinder-differ-warm";
      };
      path = [ pkgs.git ];
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

    services.caddy = lib.mkIf cfg.caddy.enable {
      enable = lib.mkDefault true;
      # Only the device-facing routes are public. /warm, /status, /pairs and
      # /metrics stay on loopback (curl on the host).
      virtualHosts.${cfg.domain}.extraConfig = ''
        @public path /delta /deltas /update-start /blobs/* /health

        # handle blocks, not a bare `respond`: respond sorts BEFORE
        # reverse_proxy in Caddy's directive order and would 403 everything.
        handle @public {
          # Patch blobs are content-addressed (base-hash_target-hash) and
          # immutable, so any cache can keep them for ever.
          @blobs path /blobs/*
          header @blobs Cache-Control "public, max-age=31536000, immutable"
          # /deltas answers with a stream of JSON lines, one per patch as it
          # is ready: pass each line on at once, do not buffer.
          reverse_proxy ${listen} {
            flush_interval -1
          }
        }
        handle {
          respond 403
        }
      '';
    };

    services.prometheus.scrapeConfigs = lib.mkIf cfg.monitoring.prometheus [{
      # Jobs per lane and result, compute time, patch and NAR bytes, answers
      # to devices, queue lengths and cache sizes.
      job_name = "pifinder-differ";
      static_configs = [{ targets = [ listen ]; }];
      scrape_interval = "30s";
    }];

    services.grafana.provision.dashboards.settings.providers =
      lib.mkIf cfg.monitoring.grafanaDashboard [{
        name = "pifinder-differ";
        options.path = ./grafana;
      }];
  };
}
