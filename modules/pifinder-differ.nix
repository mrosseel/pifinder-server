{ config, lib, pkgs, ... }:

# pifinder-differ — on-demand + self-warming zstd delta server for PiFinder
# NixOS updates. Decision record: docs/adr/0031-delta-updates-on-demand-differ.md
# in the PiFinder repo (nixos branch). Sits beside Attic (modules/attic.nix)
# and serves byte-level patches between store-path NARs:
#
#   POST /delta   device names a target + the bases it holds → 200/202/204
#   POST /warm    precompute all stem-paired deltas between two toplevels
#   GET  /pairs   every computed pair with sizes/ratios (observability)
#   GET  /status  queues, counters, warm-run progress
#   GET  /blobs/* the patch blobs
#
# v0.2 works from the cache itself, not from a nix store: closures,
# references and chunk lists come from atticd's SQLite DB (read-only),
# candidate bases are ranked by FastCDC chunk overlap, and NAR bytes are
# fetched from atticd over loopback. Patches are NAR-to-NAR (device dumps
# its base with `nix-store --dump`), so GC holes in the cache degrade
# per-path instead of failing whole closures.
#
# Test phase: bound to loopback only — reach it via SSH port-forward or
# curl on the host. No Caddy vhost until the device-side applier lands.
#
# The server hosts other apps. All compute is idle-priority and one core is
# always left free: 2 workers on this 4-core / 6 GiB machine (zstd -19
# with a large window peaks ~800 MiB per job).

let
  pifinder-differ = pkgs.rustPlatform.buildRustPackage {
    pname = "pifinder-differ";
    version = "0.3.0";
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
    after = [ "network-online.target" ];
    wants = [ "network-online.target" ];

    # curl fetches NARs from loopback atticd, zstd patches, df disk guard.
    path = [ pkgs.curl pkgs.zstd pkgs.coreutils pkgs.bash ];

    environment = {
      DIFFER_LISTEN = "127.0.0.1:8090";
      DIFFER_STATE_DIR = "/var/lib/pifinder-differ";
      DIFFER_WORKERS = "2";
      DIFFER_ATTIC_URL = "http://127.0.0.1:8080";
      DIFFER_ATTIC_DB = "/var/lib/atticd/server.db";
      DIFFER_CACHES = "pifinder pifinder-release";
      # Local LRU cache of decompressed NARs under the state dir. v0.2 was
      # S3-fetch-bound (~600 s for one 202 MiB pair); with the release lane's
      # working set (~1 GiB per toplevel diff) this holds ~10 diffs. Disk has
      # ~270 G free — raise if warm runs still show cold fetches.
      DIFFER_NAR_CACHE_BYTES = toString (10 * 1024 * 1024 * 1024);
    };

    serviceConfig = {
      ExecStart = "${pifinder-differ}/bin/pifinder-differ";
      StateDirectory = "pifinder-differ";
      Restart = "on-failure";
      RestartSec = 5;

      # Root only because /var/lib/atticd is 0700 root:root and the differ
      # reads server.db in there. Nothing else needs privilege any more
      # (v0.2 dropped all nix-store use) — a dedicated user + a group on
      # the atticd dir would let this drop root entirely.

      # Never compete with the co-hosted services.
      Nice = 19;
      IOSchedulingClass = "idle";
      CPUWeight = 20;
      MemoryHigh = "2500M";
      MemoryMax = "3G";
    };
  };
}
