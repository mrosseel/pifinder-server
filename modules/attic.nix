{ config, lib, pkgs, ... }:

# Attic Nix binary cache for PiFinder NixOS distribution.
# Served at https://<domain> (default cache.pifinder.eu). With caddy.enable
# the module writes the Caddy vhost.
# See docs/adr/0004-attic-binary-cache.md in the PiFinder repo for the rationale.
#
# Four systemd oneshots make this fully declarative — no manual SSH
# atticadm steps. Each runs at most once, guarded by ConditionPathExists:
#
#   atticd-bootstrap-secret        →  generates the RS256 JWT secret
#                                     at /var/lib/atticd/env
#   atticd-bootstrap-cache         →  creates the public 'pifinder' cache
#                                     (dev/nightly builds)
#   atticd-bootstrap-cache-release →  creates the public 'pifinder-release'
#                                     cache (retained release closures —
#                                     PiFinder ADR 0004)
#   atticd-bootstrap-token         →  mints a 5-year CI push/pull JWT for
#                                     both caches, at /var/lib/atticd/ci-token
#
# The cache names pifinder and pifinder-release are fixed: the PiFinder CI
# and the devices use them.
#
# After deploy, the one-and-only manual step is to read the CI token
# (sudo cat /var/lib/atticd/ci-token) and paste it into the PiFinder
# repo's GitHub Actions secrets as ATTIC_TOKEN. Cache public keys are
# fetchable from https://<domain>/pifinder and https://<domain>/pifinder-release.
#
# Wiping any of the marker files (.pifinder-cache-bootstrapped,
# .pifinder-token-bootstrapped) re-runs the corresponding step, which
# would mint a NEW token and invalidate the old one. So don't.

let
  cfg = config.services.pifinder-attic;
  loopback = "127.0.0.1:${toString cfg.port}";
  # Marker of the retention step: "90 days" gives .pifinder-dev-retention-90d.
  retentionMarker = lib.replaceStrings [ " days" " " ] [ "d" "-" ] cfg.devRetention;
in
{
  options.services.pifinder-attic = {
    enable = lib.mkEnableOption "the Attic binary cache for PiFinder builds";

    domain = lib.mkOption {
      type = lib.types.str;
      default = "cache.pifinder.eu";
      description = "Public host name of the cache. Attic puts it in its replies.";
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = 8080;
      description = "Loopback port of atticd.";
    };

    storage = lib.mkOption {
      type = lib.types.attrsOf lib.types.anything;
      default = {
        type = "s3";
        region = "us-west-1";
        bucket = "pifinder-nix-cache";
      };
      description = ''
        atticd chunk store (the `storage` section of its settings). The S3
        keys go in /var/lib/atticd/env as AWS_ACCESS_KEY_ID and
        AWS_SECRET_ACCESS_KEY, never here. For a local disk store use
        `{ type = "local"; path = "/var/lib/atticd/storage"; }`.
      '';
    };

    devRetention = lib.mkOption {
      type = lib.types.str;
      default = "90 days";
      description = ''
        Retention of the dev cache `pifinder`. The `pifinder-release` cache
        is kept for ever.
      '';
    };

    caddy.enable = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Write the Caddy vhost for `domain` (and enable Caddy).";
    };
  };

  config = lib.mkIf cfg.enable {
    services.atticd = {
      enable = true;
      environmentFile = "/var/lib/atticd/env";

      settings = {
        listen = loopback;

        # Public URL clients use — must match the Caddy vhost. Attic embeds
        # this in cache-info responses and signed URLs.
        api-endpoint = "https://${cfg.domain}/";

        # Default chunking thresholds from the upstream nixosModule (16K min,
        # 64K avg, 256K max) are appropriate for our closure sizes; left at
        # the module defaults.

        # Chunk store: AWS S3 (ADR 0001 named S3/R2 as the eventual backend).
        # Overrides the module's mkDefault local-disk store. Credentials are
        # NOT here — AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY live in the
        # environmentFile (/var/lib/atticd/env), kept out of the Nix store like
        # the RS256 secret. No `endpoint`: real AWS derives s3.<region>....
        storage = cfg.storage;

        # Lifetime management. atticd runs GC on this interval and deletes store
        # paths older than each cache's retention period. We deliberately set NO
        # global default-retention-period, so a cache without a per-cache period
        # (pifinder-release) is kept forever; the dev `pifinder` cache gets a
        # finite period applied by atticd-set-dev-retention below.
        garbage-collection = {
          interval = "1 day";
        };
      };
    };

    # Bootstrap the RS256 JWT secret on first start. Idempotent: only runs
    # when /var/lib/atticd/env is missing, so existing tokens survive
    # rebuilds.
    systemd.services.atticd-bootstrap-secret = {
      description = "Generate atticd RS256 JWT secret on first start";
      before = [ "atticd.service" ];
      wantedBy = [ "atticd.service" ];
      unitConfig.ConditionPathExists = "!/var/lib/atticd/env";
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
      };
      path = with pkgs; [ openssl coreutils ];
      script = ''
        install -d -m 0700 -o root -g root /var/lib/atticd
        umask 077
        SECRET=$(openssl genrsa -traditional 4096 2>/dev/null | base64 -w0)
        printf 'ATTIC_SERVER_TOKEN_RS256_SECRET_BASE64="%s"\n' "$SECRET" \
          > /var/lib/atticd/env
        chown root:root /var/lib/atticd/env
        chmod 0600 /var/lib/atticd/env
      '';
    };

    # Create the public 'pifinder' cache on first start.
    #
    # atticadm itself only has `make-token` and `help` subcommands — cache
    # creation is done by the `attic` *client* against the running atticd
    # API. So we mint a one-shot setup token with create_cache /
    # configure_cache rights, log into the local loopback endpoint with it,
    # and run `attic cache create … --public`. The setup token expires in
    # 5 minutes and is never written to disk.
    #
    # Marker is `-v2` because v1 (which ran `atticadm create-cache`, a
    # non-existent subcommand) silently no-op'd; renaming forces this
    # corrected service to run on the next rebuild.
    systemd.services.atticd-bootstrap-cache = {
      description = "Create the pifinder cache on first start";
      after = [ "atticd.service" ];
      requires = [ "atticd.service" ];
      wantedBy = [ "multi-user.target" ];
      unitConfig.ConditionPathExists = "!/var/lib/atticd/.pifinder-cache-bootstrapped-v2";
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
      };
      path = with pkgs; [ curl coreutils attic-client ];
      script = ''
        set -euo pipefail
        # Wait for atticd to bind 127.0.0.1:8080.
        for i in $(seq 1 60); do
          if curl -fsS -o /dev/null http://${loopback}/ ; then
            break
          fi
          sleep 1
        done
        SETUP_TOKEN=$(/run/current-system/sw/bin/atticd-atticadm make-token \
          --sub bootstrap \
          --validity 5m \
          --create-cache pifinder \
          --configure-cache pifinder \
          --push pifinder \
          --pull pifinder)
        # Keep attic client state out of root's $HOME — fresh per run.
        export ATTIC_CONFIG_DIR=$(mktemp -d)
        trap 'rm -rf "$ATTIC_CONFIG_DIR"' EXIT
        attic login local http://${loopback} "$SETUP_TOKEN"
        # `cache create --public` sets the public flag on creation.
        # `|| true` so re-runs are idempotent (existing-cache error is
        # benign — the marker below ensures we don't loop on it forever).
        # We deliberately do NOT call `attic cache configure --public`
        # afterwards: it requires a stronger permission than
        # `configure_cache` alone and would fail even when the state is
        # already correct.
        attic cache create local:pifinder --public || true
        touch /var/lib/atticd/.pifinder-cache-bootstrapped-v2
      '';
    };

    # Create the public 'pifinder-release' cache on first start. Mirrors
    # atticd-bootstrap-cache. Holds tagged release closures that must outlive the
    # dev cache's retention (PiFinder ADR 0004). Devices pull the stable channel
    # from it; a Pi upgrading months after a release must still resolve the path.
    #
    # Retention is left at the default ("Global"), and global GC is off, so today
    # nothing is collected. IMPORTANT: when you eventually prune the dev cache, do
    # it PER-CACHE — `attic cache configure local:pifinder --retention-period <N>`
    # — never via a global retention, which would also prune this cache while it
    # stays at "Global".
    systemd.services.atticd-bootstrap-cache-release = {
      description = "Create the pifinder-release cache on first start";
      after = [ "atticd.service" ];
      requires = [ "atticd.service" ];
      wantedBy = [ "multi-user.target" ];
      unitConfig.ConditionPathExists = "!/var/lib/atticd/.pifinder-release-cache-bootstrapped";
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
      };
      path = with pkgs; [ curl coreutils attic-client ];
      script = ''
        set -euo pipefail
        for i in $(seq 1 60); do
          if curl -fsS -o /dev/null http://${loopback}/ ; then
            break
          fi
          sleep 1
        done
        SETUP_TOKEN=$(/run/current-system/sw/bin/atticd-atticadm make-token \
          --sub bootstrap \
          --validity 5m \
          --create-cache pifinder-release \
          --configure-cache pifinder-release \
          --push pifinder-release \
          --pull pifinder-release)
        export ATTIC_CONFIG_DIR=$(mktemp -d)
        trap 'rm -rf "$ATTIC_CONFIG_DIR"' EXIT
        attic login local http://${loopback} "$SETUP_TOKEN"
        attic cache create local:pifinder-release --public || true
        touch /var/lib/atticd/.pifinder-release-cache-bootstrapped
      '';
    };

    # Mint a long-lived CI token on first start and write it to a
    # root-readable file. Operator reads it once via `sudo cat` and pastes
    # into the PiFinder repo's GitHub Actions secrets as ATTIC_TOKEN.
    systemd.services.atticd-bootstrap-token = {
      description = "Mint the CI push/pull token on first start";
      after = [ "atticd-bootstrap-cache.service" "atticd-bootstrap-cache-release.service" ];
      requires = [ "atticd-bootstrap-cache.service" "atticd-bootstrap-cache-release.service" ];
      wantedBy = [ "multi-user.target" ];
      # -v2: the CI token now also carries pifinder-release push/pull (the release
      # workflow pushes there). Bumping the marker re-mints the token ONCE — the
      # operator must then re-paste it into the PiFinder repo's ATTIC_TOKEN secret.
      # The old pifinder-only token keeps working for the dev cache but cannot push
      # releases, so CI's `attic push pifinder:pifinder-release` 403s until re-paste.
      unitConfig.ConditionPathExists = "!/var/lib/atticd/.pifinder-token-bootstrapped-v2";
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
      };
      path = with pkgs; [ coreutils ];
      script = ''
        set -euo pipefail
        umask 077
        /run/current-system/sw/bin/atticd-atticadm make-token \
          --sub ci \
          --validity 5y \
          --pull pifinder \
          --push pifinder \
          --pull pifinder-release \
          --push pifinder-release \
          > /var/lib/atticd/ci-token
        chown root:root /var/lib/atticd/ci-token
        chmod 0600 /var/lib/atticd/ci-token
        touch /var/lib/atticd/.pifinder-token-bootstrapped-v2
      '';
    };

    # Apply a finite retention period to the dev `pifinder` cache so it does not
    # grow without bound (testable-PR + trunk + beta closures all land here).
    # pifinder-release is left at "Global Default" (no period → kept forever, the
    # whole point of the retained cache). Marker-guarded; the period below is the
    # source of truth — bump the marker suffix to re-apply a changed value.
    # NB: retention config requires the dedicated configure-cache-retention
    # permission, separate from configure-cache.
    systemd.services.atticd-set-dev-retention = {
      description = "Set the pifinder dev cache retention period (${cfg.devRetention})";
      after = [ "atticd-bootstrap-cache.service" ];
      requires = [ "atticd-bootstrap-cache.service" ];
      wantedBy = [ "multi-user.target" ];
      unitConfig.ConditionPathExists = "!/var/lib/atticd/.pifinder-dev-retention-${retentionMarker}";
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
      };
      path = with pkgs; [ curl coreutils attic-client ];
      script = ''
        set -euo pipefail
        for i in $(seq 1 60); do
          if curl -fsS -o /dev/null http://${loopback}/ ; then break; fi
          sleep 1
        done
        SETUP_TOKEN=$(/run/current-system/sw/bin/atticd-atticadm make-token \
          --sub set-retention \
          --validity 5m \
          --configure-cache-retention pifinder \
          --configure-cache pifinder \
          --pull pifinder)
        export ATTIC_CONFIG_DIR=$(mktemp -d)
        trap 'rm -rf "$ATTIC_CONFIG_DIR"' EXIT
        attic login local http://${loopback} "$SETUP_TOKEN"
        attic cache configure local:pifinder --retention-period "${cfg.devRetention}"
        touch /var/lib/atticd/.pifinder-dev-retention-${retentionMarker}
      '';
    };

    services.caddy = lib.mkIf cfg.caddy.enable {
      enable = lib.mkDefault true;
      # Clients are the Nix daemon and attic push, not browsers: no HTML
      # headers, and no cap on the size of an upload.
      virtualHosts.${cfg.domain}.extraConfig = ''
        reverse_proxy ${loopback} {
          # Don't buffer request bodies: push uploads can be many MB.
          flush_interval -1
        }
      '';
    };
  };
}
