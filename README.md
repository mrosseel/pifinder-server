# pifinder-server

NixOS configuration and code for the server side of PiFinder's update
infrastructure. Two components, both consumed as NixOS modules by the host's
own flake:

- **`modules/attic.nix`** — the Attic binary cache behind
  `https://cache.pifinder.eu` (caches `pifinder` for dev/PR builds with 90-day
  retention, `pifinder-release` retained forever; chunk store on S3).
  Bootstraps its own JWT secret, caches and CI token declaratively; the one
  manual step after first deploy is pasting `/var/lib/atticd/ci-token` into
  the PiFinder repo's `ATTIC_TOKEN` secret.
- **`modules/pifinder-differ.nix` + `differ/`** — the delta update server
  behind `https://deltas.pifinder.eu`. Computes byte-level
  `zstd --patch-from` patches between store-path NARs so devices download the
  real content difference (measured: 255 MiB of changed paths → 1.5 MiB of
  patches). Attic-native: closures, references and chunk-overlap ranking come
  from atticd's SQLite DB read-only; NAR bytes from atticd over loopback with
  a local LRU cache. Requests are budgeted per update session, sized from the
  target closure. Design record: ADR 0031 in the PiFinder repo
  (`docs/adr/0031-delta-updates-on-demand-differ.md`); device side:
  `python/PiFinder/delta_updates.py`.

## Deploying on a server

Import the module and switch the two services on. The defaults are the
values of the pifinder.eu server; a second server sets its own domains, S3
bucket and manifests.

```nix
# flake.nix of the host
inputs.pifinder-server.url = "github:mrosseel/pifinder-server";
# in the host's module list:
inputs.pifinder-server.nixosModules.default
```

```nix
services.pifinder-attic = {
  enable = true;
  domain = "cache.example.org";
  storage = { type = "s3"; region = "eu-west-1"; bucket = "example-pifinder-cache"; };
};
services.pifinder-differ = {
  enable = true;
  domain = "deltas.example.org";
  warm.manifests = [ "example/PiFinder/nixos-manifest/update-manifest.json" ];
};
```

[examples/host.nix](examples/host.nix) is a full example with the steps
before and after the first deploy. `nix flake check` evaluates it.

What the modules do:

- **Caddy:** each service writes its own vhost and enables Caddy
  (`caddy.enable`, on by default). For another proxy, set it to false and
  copy the config from [docs/caddy.md](docs/caddy.md).
- **Monitoring:** `services.pifinder-differ.monitoring.prometheus` adds the
  scrape job, and `monitoring.grafanaDashboard` provisions the dashboard.
  Both are off by default, because they need Prometheus and Grafana on the
  host.
- **Secrets:** the module never holds them. The S3 keys go in
  `/var/lib/atticd/env` before the first start. The CI token appears in
  `/var/lib/atticd/ci-token` after it.

The differ must run on the same host as Attic: it reads the Attic database
and fetches NARs from Attic over loopback.

## Developing the differ

```
nix build .#pifinder-differ
cd differ && cargo test
```
