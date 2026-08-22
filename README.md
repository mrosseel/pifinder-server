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

The HTTPS front (Caddy) is configured on the host, not here — see
[docs/caddy.md](docs/caddy.md) for the required vhosts.

## Consuming

```nix
inputs.pifinder-server.url = "github:mrosseel/pifinder-server";
# in the host's module list:
inputs.pifinder-server.nixosModules.default
```

## Developing the differ

```
nix build .#pifinder-differ
cd differ && cargo test
```
