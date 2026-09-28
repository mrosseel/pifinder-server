# HTTPS front (Caddy)

The services in this repo bind loopback only and expect a reverse proxy to
terminate TLS. With `caddy.enable` (the default) each module writes its own
Caddy vhost. This file shows the same config for a host that runs its own
proxy (`caddy.enable = false`).

## cache.pifinder.eu → atticd (127.0.0.1:8080)

```caddyfile
cache.pifinder.eu {
  reverse_proxy 127.0.0.1:8080 {
    # Don't buffer request bodies — push uploads can be many MB.
    flush_interval -1
  }
}
```

## deltas.pifinder.eu → pifinder-differ (127.0.0.1:8090)

Only the device-facing routes are public. `/warm`, `/status`, `/pairs` and
`/metrics` are operator surface and stay loopback-only (curl on the host over
SSH).

```caddyfile
deltas.pifinder.eu {
  @public path /delta /deltas /update-start /blobs/* /health

  # handle blocks, not a bare `respond`: respond sorts BEFORE reverse_proxy
  # in Caddy's directive order and would 403 everything.
  handle @public {
    # Patch blobs are content-addressed (base-hash_target-hash) and
    # immutable — cache forever, anywhere.
    @blobs path /blobs/*
    header @blobs Cache-Control "public, max-age=31536000, immutable"
    # /deltas answers with a stream of JSON lines, one per patch as it is
    # ready: pass each line on at once, do not buffer.
    reverse_proxy 127.0.0.1:8090 {
      flush_interval -1
    }
  }
  handle {
    respond 403
  }
}
```

Notes:

- The differ trusts `X-Forwarded-For` (Caddy sets it) for its per-IP bucket;
  this is safe only because the differ listens on loopback. Never expose
  port 8090 directly.
- Both vhosts need DNS A records; ACME issuance is automatic once they
  resolve.
- Requests without the header (direct loopback curl) bypass rate limiting —
  that is the operator path.
