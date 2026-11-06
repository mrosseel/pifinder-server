# AWS host: `pifinder-aws`

This host serves `cache.pifinder.io` and `deltas.pifinder.io`. The `.eu` names
are aliases. `modules/site.nix` holds the PiFinder values. This directory adds
only the machine: EC2 boot, disk, SSH and users.

The flake output is `nixosConfigurations.pifinder-aws` (aarch64-linux).

## 1. Make the instance

| Setting        | Value                                                    |
|----------------|----------------------------------------------------------|
| Region         | `us-west-1`, the same region as the S3 bucket            |
| Instance type  | `t4g.large` (2 cores, 8 GiB). For 2 patch jobs, use `t4g.xlarge` and set `services.pifinder-differ.workers = 2` |
| AMI            | Debian 12 arm64 (user `admin`). Any AMI with SSH and sudo works |
| Root volume    | 40 GiB gp3. The differ keeps a 10 GiB NAR cache          |
| Security group | TCP 22 from your address. TCP 80 and 443 from everywhere |
| Elastic IP     | Yes. The DNS records point to it                         |

The instance needs no IAM role. The S3 keys come with the state from the
current server (step 4).

## 2. Install NixOS

From this repository:

```bash
nix run github:nix-community/nixos-anywhere -- \
  --flake .#pifinder-aws \
  --build-on remote \
  --target-host admin@<elastic-ip>
```

`--build-on remote` builds on the instance. An x86 machine would build the
aarch64 system under emulation, which is slow.

nixos-anywhere erases `/dev/nvme0n1`. After the reboot, log in with
`ssh mike@<elastic-ip>`.

## 3. Check the empty host

- `systemctl status atticd` shows `ConditionPathExists=/var/lib/private/atticd/server.db was not met`.
  This is correct. The cache services stay off until the state from step 4 is
  in place. Without that state, the bootstrap units would make new signing
  keys, and no device trusts them.
- Caddy runs, but it gets no certificates until the DNS points here (step 5).

Later changes:

```bash
nixos-rebuild switch --flake .#pifinder-aws \
  --target-host mike@<elastic-ip> --build-host mike@<elastic-ip> --sudo
```

## 4. Move the cache state

Do the steps in this sequence. Only one atticd must use the database and the
S3 bucket at a time.

1. One day before, set the TTL of the four DNS records to 300 seconds.
2. On the current server, stop the services:
   ```bash
   sudo systemctl stop pifinder-differ-warm.timer pifinder-differ atticd
   ```
3. From a machine that can log in to both servers, copy the state:
   ```bash
   ssh mike@pifinder.eu 'sudo tar -C /var/lib/private -czf - atticd' \
     | ssh mike@<elastic-ip> 'sudo install -d -m 0700 /var/lib/private && sudo tar -C /var/lib/private -xzf -'
   ```
   The copy holds `server.db` with the cache signing keys, and `env` with the
   JWT secret and the S3 keys. So `ATTIC_TOKEN` and the device trust do not
   change. The 11 GiB differ cache is not copied. It fills again.
4. On the AWS host, start the services and check the cache:
   ```bash
   sudo systemctl start atticd pifinder-differ pifinder-differ-warm.timer
   curl -s http://127.0.0.1:8080/pifinder/nix-cache-info
   ```

## 5. Change the DNS

- `cache.pifinder.io` and `deltas.pifinder.io`: A record to the Elastic IP
  (brickbots).
- `cache.pifinder.eu` and `deltas.pifinder.eu`: A record to the Elastic IP
  (pifinder.eu).

Caddy gets the certificates when the names resolve here
(`journalctl -u caddy -f`). Then check:

```bash
curl -s https://cache.pifinder.io/pifinder-release/nix-cache-info
curl -s https://deltas.pifinder.io/health
```

Between step 4.2 and the end of step 5, the cache is down. With a TTL of 300
seconds, that is a few minutes.

## 6. Remove the services from the old server

Remove `pifinder-site` (or the two services) from the old server's
configuration and deploy it. Never start atticd there again. Its GC would run
on the same S3 bucket with an old database.

## Back out

Before step 6, and only if the AWS host took no pushes: stop the services on
the AWS host, start them on the old server, and point the DNS back. After a
push to the AWS host, copy its state back first (step 4.3, in the other
direction).
