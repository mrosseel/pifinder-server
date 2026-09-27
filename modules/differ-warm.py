"""Pre-warm pifinder-differ for new builds in the PiFinder update manifests.

Runs from a systemd timer. For every manifest entry whose label moves to a new
store path (a PR or trunk rebuild), and for each release after the one before
it in its channel, it asks the differ (POST /warm on loopback) to compute the
patches from the old build to the new one. Devices that upgrade along that
step then get their patches at once instead of a 202 "computing".

The manifests are read through the GitHub contents API, not
raw.githubusercontent.com, whose CDN serves a branch file up to 5 minutes
stale. Each request sends the last ETag; a 304 answer does not count against
the unauthenticated limit of 60 requests per hour, so a 1-minute timer fits.

State (which store path each label had, which pairs were warmed, and the last
ETag and body per manifest) lives in $STATE_DIRECTORY/seen.json. Standard
library only.
"""

import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

MANIFESTS = os.environ["WARM_MANIFESTS"].split()
DIFFER_URL = os.environ.get("WARM_DIFFER_URL", "http://127.0.0.1:8090")
STATE_FILE = Path(os.environ["STATE_DIRECTORY"]) / "seen.json"
STORE_PATH = re.compile(r"/nix/store/[a-z0-9]{32}-[^/]+")
# Warmed pairs to remember. Old ones fall out; re-warming is harmless.
MAX_WARMED = 500
# Switches between entries: a device on a hub build (the nixos branch) that
# switches to a PR build, or back. Warmed both ways, only for PR builds of
# the last CROSS_DAYS days, and for at most CROSS_MAX of them (newest first).
HUBS = os.environ.get("WARM_HUBS", "PR#379").split()
CROSS_DAYS = int(os.environ.get("WARM_CROSS_DAYS", "7"))
CROSS_MAX = int(os.environ.get("WARM_CROSS_MAX", "8"))


def fetch_json(url: str, cached: dict):
    """The manifest at `url`. `cached` holds the last ETag and body for it and
    is updated; a 304 answer reuses the cached body."""
    headers = {"accept": "application/vnd.github.raw+json"}
    if cached.get("etag"):
        headers["if-none-match"] = cached["etag"]
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode()
            cached["etag"] = resp.headers.get("etag", "")
            cached["body"] = body
    except urllib.error.HTTPError as exc:
        if exc.code != 304 or "body" not in cached:
            raise
    return json.loads(cached["body"])


def warm(base: str, target: str) -> bool:
    body = json.dumps({"base_toplevel": base, "target_toplevel": target}).encode()
    req = urllib.request.Request(
        f"{DIFFER_URL}/warm",
        data=body,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status in (200, 202)
    except urllib.error.HTTPError as exc:
        return exc.code == 202
    except (urllib.error.URLError, OSError) as exc:
        print(f"warm {base} -> {target}: {exc}")
        return False


def build_key(entry: dict) -> str:
    """What stays the same across rebuilds: a PR or trunk label carries the
    short commit ("PR#379-56046f2", "nixos-d1657e6"), so drop that suffix."""
    label = entry.get("label") or ""
    if entry.get("kind") in ("pr", "trunk") and "-" in label:
        return label.rsplit("-", 1)[0]
    return label


def built_at(entry: dict) -> datetime | None:
    try:
        return datetime.fromisoformat(entry.get("built_at") or "")
    except ValueError:
        return None


def cross_pairs(builds: list[dict], now: datetime) -> set[tuple[str, str]]:
    """Hub build <-> recent PR builds of one channel, both ways."""
    hubs = [e for e in builds if build_key(e) in HUBS]
    since = now - timedelta(days=CROSS_DAYS)
    recent = [
        e
        for e in builds
        if e.get("kind") == "pr"
        and build_key(e) not in HUBS
        and (built_at(e) or since) > since
    ]
    recent.sort(key=lambda e: built_at(e) or since, reverse=True)
    pairs = set()
    for hub in hubs:
        for spoke in recent[:CROSS_MAX]:
            pairs.add((hub["store_path"], spoke["store_path"]))
            pairs.add((spoke["store_path"], hub["store_path"]))
    return pairs


def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except (OSError, ValueError):
        return {"seen": {}, "warmed": [], "manifests": {}}


def main() -> None:
    state = load_state()
    seen: dict = state.get("seen", {})
    # Oldest first, so the list can be cut from the front.
    warmed = [tuple(p) for p in state.get("warmed", [])]
    manifests: dict = state.get("manifests", {})
    pairs: set = set()

    for url in MANIFESTS:
        try:
            manifest = fetch_json(url, manifests.setdefault(url, {}))
        except (urllib.error.URLError, OSError, ValueError) as exc:
            print(f"manifest {url}: {exc}")
            continue
        for channel, entries in (manifest.get("channels") or {}).items():
            builds = [
                e
                for e in entries or []
                if e.get("available") and STORE_PATH.fullmatch(e.get("store_path") or "")
            ]
            # A label that moves to a new build: old build -> new build.
            for e in builds:
                key = f"{url}#{channel}#{build_key(e)}"
                old, new = seen.get(key), e["store_path"]
                if old and old != new:
                    pairs.add((old, new))
                seen[key] = new
            # Releases, newest first: each from the release before it.
            if channel in ("stable", "beta"):
                for newer, older in zip(builds, builds[1:]):
                    pairs.add((older["store_path"], newer["store_path"]))
            pairs |= cross_pairs(builds, datetime.now(timezone.utc))

    for base, target in sorted(pairs - set(warmed)):
        if warm(base, target):
            print(f"warming {base} -> {target}")
            warmed.append((base, target))

    state = {"seen": seen, "warmed": warmed[-MAX_WARMED:], "manifests": manifests}
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1))
    tmp.replace(STATE_FILE)


if __name__ == "__main__":
    main()
