"""Pre-warm pifinder-differ for new builds in the PiFinder update manifests.

Runs from a systemd timer. For every manifest entry whose label moves to a new
store path (a PR or trunk rebuild), and for each release after the one before
it in its channel, it asks the differ (POST /warm on loopback) to compute the
patches from the old build to the new one. Devices that upgrade along that
step then get their patches at once instead of a 202 "computing".

State (which store path each label had, and which pairs were warmed) lives in
$STATE_DIRECTORY/seen.json. Standard library only.
"""

import json
import os
import re
import urllib.error
import urllib.request
from pathlib import Path

MANIFESTS = os.environ["WARM_MANIFESTS"].split()
DIFFER_URL = os.environ.get("WARM_DIFFER_URL", "http://127.0.0.1:8090")
STATE_FILE = Path(os.environ["STATE_DIRECTORY"]) / "seen.json"
STORE_PATH = re.compile(r"/nix/store/[a-z0-9]{32}-[^/]+")
# Warmed pairs to remember. Old ones fall out; re-warming is harmless.
MAX_WARMED = 500


def fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


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


def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except (OSError, ValueError):
        return {"seen": {}, "warmed": []}


def main() -> None:
    state = load_state()
    seen: dict = state.get("seen", {})
    # Oldest first, so the list can be cut from the front.
    warmed = [tuple(p) for p in state.get("warmed", [])]
    pairs: set = set()

    for url in MANIFESTS:
        try:
            manifest = fetch_json(url)
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

    for base, target in sorted(pairs - set(warmed)):
        if warm(base, target):
            print(f"warming {base} -> {target}")
            warmed.append((base, target))

    state = {"seen": seen, "warmed": warmed[-MAX_WARMED:]}
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1))
    tmp.replace(STATE_FILE)


if __name__ == "__main__":
    main()
