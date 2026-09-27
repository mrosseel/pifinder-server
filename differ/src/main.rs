// pifinder-differ v0.4 — on-demand + self-warming binary delta server for the
// PiFinder NixOS update transport.
//
// Runs beside atticd and works from the caches themselves, not from a nix store:
//   - metadata (closures, references, NAR sizes) comes from atticd's SQLite
//     database, opened read-only. `attic push` skips paths that the upstream
//     cache (cache.nixos.org) already has, so the paths that attic does not
//     hold come from the upstream narinfo, kept on disk;
//   - candidate bases are ranked by FastCDC chunk overlap (Jaccard) straight
//     from the DB — no bytes fetched to pick a winner. Upstream paths have no
//     chunk list and rank by NAR size closeness;
//   - NAR bytes are fetched from atticd over loopback or from the upstream
//     cache, and patched NAR-to-NAR with zstd --patch-from. NARs are
//     canonical on both ends (`nix-store --dump` on the device), so no
//     export-stream/deriver nondeterminism.
//
// Every pair is first patched at FAST_LEVEL, so a device gets it in seconds.
// A refine job then patches the same pair again at the final level and
// replaces the blob when the result is smaller.
//
// Endpoints:
//   POST /update-start — open a session; an optional base_toplevel starts
//                  a warm run for exactly that step, in the demand lane
//   POST /delta  — a device names a target and the bases it holds (demand)
//   POST /deltas — the same for many targets in one request; the answer is a
//                  stream of JSON lines, one per target as soon as its patch
//                  is ready, with a heartbeat line while it waits
//   POST /warm   — enqueue every stem-paired path between two toplevels
//   GET  /pairs  — every computed pair with sizes/ratios
//   GET  /status — queues, counters, warm-run progress
//   GET  /metrics — the same counters for Prometheus (loopback only)
//   GET  /blobs/<base>_<target>.zst
//
// Demand jobs always run before warm jobs, and warm jobs before refine jobs. All compute runs at the unit's
// idle CPU/IO priority so co-hosted services are never starved.
//
// Device applies a patch as:
//   nix-store --dump $BASE > base.nar
//   zstd -d --long=$window_log --patch-from=base.nar patch.zst -o new.nar
//   sha256sum new.nar == nar_sha256, then import with references+deriver
//   from the /delta response.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use axum::extract::{Path as AxPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

const ALGO: &str = "zstd-patch-from-nar-v2";
const MIN_WINDOW_LOG: u32 = 27; // 128 MiB floor; raised per pair when NARs are bigger
const MAX_WINDOW_LOG: u32 = 30; // 1 GiB — refuse pairs the device could never decode
// First pass. On a 134 MiB python NAR: level 3 takes 0.4 s for a 41.6 MB
// patch, level 19 takes 40 s for 39.0 MB. The refine pass makes up the rest.
const FAST_LEVEL: u32 = 3;
const FINAL_LEVEL_SMALL: u32 = 19; // targets below the split
const FINAL_LEVEL_LARGE: u32 = 12; // large targets: 19 costs minutes for ~% gain
const LARGE_TARGET_BYTES: u64 = 20 * 1024 * 1024;
const KEEP_RATIO: f64 = 0.40; // patch bigger than 40% of the NAR: not worth it
// Best-candidate chunk overlap below this: the pair is effectively a full
// download (e.g. a migrating device with no shared history). Decide that from
// the DB alone — never fetch NARs for it, so hopeless pairs cannot evict the
// release lane's working set from the NAR cache.
//
// Only meaningful when the target spans enough chunks: a NAR below ~8 chunks
// (FastCDC avg 64 KiB) is one-or-few chunks, where a 2-byte edit reads as
// zero overlap even though the byte-level patch would be ~100 B. Small
// targets skip the floor and rank by NAR-size closeness instead.
const MIN_CHUNK_OVERLAP: f64 = 0.05;
const MIN_CHUNKS_FOR_OVERLAP: usize = 8;
const MIN_FREE_BYTES: u64 = 5 * 1024 * 1024 * 1024;
// A full nixpkgs step changes about 500 paths; the device asks for all of
// them in one round, and a 503 makes it download the rest in full.
const DEMAND_QUEUE_CAP: usize = 512;
const WARM_QUEUE_CAP: usize = 10_000;
const REFINE_QUEUE_CAP: usize = 10_000;
// Upstream narinfo lookups: parallel transfers per curl call, URLs per call,
// and how long a "not there" answer is kept in memory.
const UPSTREAM_PARALLEL: usize = 32;
const UPSTREAM_BATCH: usize = 256;
const UPSTREAM_NEGATIVE_TTL: Duration = Duration::from_secs(3600);
const UPSTREAM_MEM_MAX: usize = 100_000;
// POST /deltas: targets per request, how long the stream stays open, and how
// often it sends a heartbeat line while targets are pending.
const MAX_STREAM_TARGETS: usize = 4000;
const STREAM_MAX: Duration = Duration::from_secs(90);
const STREAM_HEARTBEAT: Duration = Duration::from_secs(10);
// A device's base_toplevel starts one warm run per step, not one per session.
const PRIORITY_WARM_TTL: Duration = Duration::from_secs(3600);

// Rate limiting is budgeted PER UPDATE, not per IP: a device opens a session
// naming its target toplevel, and the session's request budget is derived
// from that closure's real size — exactly what a legitimate upgrade needs,
// NAT-friendly (every device behind one hotspot gets its own budget), and
// useless to inflate (targets must exist in the attic DB).
//
// The only per-IP bucket left guards the cheap unauthenticated routes
// (/update-start minting and /health). Loopback traffic (no X-Forwarded-For,
// i.e. not via Caddy) bypasses limiting entirely — that is the ops surface.
const RL_IP_BURST: f64 = 100.0;
const RL_IP_REFILL_PER_SEC: f64 = 1.0;
const RL_MINT_COST: f64 = 5.0;
const RL_MAX_TRACKED_IPS: usize = 10_000;
const SESSION_TTL: Duration = Duration::from_secs(3600);
const SESSION_SLACK: i64 = 200; // headroom over 2×closure for retries
const MAX_SESSIONS: usize = 10_000;
const MAX_CLOSURES: usize = 256;
// The device sends 3 candidate bases; more only costs chunk-overlap queries.
const MAX_BASES: usize = 8;

// ---------------------------------------------------------------- state

struct App {
    blob_dir: PathBuf,
    meta_dir: PathBuf,
    tmp_dir: PathBuf,
    nar_cache_dir: PathBuf,
    nar_cache_max: u64,
    attic_url: String,
    attic_db: PathBuf,
    caches: Vec<String>,
    // Upstream binary cache for paths attic does not hold; empty = off.
    upstream_url: String,
    upstream_dir: PathBuf, // narinfo files, one per store path hash
    // Upstream lookups: Some = found, None = not there (expires after
    // UPSTREAM_NEGATIVE_TTL). Bounded by UPSTREAM_MEM_MAX.
    upstream_mem: Mutex<HashMap<String, (Instant, Option<StoreObject>)>>,
    fetch_seq: AtomicU64,
    db: Mutex<Option<Connection>>,
    demand: Mutex<VecDeque<Job>>,
    warm: Mutex<VecDeque<Job>>,
    refine: Mutex<VecDeque<Job>>,
    inflight: Mutex<HashSet<String>>, // target hashes being computed
    // (base, target) toplevel pairs warmed on a device's request, and when.
    priority_warms: Mutex<HashMap<(String, String), Instant>>,
    warm_runs: Mutex<Vec<WarmRun>>,
    jobs_done: AtomicU64,
    jobs_failed: AtomicU64,
    // Woken each time a worker ends a job, for the /deltas streams.
    job_done: tokio::sync::Notify,
    // Prometheus counters: the series ("name{labels}") and its value.
    metrics: Mutex<HashMap<String, f64>>,
    warm_seq: AtomicU64,
    rate: Mutex<HashMap<String, TokenBucket>>,
    sessions: Mutex<HashMap<String, Session>>,
    // Closure store-path hashes per target toplevel hash, shared by every
    // session for that toplevel. Bounded by MAX_CLOSURES.
    closures: Mutex<HashMap<String, Arc<HashSet<String>>>>,
    rate_limited: AtomicU64,
}

struct TokenBucket {
    tokens: f64,
    last: Instant,
}

struct Session {
    budget: i64,
    expires: Instant,
    // Store-path hashes of the target closure: /delta serves only these.
    closure: Arc<HashSet<String>>,
}

#[derive(Clone)]
struct Job {
    target: String,       // full /nix/store/... path
    bases: Vec<String>,   // candidate bases, full paths, best guess first
    source: &'static str, // "demand" | "warm" | "refine"
}

#[derive(Clone, Serialize)]
struct WarmRun {
    id: u64,
    base_toplevel: String,
    target_toplevel: String,
    state: String, // pairing | queued | failed
    priority: bool, // started by a device's /update-start
    paired: usize,
    skipped_existing: usize,
    unpaired: usize,
    error: Option<String>,
    started_unix: u64,
}

#[derive(Serialize, Deserialize, Clone)]
struct PairMeta {
    base: String,
    target: String,
    algo: String,
    window_log: u32,
    level: u32,
    patch_size: u64,
    nar_size: u64,
    nar_sha256: String,
    #[serde(default)]
    references: Vec<String>, // full store paths of the target's references
    #[serde(default)]
    deriver: Option<String>,
    // Jaccard overlap of the chosen base, for observability; -1 = not
    // measured (an upstream path has no chunk list).
    #[serde(default)]
    chunk_overlap: f64,
    // The refine pass ran. The blob is its result, or the fast one when the
    // refined patch was not smaller.
    #[serde(default)]
    refined: bool,
    compute_ms: u64,
    rank_ms: u64,
    candidates_ranked: usize,
    source: String,
    created_unix: u64,
    rejected: bool, // true: patch exceeded KEEP_RATIO, no blob kept
}

// Where a store path's NAR comes from.
#[derive(Clone, Debug, PartialEq)]
enum Origin {
    // A row of attic's `object` × `nar` tables.
    Attic { cache: String, nar_id: i64 },
    // The upstream cache; `url` is relative to upstream_url.
    Upstream { url: String, compression: String },
}

#[derive(Clone, Debug, PartialEq)]
struct StoreObject {
    store_path: String,
    references: Vec<String>, // basenames ("<hash>-<name>")
    deriver: Option<String>,
    nar_size: u64,
    origin: Origin,
}

// ---------------------------------------------------------------- helpers

fn log(msg: &str) {
    eprintln!("[pifinder-differ] {msg}");
}

/// Add `by` to the Prometheus counter `series` ("name{label=\"v\"}").
fn count(app: &App, series: &str, by: f64) {
    *app.metrics.lock().unwrap().entry(series.to_string()).or_insert(0.0) += by;
}

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// "/nix/store/<32hash>-name" -> Some(("<32hash>", "name"))
fn split_store_path(p: &str) -> Option<(String, String)> {
    let base = p.strip_prefix("/nix/store/")?;
    if base.contains('/') {
        return None;
    }
    let (hash, name) = base.split_at_checked(32)?;
    let name = name.strip_prefix('-')?;
    if hash.len() != 32
        || !hash.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        || name.is_empty()
    {
        return None;
    }
    Some((hash.to_string(), name.to_string()))
}

/// Package stem: name with trailing version-ish components dropped.
/// "python3.13-numpy-2.1.3" -> "python3.13-numpy"
fn stem(name: &str) -> String {
    let mut parts: Vec<&str> = name.split('-').collect();
    while parts.len() > 1
        && parts
            .last()
            .and_then(|p| p.chars().next())
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
    {
        parts.pop();
    }
    parts.join("-")
}

fn pair_key(base_hash: &str, target_hash: &str) -> String {
    format!("{base_hash}_{target_hash}")
}

/// Smallest window log (>= floor) whose window covers both NARs.
fn window_log_for(base: u64, target: u64) -> u32 {
    let need = base.max(target);
    let mut w = MIN_WINDOW_LOG;
    while w < MAX_WINDOW_LOG && (1u64 << w) < need {
        w += 1;
    }
    w
}

async fn run(cmd: &mut Command) -> Result<std::process::Output> {
    let rendered = format!("{:?}", cmd.as_std());
    let out = cmd.output().await.with_context(|| format!("spawn {rendered}"))?;
    if !out.status.success() {
        bail!(
            "{rendered} failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(out)
}

async fn free_bytes(dir: &Path) -> Result<u64> {
    let out = run(Command::new("df")
        .args(["--output=avail", "-B1"])
        .arg(dir)
        .stdin(Stdio::null()))
    .await?;
    let s = String::from_utf8_lossy(&out.stdout);
    s.lines()
        .nth(1)
        .and_then(|l| l.trim().parse::<u64>().ok())
        .ok_or_else(|| anyhow!("unparseable df output"))
}

async fn sha256_file(p: &Path) -> Result<String> {
    let mut f = tokio::fs::File::open(p).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

// ---------------------------------------------------------------- attic DB

/// Run `f` against the read-only attic DB connection, (re)opening on demand.
fn with_db<T>(app: &App, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T> {
    let mut guard = app.db.lock().unwrap();
    if guard.is_none() {
        let conn = Connection::open_with_flags(
            &app.attic_db,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("open {}", app.attic_db.display()))?;
        conn.busy_timeout(Duration::from_secs(5))?;
        *guard = Some(conn);
    }
    match f(guard.as_ref().unwrap()) {
        Ok(v) => Ok(v),
        Err(e) => {
            // Drop the connection on any error so a stale handle (e.g. after
            // an atticd migration) heals on the next call.
            *guard = None;
            Err(e.into())
        }
    }
}

/// Look up a store path hash in the allowed caches, first cache wins.
fn attic_object(app: &App, sph: &str) -> Result<Option<StoreObject>> {
    with_db(app, |conn| {
        let mut stmt = conn.prepare_cached(
            "SELECT o.store_path, o.\"references\", o.deriver, o.nar_id,
                    n.nar_size, c.name
             FROM object o
             JOIN nar n ON n.id = o.nar_id
             JOIN cache c ON c.id = o.cache_id
             WHERE o.store_path_hash = ?1 AND c.deleted_at IS NULL",
        )?;
        let rows: Vec<(String, String, Option<String>, i64, i64, String)> = stmt
            .query_map([sph], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    })
    .map(|rows| {
        let mut best: Option<(usize, StoreObject)> = None;
        for (store_path, refs_json, deriver, nar_id, nar_size, cache) in rows {
            let rank = app.caches.iter().position(|c| *c == cache);
            let Some(rank) = rank else { continue };
            if best.as_ref().map(|(r, _)| rank < *r).unwrap_or(true) {
                let references: Vec<String> =
                    serde_json::from_str(&refs_json).unwrap_or_default();
                best = Some((
                    rank,
                    StoreObject {
                        store_path,
                        references,
                        deriver,
                        nar_size: nar_size.max(0) as u64,
                        origin: Origin::Attic { cache, nar_id },
                    },
                ));
            }
        }
        best.map(|(_, obj)| obj)
    })
}

// ---------------------------------------------------------------- upstream

/// Parse an upstream .narinfo. None if it is not for `sph` or lacks a field
/// the differ needs.
fn parse_narinfo(text: &str, sph: &str) -> Option<StoreObject> {
    let field = |k: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(k).and_then(|v| v.strip_prefix(": ")))
            .map(|v| v.trim().to_string())
    };
    let store_path = field("StorePath")?;
    let (hash, _) = split_store_path(&store_path)?;
    if hash != sph {
        return None;
    }
    let url = field("URL")?;
    if url.is_empty() || url.contains("..") || url.starts_with('/') || url.contains("://") {
        return None;
    }
    let nar_size = field("NarSize")?.parse::<u64>().ok()?;
    let references = field("References")
        .map(|v| v.split_whitespace().map(String::from).collect())
        .unwrap_or_default();
    let deriver = field("Deriver").filter(|d| !d.is_empty() && d != "unknown-deriver");
    Some(StoreObject {
        store_path,
        references,
        deriver,
        nar_size,
        origin: Origin::Upstream {
            url,
            compression: field("Compression").unwrap_or_else(|| "none".into()),
        },
    })
}

/// Upstream objects for `hashes`, from memory, the narinfo files on disk,
/// or the upstream cache. A narinfo never changes for a store path hash, so
/// a found one is kept on disk for good. Hashes that are not upstream, or
/// that failed to fetch, are left out of the result.
async fn upstream_objects(app: &App, hashes: &[String]) -> Result<HashMap<String, StoreObject>> {
    let mut found: HashMap<String, StoreObject> = HashMap::new();
    if app.upstream_url.is_empty() {
        return Ok(found);
    }
    let mut todo: Vec<String> = Vec::new();
    {
        let mem = app.upstream_mem.lock().unwrap();
        let now = Instant::now();
        for h in hashes {
            match mem.get(h) {
                Some((_, Some(obj))) => {
                    found.insert(h.clone(), obj.clone());
                }
                Some((at, None)) if now.duration_since(*at) < UPSTREAM_NEGATIVE_TTL => {}
                _ => todo.push(h.clone()),
            }
        }
    }
    let mut fetch: Vec<String> = Vec::new();
    for h in todo {
        let file = app.upstream_dir.join(format!("{h}.narinfo"));
        match tokio::fs::read_to_string(&file).await {
            Ok(text) => match parse_narinfo(&text, &h) {
                Some(obj) => {
                    upstream_remember(app, &h, Some(obj.clone()));
                    found.insert(h, obj);
                }
                None => {
                    let _ = tokio::fs::remove_file(&file).await;
                    fetch.push(h);
                }
            },
            Err(_) => fetch.push(h),
        }
    }
    for batch in fetch.chunks(UPSTREAM_BATCH) {
        for (h, obj) in upstream_fetch(app, batch).await? {
            found.insert(h, obj);
        }
    }
    Ok(found)
}

fn upstream_remember(app: &App, sph: &str, obj: Option<StoreObject>) {
    let mut mem = app.upstream_mem.lock().unwrap();
    if mem.len() >= UPSTREAM_MEM_MAX && !mem.contains_key(sph) {
        mem.clear();
    }
    mem.insert(sph.to_string(), (Instant::now(), obj));
}

/// One curl call fetches the narinfos for `hashes` in parallel. A 404 is
/// remembered as "not upstream"; a network error is not remembered.
async fn upstream_fetch(app: &App, hashes: &[String]) -> Result<Vec<(String, StoreObject)>> {
    let seq = app.fetch_seq.fetch_add(1, Ordering::Relaxed);
    let dir = app.tmp_dir.join(format!("narinfo-{seq}"));
    tokio::fs::create_dir_all(&dir).await?;
    let mut cmd = Command::new("curl");
    cmd.args(["-sS", "--fail", "--parallel"])
        .arg(format!("--parallel-max={UPSTREAM_PARALLEL}"))
        .args(["--max-time", "30", "--write-out", "%{http_code} %{filename_effective}\n"]);
    for h in hashes {
        cmd.arg("-o").arg(dir.join(format!("{h}.narinfo")));
        cmd.arg(format!("{}/{h}.narinfo", app.upstream_url));
    }
    // --fail makes curl exit non-zero when any URL is a 404, so the exit
    // status says nothing. The per-transfer lines say which one.
    let out = cmd
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        .context("spawn curl for upstream narinfo")?;
    let mut got = Vec::new();
    let mut errors = 0usize;
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let Some((code, file)) = line.split_once(' ') else { continue };
        let Some(h) = Path::new(file)
            .file_stem()
            .and_then(|s| s.to_str())
            .filter(|h| hashes.iter().any(|x| x == h))
        else {
            continue;
        };
        match code {
            "200" => {
                let text = tokio::fs::read_to_string(file).await.unwrap_or_default();
                match parse_narinfo(&text, h) {
                    Some(obj) => {
                        let keep = app.upstream_dir.join(format!("{h}.narinfo"));
                        if tokio::fs::rename(file, &keep).await.is_err() {
                            let _ = tokio::fs::write(&keep, &text).await;
                        }
                        upstream_remember(app, h, Some(obj.clone()));
                        got.push((h.to_string(), obj));
                    }
                    None => errors += 1,
                }
            }
            "404" | "403" => upstream_remember(app, h, None),
            _ => errors += 1,
        }
    }
    let _ = tokio::fs::remove_dir_all(&dir).await;
    if errors > 0 {
        log(&format!("upstream narinfo: {errors} of {} lookups failed", hashes.len()));
    }
    Ok(got)
}

/// Objects for `hashes`: attic first (in the configured cache order), then
/// the upstream cache for the rest.
async fn find_objects(app: &App, hashes: &[String]) -> Result<HashMap<String, StoreObject>> {
    let mut found = HashMap::new();
    let mut rest = Vec::new();
    for h in hashes {
        match attic_object(app, h)? {
            Some(obj) => {
                found.insert(h.clone(), obj);
            }
            None => rest.push(h.clone()),
        }
    }
    if !rest.is_empty() {
        found.extend(upstream_objects(app, &rest).await?);
    }
    Ok(found)
}

async fn find_object(app: &App, sph: &str) -> Result<Option<StoreObject>> {
    Ok(find_objects(app, &[sph.to_string()]).await?.remove(sph))
}

fn chunk_set(app: &App, nar_id: i64) -> Result<HashSet<i64>> {
    with_db(app, |conn| {
        let mut stmt = conn
            .prepare_cached("SELECT chunk_id FROM chunkref WHERE nar_id = ?1 AND chunk_id IS NOT NULL")?;
        let ids: Vec<i64> = stmt
            .query_map([nar_id], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(ids.into_iter().collect())
    })
}

fn jaccard(a: &HashSet<i64>, b: &HashSet<i64>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 0.0;
    }
    let inter = a.intersection(b).count() as f64;
    let union = (a.len() + b.len()) as f64 - inter;
    if union == 0.0 { 0.0 } else { inter / union }
}

/// Full runtime closure of a toplevel, walked via `references`, one level
/// at a time so the upstream lookups of a level go out in one batch. The
/// toplevel itself must be in attic. Returns full store paths; paths that
/// neither attic nor upstream has (GC holes) are skipped.
async fn closure(app: &App, toplevel_sph: &str) -> Result<Vec<String>> {
    if attic_object(app, toplevel_sph)?.is_none() {
        return Ok(Vec::new());
    }
    let mut seen: HashSet<String> = HashSet::from([toplevel_sph.to_string()]);
    let mut order: Vec<String> = Vec::new();
    let mut frontier: Vec<String> = vec![toplevel_sph.to_string()];
    let (mut missing, mut upstream) = (0usize, 0usize);
    while !frontier.is_empty() {
        let objs = find_objects(app, &frontier).await?;
        let mut next = Vec::new();
        for h in &frontier {
            let Some(obj) = objs.get(h) else {
                missing += 1;
                continue;
            };
            if matches!(obj.origin, Origin::Upstream { .. }) {
                upstream += 1;
            }
            order.push(obj.store_path.clone());
            for r in &obj.references {
                if let Some((rh, _)) = split_store_path(&format!("/nix/store/{r}")) {
                    if seen.insert(rh.clone()) {
                        next.push(rh);
                    }
                }
            }
        }
        frontier = next;
    }
    log(&format!(
        "closure of {toplevel_sph}: {} paths, {upstream} from upstream, {missing} missing",
        order.len()
    ));
    Ok(order)
}

// ---------------------------------------------------------------- NAR fetch

/// LRU eviction: newest-mtime files survive, files under an hour old are
/// never evicted (they may be open in a running zstd/sha pass).
fn evict_nar_cache(app: &App) {
    let Ok(rd) = std::fs::read_dir(&app.nar_cache_dir) else { return };
    let mut files: Vec<(std::time::SystemTime, u64, PathBuf)> = rd
        .flatten()
        .filter_map(|e| {
            let md = e.metadata().ok()?;
            Some((md.modified().ok()?, md.len(), e.path()))
        })
        .collect();
    let mut total: u64 = files.iter().map(|(_, s, _)| s).sum();
    if total <= app.nar_cache_max {
        return;
    }
    files.sort_by_key(|(t, _, _)| *t); // oldest first
    let hour_ago = SystemTime::now() - Duration::from_secs(3600);
    for (mtime, size, path) in files {
        if total <= app.nar_cache_max || mtime > hour_ago {
            break;
        }
        if std::fs::remove_file(&path).is_ok() {
            total = total.saturating_sub(size);
            log(&format!("narcache evicted {}", path.display()));
        }
    }
}

fn nar_cache_stats(app: &App) -> (usize, u64) {
    let Ok(rd) = std::fs::read_dir(&app.nar_cache_dir) else { return (0, 0) };
    let mut n = 0usize;
    let mut bytes = 0u64;
    for e in rd.flatten() {
        if let Ok(md) = e.metadata() {
            n += 1;
            bytes += md.len();
        }
    }
    (n, bytes)
}

/// Get a decompressed NAR, from the local LRU cache, from atticd over
/// loopback, or from the upstream cache. Returns the cache path and the NAR
/// size.
async fn get_nar(app: &App, obj: &StoreObject) -> Result<(PathBuf, u64)> {
    let (sph, _) = split_store_path(&obj.store_path)
        .ok_or_else(|| anyhow!("bad store path {}", obj.store_path))?;
    let cached = app.nar_cache_dir.join(format!("{sph}.nar"));
    if let Ok(md) = tokio::fs::metadata(&cached).await {
        // Bump mtime so LRU keeps hot NARs.
        let _ = run(Command::new("touch").arg(&cached).stdin(Stdio::null())).await;
        return Ok((cached, md.len()));
    }
    let seq = app.fetch_seq.fetch_add(1, Ordering::Relaxed);
    let tmp = app.tmp_dir.join(format!("fetch-{seq}-{sph}.nar"));
    let fetched = match &obj.origin {
        Origin::Attic { cache, .. } => fetch_nar(app, cache, &sph, &tmp).await,
        Origin::Upstream { url, compression } => {
            fetch_upstream_nar(app, url, compression, &tmp).await
        }
    };
    let size = match fetched {
        Ok(size) => size,
        Err(e) => {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e);
        }
    };
    if size != obj.nar_size {
        let _ = tokio::fs::remove_file(&tmp).await;
        bail!("NAR of {} is {size} B, narinfo says {} B", obj.store_path, obj.nar_size);
    }
    tokio::fs::rename(&tmp, &cached).await?;
    evict_nar_cache(app);
    Ok((cached, size))
}

/// Fetch one NAR from the upstream cache into `dest`, decompressed.
async fn fetch_upstream_nar(app: &App, url: &str, compression: &str, dest: &Path) -> Result<u64> {
    let nar_url = format!("{}/{url}", app.upstream_url);
    let decompress = match compression {
        "none" => "cat",
        "xz" => "xz -dc",
        "zstd" => "zstd -dcq",
        "bzip2" => "bzip2 -dc",
        other => bail!("unsupported upstream NAR compression {other}"),
    };
    // The URL comes from a parsed narinfo (no quotes, no "..", no scheme);
    // pass it and the destination as arguments, not inside the script.
    run(Command::new("bash")
        .args([
            "-c",
            &format!("set -o pipefail; curl -fsSL --max-time 1800 \"$1\" | {decompress} > \"$2\""),
            "fetch",
            &nar_url,
        ])
        .arg(dest)
        .stdin(Stdio::null()))
    .await?;
    Ok(tokio::fs::metadata(dest).await?.len())
}

/// Fetch one NAR from atticd over loopback into `dest`, decompressed.
async fn fetch_nar(app: &App, cache: &str, sph: &str, dest: &Path) -> Result<u64> {
    let narinfo_url = format!("{}/{}/{}.narinfo", app.attic_url, cache, sph);
    let out = run(Command::new("curl")
        .args(["-fsS", "--max-time", "30", &narinfo_url])
        .stdin(Stdio::null()))
    .await?;
    let narinfo = String::from_utf8_lossy(&out.stdout).to_string();
    let field = |k: &str| {
        narinfo
            .lines()
            .find_map(|l| l.strip_prefix(k))
            .map(|v| v.trim().to_string())
    };
    let url = field("URL:").ok_or_else(|| anyhow!("narinfo for {sph} has no URL"))?;
    let compression = field("Compression:").unwrap_or_else(|| "none".into());
    let nar_url = format!("{}/{}/{}", app.attic_url, cache, url);

    // -L: atticd 307-redirects single-chunk NARs to presigned S3 URLs and
    // streams multi-chunk NARs inline; both must work.
    let pipeline = match compression.as_str() {
        "none" => format!("curl -fsSL '{nar_url}' -o '{}'", dest.display()),
        "zstd" => format!(
            "set -o pipefail; curl -fsSL '{nar_url}' | zstd -dq -o '{}'",
            dest.display()
        ),
        other => bail!("unsupported NAR compression {other} for {sph}"),
    };
    run(Command::new("bash").args(["-c", &pipeline]).stdin(Stdio::null())).await?;
    Ok(tokio::fs::metadata(dest).await?.len())
}

async fn zstd_patch(level: u32, wlog: u32, base: &Path, target: &Path, out: &Path) -> Result<u64> {
    run(Command::new("zstd")
        .arg(format!("-{level}"))
        .arg(format!("--long={wlog}"))
        .arg("--single-thread")
        .arg("--force")
        .arg("--quiet")
        .arg(format!("--patch-from={}", base.display()))
        .arg(target)
        .arg("-o")
        .arg(out)
        .stdin(Stdio::null()))
    .await?;
    Ok(tokio::fs::metadata(out).await?.len())
}

// ---------------------------------------------------------------- compute

fn meta_path(app: &App, key: &str) -> PathBuf {
    app.meta_dir.join(format!("{key}.json"))
}

fn blob_path(app: &App, key: &str) -> PathBuf {
    app.blob_dir.join(format!("{key}.zst"))
}

fn load_meta(app: &App, key: &str) -> Option<PairMeta> {
    let raw = std::fs::read(meta_path(app, key)).ok()?;
    serde_json::from_slice(&raw).ok()
}

fn final_level(target_size: u64) -> u32 {
    if target_size < LARGE_TARGET_BYTES { FINAL_LEVEL_SMALL } else { FINAL_LEVEL_LARGE }
}

/// Write `meta` for `key` atomically, through `work`.
async fn store_meta(app: &App, work: &Path, key: &str, meta: &PairMeta) -> Result<()> {
    let tmp_meta = work.join(format!("meta-{key}.json"));
    tokio::fs::write(&tmp_meta, serde_json::to_vec_pretty(meta)?).await?;
    tokio::fs::rename(&tmp_meta, meta_path(app, key)).await?;
    Ok(())
}

async fn compute(app: &App, job: &Job) -> Result<PairMeta> {
    let (t_hash, t_name) = split_store_path(&job.target)
        .ok_or_else(|| anyhow!("bad target {}", job.target))?;

    if free_bytes(&app.tmp_dir).await? < MIN_FREE_BYTES {
        bail!("low disk, skipping {t_name}");
    }

    // A refine job and a new pair for the same target can run at once.
    let seq = app.fetch_seq.fetch_add(1, Ordering::Relaxed);
    let work = app.tmp_dir.join(format!("{t_hash}-{seq}"));
    tokio::fs::create_dir_all(&work).await?;
    let result = if job.source == "refine" {
        refine_inner(app, job, &t_hash, &work).await
    } else {
        compute_inner(app, job, &t_hash, &work).await
    };
    let _ = tokio::fs::remove_dir_all(&work).await;
    result
}

async fn compute_inner(app: &App, job: &Job, t_hash: &str, work: &Path) -> Result<PairMeta> {
    let started = Instant::now();

    let target_obj = find_object(app, t_hash)
        .await?
        .ok_or_else(|| anyhow!("target {} not in attic or upstream", job.target))?;

    // Rank candidates by chunk overlap — DB only, no bytes fetched. An
    // upstream path has no chunk list, so any upstream side ranks by size.
    let rank_started = Instant::now();
    let target_chunks = match &target_obj.origin {
        Origin::Attic { nar_id, .. } => Some(chunk_set(app, *nar_id)?),
        Origin::Upstream { .. } => None,
    };
    let mut ranked: Vec<(Option<f64>, String, StoreObject)> = Vec::new();
    for base in &job.bases {
        let Some((b_hash, _)) = split_store_path(base) else { continue };
        let Some(obj) = find_object(app, &b_hash).await? else { continue };
        let overlap = match (&target_chunks, &obj.origin) {
            (Some(tc), Origin::Attic { nar_id, .. }) => Some(jaccard(tc, &chunk_set(app, *nar_id)?)),
            _ => None,
        };
        ranked.push((overlap, b_hash, obj));
    }
    let rank_ms = rank_started.elapsed().as_millis() as u64;
    let candidates_ranked = ranked.len();
    let overlap_meaningful = target_chunks
        .as_ref()
        .map(|c| c.len() >= MIN_CHUNKS_FOR_OVERLAP)
        .unwrap_or(false)
        && ranked.iter().all(|(o, _, _)| o.is_some());
    if overlap_meaningful {
        ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    } else {
        // Chunking can't see sub-chunk similarity — prefer the base whose
        // NAR size is closest to the target's.
        let t_size = target_obj.nar_size as i64;
        ranked.sort_by_key(|(_, _, obj)| (obj.nar_size as i64 - t_size).abs());
    }
    // Overlap floor: a hopeless pair is decided here, from the DB alone.
    // Mark every requested pair rejected so /delta answers 204 (full
    // download) instead of looping 202, and fetch nothing.
    let best_overlap = ranked.first().and_then(|(o, _, _)| *o).unwrap_or(0.0);
    if overlap_meaningful && best_overlap < MIN_CHUNK_OVERLAP {
        for (overlap, b_hash, base_obj) in &ranked {
            let key = pair_key(b_hash, t_hash);
            let meta = PairMeta {
                base: base_obj.store_path.clone(),
                target: job.target.clone(),
                algo: ALGO.into(),
                window_log: 0,
                level: 0,
                patch_size: 0,
                nar_size: target_obj.nar_size,
                nar_sha256: String::new(),
                references: Vec::new(),
                deriver: None,
                chunk_overlap: overlap.unwrap_or(-1.0),
                refined: false,
                compute_ms: started.elapsed().as_millis() as u64,
                rank_ms,
                candidates_ranked,
                source: job.source.into(),
                created_unix: now_unix(),
                rejected: true,
            };
            store_meta(app, work, &key, &meta).await?;
        }
        bail!("best chunk overlap {best_overlap:.3} below floor {MIN_CHUNK_OVERLAP} — full download, nothing fetched");
    }

    let (overlap, b_hash, base_obj) = ranked
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no candidate base present in attic or upstream"))?;

    let wlog = window_log_for(base_obj.nar_size, target_obj.nar_size);
    if (1u64 << wlog) < base_obj.nar_size.max(target_obj.nar_size) {
        bail!(
            "NAR too large for max window ({} B > 2^{MAX_WINDOW_LOG})",
            base_obj.nar_size.max(target_obj.nar_size)
        );
    }

    // NARs come from the local LRU cache, falling back to atticd/S3 or the
    // upstream cache.
    let (base_nar, _) = get_nar(app, &base_obj).await?;
    let (target_nar, target_size) = get_nar(app, &target_obj).await?;

    let patch_tmp = work.join("patch.zst");
    let patch_size = zstd_patch(FAST_LEVEL, wlog, &base_nar, &target_nar, &patch_tmp).await?;
    let nar_sha256 = sha256_file(&target_nar).await?;

    let key = pair_key(&b_hash, t_hash);
    let rejected = (patch_size as f64) > (target_size as f64) * KEEP_RATIO;
    if rejected {
        let _ = tokio::fs::remove_file(&patch_tmp).await;
    } else {
        tokio::fs::rename(&patch_tmp, blob_path(app, &key)).await?;
    }

    let meta = PairMeta {
        base: base_obj.store_path.clone(),
        target: job.target.clone(),
        algo: ALGO.into(),
        window_log: wlog,
        level: FAST_LEVEL,
        patch_size,
        nar_size: target_size,
        nar_sha256,
        references: target_obj
            .references
            .iter()
            .map(|r| format!("/nix/store/{r}"))
            .collect(),
        deriver: target_obj.deriver.clone(),
        chunk_overlap: overlap.unwrap_or(-1.0),
        refined: false,
        compute_ms: started.elapsed().as_millis() as u64,
        rank_ms,
        candidates_ranked,
        source: job.source.into(),
        created_unix: now_unix(),
        rejected,
    };
    store_meta(app, work, &key, &meta).await?;
    if !rejected && final_level(target_size) > FAST_LEVEL {
        enqueue_refine(app, &job.target, &base_obj.store_path);
    }
    Ok(meta)
}

/// Patch a kept pair again at the final level. The smaller blob wins; the
/// rename is atomic, so a device that downloads the blob meanwhile gets one
/// whole patch or the other, and both rebuild the same NAR.
async fn refine_inner(app: &App, job: &Job, t_hash: &str, work: &Path) -> Result<PairMeta> {
    let started = Instant::now();
    let base = job.bases.first().ok_or_else(|| anyhow!("refine job without a base"))?;
    let (b_hash, _) = split_store_path(base).ok_or_else(|| anyhow!("bad base {base}"))?;
    let key = pair_key(&b_hash, t_hash);
    let mut meta = load_meta(app, &key).ok_or_else(|| anyhow!("no pair {key} to refine"))?;
    let level = final_level(meta.nar_size);
    if meta.rejected || meta.refined || meta.level >= level {
        return Ok(meta);
    }
    let base_obj = find_object(app, &b_hash)
        .await?
        .ok_or_else(|| anyhow!("base {base} not in attic or upstream"))?;
    let target_obj = find_object(app, t_hash)
        .await?
        .ok_or_else(|| anyhow!("target {} not in attic or upstream", job.target))?;
    let (base_nar, _) = get_nar(app, &base_obj).await?;
    let (target_nar, _) = get_nar(app, &target_obj).await?;

    let patch_tmp = work.join("patch.zst");
    let patch_size = zstd_patch(level, meta.window_log, &base_nar, &target_nar, &patch_tmp).await?;
    if patch_size < meta.patch_size {
        tokio::fs::rename(&patch_tmp, blob_path(app, &key)).await?;
        meta.patch_size = patch_size;
        meta.level = level;
    }
    meta.refined = true;
    meta.compute_ms += started.elapsed().as_millis() as u64;
    store_meta(app, work, &key, &meta).await?;
    Ok(meta)
}

// ---------------------------------------------------------------- workers

fn pop_job(app: &App) -> Option<Job> {
    if let Some(j) = app.demand.lock().unwrap().pop_front() {
        return Some(j);
    }
    if let Some(j) = app.warm.lock().unwrap().pop_front() {
        return Some(j);
    }
    app.refine.lock().unwrap().pop_front()
}

async fn worker_loop(app: Arc<App>) {
    loop {
        let Some(job) = pop_job(&app) else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        let t = job.target.clone();
        let started = Instant::now();
        let result = compute(&app, &job).await;
        let lane = job.source;
        count(
            &app,
            &format!("pifinder_differ_job_seconds_total{{lane=\"{lane}\"}}"),
            started.elapsed().as_secs_f64(),
        );
        match &result {
            Ok(m) if m.rejected => {
                count(&app, &format!("pifinder_differ_jobs_total{{lane=\"{lane}\",result=\"rejected\"}}"), 1.0);
            }
            Ok(m) => {
                count(&app, &format!("pifinder_differ_jobs_total{{lane=\"{lane}\",result=\"ok\"}}"), 1.0);
                if lane != "refine" {
                    count(&app, "pifinder_differ_patch_bytes_total", m.patch_size as f64);
                    count(&app, "pifinder_differ_nar_bytes_total", m.nar_size as f64);
                }
            }
            Err(_) => {
                count(&app, &format!("pifinder_differ_jobs_total{{lane=\"{lane}\",result=\"failed\"}}"), 1.0);
            }
        }
        match result {
            Ok(m) => {
                app.jobs_done.fetch_add(1, Ordering::Relaxed);
                log(&format!(
                    "{} {} -> patch {} B / nar {} B (overlap {:.2}, level {}, w{}, {} ms, rank {} ms over {} bases){}",
                    job.source,
                    t,
                    m.patch_size,
                    m.nar_size,
                    m.chunk_overlap,
                    m.level,
                    m.window_log,
                    m.compute_ms,
                    m.rank_ms,
                    m.candidates_ranked,
                    if m.rejected { " REJECTED" } else { "" },
                ));
            }
            Err(e) => {
                app.jobs_failed.fetch_add(1, Ordering::Relaxed);
                log(&format!("{} {t} FAILED: {e:#}", job.source));
            }
        }
        // A refine job does not hold the inflight mark; a new pair for the
        // same target may hold it.
        if job.source != "refine" {
            if let Some((h, _)) = split_store_path(&t) {
                app.inflight.lock().unwrap().remove(&h);
            }
        }
        app.job_done.notify_waiters();
    }
}

/// Queue a job unless the target is already computed against one of the given
/// bases, already inflight, or the queue is full. Returns what happened.
/// A demand for a target that waits in the warm queue moves that job to the
/// demand queue.
fn enqueue(app: &App, job: Job, demand: bool) -> &'static str {
    let Some((t_hash, _)) = split_store_path(&job.target) else {
        return "invalid";
    };
    for base in &job.bases {
        if let Some((b_hash, _)) = split_store_path(base) {
            if load_meta(app, &pair_key(&b_hash, &t_hash)).is_some() {
                return "exists";
            }
        }
    }
    if !app.inflight.lock().unwrap().insert(t_hash.clone()) {
        if demand {
            promote(app, &t_hash);
        }
        return "inflight";
    }
    let (q, cap) = if demand {
        (&app.demand, DEMAND_QUEUE_CAP)
    } else {
        (&app.warm, WARM_QUEUE_CAP)
    };
    let mut q = q.lock().unwrap();
    if q.len() >= cap {
        app.inflight.lock().unwrap().remove(&t_hash);
        return "full";
    }
    q.push_back(job);
    "queued"
}

/// Move the warm job for `t_hash`, if one waits, to the demand queue.
fn promote(app: &App, t_hash: &str) {
    let job = {
        let mut warm = app.warm.lock().unwrap();
        let pos = warm.iter().position(|j| {
            split_store_path(&j.target).map(|(h, _)| h == t_hash).unwrap_or(false)
        });
        match pos {
            Some(pos) => warm.remove(pos),
            None => None,
        }
    };
    let Some(job) = job else { return };
    let mut demand = app.demand.lock().unwrap();
    if demand.len() < DEMAND_QUEUE_CAP {
        demand.push_back(job);
    } else {
        drop(demand);
        app.warm.lock().unwrap().push_front(job);
    }
}

fn enqueue_refine(app: &App, target: &str, base: &str) {
    let mut q = app.refine.lock().unwrap();
    if q.len() >= REFINE_QUEUE_CAP
        || q.iter().any(|j| j.target == target && j.bases.first().map(|b| b.as_str()) == Some(base))
    {
        return;
    }
    q.push_back(Job { target: target.to_string(), bases: vec![base.to_string()], source: "refine" });
}

/// After a restart: queue a refine job for every kept pair that has only
/// the fast patch.
fn requeue_refines(app: &App) -> usize {
    let Ok(rd) = std::fs::read_dir(&app.meta_dir) else { return 0 };
    let mut n = 0;
    for entry in rd.flatten() {
        let Ok(raw) = std::fs::read(entry.path()) else { continue };
        let Ok(m) = serde_json::from_slice::<PairMeta>(&raw) else { continue };
        if !m.rejected && !m.refined && m.level < final_level(m.nar_size) {
            enqueue_refine(app, &m.target, &m.base);
            n += 1;
        }
    }
    n
}

// ---------------------------------------------------------------- rate limit

/// Take `cost` tokens from the per-IP bucket; false = over the limit.
fn ip_allow(app: &App, ip: &str, cost: f64) -> bool {
    let mut map = app.rate.lock().unwrap();
    // Unbounded growth guard: an address-rotating attacker resets everyone's
    // bucket rather than growing the map without limit.
    if map.len() >= RL_MAX_TRACKED_IPS && !map.contains_key(ip) {
        map.clear();
    }
    let now = Instant::now();
    let bucket = map.entry(ip.to_string()).or_insert(TokenBucket {
        tokens: RL_IP_BURST,
        last: now,
    });
    let elapsed = now.duration_since(bucket.last).as_secs_f64();
    bucket.tokens = (bucket.tokens + elapsed * RL_IP_REFILL_PER_SEC).min(RL_IP_BURST);
    bucket.last = now;
    if bucket.tokens < cost {
        return false;
    }
    bucket.tokens -= cost;
    true
}

/// Client IP: first X-Forwarded-For entry, set by Caddy (trustworthy: the
/// listener is loopback-only). Absent header = direct loopback = ops traffic.
fn client_ip(req: &axum::http::Request<axum::body::Body>) -> Option<String> {
    req.headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|v| v.trim().to_string())
}

/// Spend one unit of an update session's budget; false = unknown, expired,
/// or exhausted.
fn session_spend(app: &App, token: &str) -> bool {
    let mut sessions = app.sessions.lock().unwrap();
    let now = Instant::now();
    match sessions.get_mut(token) {
        Some(s) if s.expires > now && s.budget > 0 => {
            s.budget -= 1;
            true
        }
        _ => false,
    }
}

fn new_session(app: &App, budget: i64, closure: Arc<HashSet<String>>) -> String {
    let mut raw = [0u8; 16];
    // /dev/urandom: no rng dependency, and this is an ephemeral token.
    use std::io::Read;
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut raw))
        .expect("urandom");
    let token = hex::encode(raw);
    let mut sessions = app.sessions.lock().unwrap();
    let now = Instant::now();
    if sessions.len() >= MAX_SESSIONS {
        sessions.retain(|_, s| s.expires > now && s.budget > 0);
        if sessions.len() >= MAX_SESSIONS {
            sessions.clear(); // rotating attacker: reset rather than grow
        }
    }
    sessions.insert(token.clone(), Session { budget, expires: now + SESSION_TTL, closure });
    token
}

async fn rate_limit_mw(
    State(app): State<Arc<App>>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    // No X-Forwarded-For = direct loopback (ops): never limited.
    let Some(ip) = client_ip(&req) else {
        return next.run(req).await;
    };
    let path = req.uri().path();
    let allowed = match path {
        "/health" => ip_allow(&app, &ip, 1.0),
        "/update-start" => ip_allow(&app, &ip, RL_MINT_COST),
        // Everything else public is budgeted per update session.
        _ => req
            .headers()
            .get("x-update-session")
            .and_then(|v| v.to_str().ok())
            .map(|t| session_spend(&app, t))
            .unwrap_or(false),
    };
    if !allowed {
        app.rate_limited.fetch_add(1, Ordering::Relaxed);
        count(&app, "pifinder_differ_rate_limited_total", 1.0);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "60")],
            "missing/exhausted update session — POST /update-start",
        )
            .into_response();
    }
    next.run(req).await
}

#[derive(Deserialize)]
struct UpdateStartReq {
    target_toplevel: String,
    // The toplevel the device runs now. Optional; it starts a warm run for
    // exactly this step before the device asks for single paths.
    #[serde(default)]
    base_toplevel: Option<String>,
}

/// Open an update session. The budget is derived from the target closure's
/// real size in the attic DB, so it covers exactly one honest upgrade
/// (a /delta POST and a blob GET per changed path, with slack for retries)
/// and cannot be inflated: unknown toplevels are refused.
async fn post_update_start(
    State(app): State<Arc<App>>,
    Json(req): Json<UpdateStartReq>,
) -> Response {
    let Some((sph, _)) = split_store_path(&req.target_toplevel) else {
        return (StatusCode::BAD_REQUEST, "not a store path").into_response();
    };
    // A toplevel's closure never changes, so it is walked once.
    let cached = app.closures.lock().unwrap().get(&sph).cloned();
    let hashes = match cached {
        Some(h) => h,
        None => {
            let closure = match closure(&app, &sph).await {
                Ok(c) if !c.is_empty() => c,
                Ok(_) => return (StatusCode::NOT_FOUND, "toplevel not in cache").into_response(),
                Err(e) => {
                    log(&format!("update-start failed: {e:#}"));
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };
            let hashes: Arc<HashSet<String>> = Arc::new(
                closure.iter().filter_map(|p| split_store_path(p)).map(|(h, _)| h).collect(),
            );
            let mut closures = app.closures.lock().unwrap();
            if closures.len() >= MAX_CLOSURES {
                closures.clear();
            }
            closures.insert(sph.clone(), hashes.clone());
            hashes
        }
    };
    let budget = 2 * hashes.len() as i64 + SESSION_SLACK;
    if let Some(base) = req.base_toplevel.as_deref() {
        start_priority_warm(&app, base, &req.target_toplevel);
    }
    count(&app, "pifinder_differ_sessions_total", 1.0);
    let token = new_session(&app, budget, hashes);
    Json(serde_json::json!({
        "session": token,
        "budget": budget,
        "expires_in": SESSION_TTL.as_secs(),
    }))
    .into_response()
}

// ---------------------------------------------------------------- HTTP

#[derive(Deserialize, Clone)]
struct DeltaReq {
    target: String,
    bases: Vec<String>,
}

#[derive(Serialize)]
struct DeltaHit {
    algo: String,
    basis: Vec<String>,
    window_log: u32,
    url: String,
    size: u64,
    nar_size: u64,
    nar_sha256: String,
    references: Vec<String>,
    deriver: Option<String>,
}

enum Resolved {
    Hit(DeltaHit),
    // Every requested pair is decided and rejected: download in full.
    NoPatch,
    // At least one pair is not computed yet.
    Pending,
}

/// The answer for a target and the bases the device holds, from the pair
/// metadata alone.
fn resolve(app: &App, t_hash: &str, bases: &[String]) -> Resolved {
    let mut all_rejected = true;
    for base in bases {
        let Some((b_hash, _)) = split_store_path(base) else { continue };
        let key = pair_key(&b_hash, t_hash);
        if let Some(meta) = load_meta(app, &key) {
            if meta.rejected {
                continue;
            }
            return Resolved::Hit(DeltaHit {
                algo: meta.algo,
                basis: vec![meta.base],
                window_log: meta.window_log,
                url: format!("/blobs/{key}.zst"),
                size: meta.patch_size,
                nar_size: meta.nar_size,
                nar_sha256: meta.nar_sha256,
                references: meta.references,
                deriver: meta.deriver,
            });
        }
        all_rejected = false;
    }
    if all_rejected { Resolved::NoPatch } else { Resolved::Pending }
}

async fn post_delta(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(req): Json<DeltaReq>,
) -> Response {
    let Some((t_hash, _)) = split_store_path(&req.target) else {
        return (StatusCode::BAD_REQUEST, "target is not a store path").into_response();
    };
    // Public requests carry a session (rate_limit_mw refuses them otherwise);
    // a request without one is direct loopback ops traffic.
    if let Some(token) = headers.get("x-update-session").and_then(|v| v.to_str().ok()) {
        let in_closure = app
            .sessions
            .lock()
            .unwrap()
            .get(token)
            .map(|s| s.closure.contains(&t_hash))
            .unwrap_or(false);
        if !in_closure {
            return (StatusCode::FORBIDDEN, "target not in this session's closure")
                .into_response();
        }
    }
    let bases: Vec<String> = req
        .bases
        .iter()
        .filter(|b| split_store_path(b).is_some())
        .take(MAX_BASES)
        .cloned()
        .collect();
    if bases.is_empty() {
        return (StatusCode::BAD_REQUEST, "no valid bases").into_response();
    }

    match resolve(&app, &t_hash, &bases) {
        Resolved::Hit(hit) => {
            count(&app, "pifinder_differ_answers_total{endpoint=\"delta\",answer=\"hit\"}", 1.0);
            return Json(hit).into_response();
        }
        Resolved::NoPatch => {
            count(&app, "pifinder_differ_answers_total{endpoint=\"delta\",answer=\"none\"}", 1.0);
            return StatusCode::NO_CONTENT.into_response();
        }
        Resolved::Pending => {}
    }

    count(&app, "pifinder_differ_answers_total{endpoint=\"delta\",answer=\"wait\"}", 1.0);
    match enqueue(&app, Job { target: req.target, bases, source: "demand" }, true) {
        "full" => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        // queued | inflight | exists-under-other-base: tell the device to retry
        _ => (StatusCode::ACCEPTED, [("retry-after", "15")], "computing").into_response(),
    }
}

#[derive(Deserialize)]
struct DeltasReq {
    targets: Vec<DeltaReq>,
}

/// POST /deltas: the /delta answer for many targets in one request. The
/// response is a stream of JSON lines (application/x-ndjson):
///   {"target", "state": "hit", "delta": {...as /delta 200...}}
///   {"target", "state": "none"}   no patch: download in full
///   {"target", "state": "wait"}   still computing when the stream ended
///   {"state": "heartbeat", "pending": n}   every STREAM_HEARTBEAT
///   {"state": "end"}              last line
/// A line for a target comes as soon as its patch is decided, so the device
/// applies the first patches while the server computes the rest. A stream
/// that is cut before "end" leaves the device to ask again for the targets it
/// has no line for.
async fn post_deltas(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(req): Json<DeltasReq>,
) -> Response {
    if req.targets.is_empty() || req.targets.len() > MAX_STREAM_TARGETS {
        return (StatusCode::BAD_REQUEST, "1 to 4000 targets").into_response();
    }
    // Public requests carry a session (rate_limit_mw refuses them otherwise);
    // a request without one is direct loopback ops traffic.
    let closure = match headers.get("x-update-session").and_then(|v| v.to_str().ok()) {
        Some(token) => match app.sessions.lock().unwrap().get(token) {
            Some(s) => Some(s.closure.clone()),
            None => return (StatusCode::FORBIDDEN, "unknown session").into_response(),
        },
        None => None,
    };
    let (writer, reader) = tokio::io::duplex(64 * 1024);
    tokio::spawn(stream_deltas(app, closure, req.targets, writer));
    (
        [("content-type", "application/x-ndjson"), ("cache-control", "no-cache")],
        axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(reader)),
    )
        .into_response()
}

async fn emit(app: &App, w: &mut tokio::io::DuplexStream, line: serde_json::Value) -> bool {
    use tokio::io::AsyncWriteExt;
    if line.get("target").is_some() {
        if let Some(state) = line.get("state").and_then(|v| v.as_str()) {
            count(app, &format!("pifinder_differ_answers_total{{endpoint=\"deltas\",answer=\"{state}\"}}"), 1.0);
        }
    }
    let mut buf = line.to_string();
    buf.push('\n');
    w.write_all(buf.as_bytes()).await.is_ok()
}

async fn stream_deltas(
    app: Arc<App>,
    closure: Option<Arc<HashSet<String>>>,
    targets: Vec<DeltaReq>,
    mut w: tokio::io::DuplexStream,
) {
    use serde_json::json;
    let started = Instant::now();
    // (target, target hash, bases) still without an answer.
    let mut pending: Vec<(String, String, Vec<String>)> = Vec::new();
    for req in targets {
        let Some((t_hash, _)) = split_store_path(&req.target) else { continue };
        let allowed = closure.as_ref().map(|c| c.contains(&t_hash)).unwrap_or(true);
        let bases: Vec<String> = req
            .bases
            .iter()
            .filter(|b| split_store_path(b).is_some())
            .take(MAX_BASES)
            .cloned()
            .collect();
        if !allowed || bases.is_empty() {
            if !emit(&app, &mut w, json!({"target": req.target, "state": "none"})).await {
                return;
            }
            continue;
        }
        let line = match resolve(&app, &t_hash, &bases) {
            Resolved::Hit(hit) => Some(json!({"target": req.target, "state": "hit", "delta": hit})),
            Resolved::NoPatch => Some(json!({"target": req.target, "state": "none"})),
            Resolved::Pending => {
                let job = Job { target: req.target.clone(), bases: bases.clone(), source: "demand" };
                if enqueue(&app, job, true) == "full" {
                    Some(json!({"target": req.target, "state": "none"}))
                } else {
                    pending.push((req.target, t_hash, bases));
                    None
                }
            }
        };
        if let Some(line) = line {
            if !emit(&app, &mut w, line).await {
                return;
            }
        }
    }

    let mut last_beat = Instant::now();
    while !pending.is_empty() && started.elapsed() < STREAM_MAX {
        // Register for the wake-up before the check, so that a job that ends
        // between the check and the wait is not missed.
        let notified = app.job_done.notified();
        let mut still = Vec::new();
        for (target, t_hash, bases) in pending.drain(..) {
            let line = match resolve(&app, &t_hash, &bases) {
                Resolved::Hit(hit) => Some(json!({"target": target, "state": "hit", "delta": hit})),
                Resolved::NoPatch => Some(json!({"target": target, "state": "none"})),
                // Not computed and not in the queue any more: the job failed
                // without a result. The device downloads it in full.
                Resolved::Pending if !app.inflight.lock().unwrap().contains(&t_hash) => {
                    Some(json!({"target": target, "state": "none"}))
                }
                Resolved::Pending => None,
            };
            match line {
                Some(line) => {
                    if !emit(&app, &mut w, line).await {
                        return;
                    }
                }
                None => still.push((target, t_hash, bases)),
            }
        }
        pending = still;
        if pending.is_empty() {
            break;
        }
        if last_beat.elapsed() >= STREAM_HEARTBEAT {
            last_beat = Instant::now();
            if !emit(&app, &mut w, json!({"state": "heartbeat", "pending": pending.len()})).await {
                return;
            }
        }
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
    for (target, _, _) in pending {
        if !emit(&app, &mut w, json!({"target": target, "state": "wait"})).await {
            return;
        }
    }
    let _ = emit(&app, &mut w, json!({"state": "end"})).await;
}

#[derive(Deserialize)]
struct WarmReq {
    base_toplevel: String,
    target_toplevel: String,
}

async fn post_warm(State(app): State<Arc<App>>, Json(req): Json<WarmReq>) -> Response {
    if split_store_path(&req.base_toplevel).is_none()
        || split_store_path(&req.target_toplevel).is_none()
    {
        return (StatusCode::BAD_REQUEST, "toplevels must be store paths").into_response();
    }
    let id = spawn_warm(&app, req.base_toplevel, req.target_toplevel, false);
    (StatusCode::ACCEPTED, Json(serde_json::json!({ "warm_id": id }))).into_response()
}

/// A device named the toplevel it runs. Warm that step in the demand lane,
/// once per PRIORITY_WARM_TTL.
fn start_priority_warm(app: &Arc<App>, base_top: &str, target_top: &str) {
    if split_store_path(base_top).is_none() || base_top == target_top {
        return;
    }
    let key = (base_top.to_string(), target_top.to_string());
    {
        let mut seen = app.priority_warms.lock().unwrap();
        let now = Instant::now();
        seen.retain(|_, at| now.duration_since(*at) < PRIORITY_WARM_TTL);
        if seen.contains_key(&key) || seen.len() >= MAX_CLOSURES {
            return;
        }
        seen.insert(key.clone(), now);
    }
    spawn_warm(app, key.0, key.1, true);
}

fn spawn_warm(app: &Arc<App>, base_top: String, target_top: String, priority: bool) -> u64 {
    let id = app.warm_seq.fetch_add(1, Ordering::Relaxed) + 1;
    {
        let mut runs = app.warm_runs.lock().unwrap();
        // Keep /status small: device-started runs make this grow.
        if runs.len() >= 100 {
            runs.remove(0);
        }
        runs.push(WarmRun {
            id,
            base_toplevel: base_top.clone(),
            target_toplevel: target_top.clone(),
            state: "pairing".into(),
            priority,
            paired: 0,
            skipped_existing: 0,
            unpaired: 0,
            error: None,
            started_unix: now_unix(),
        });
    }
    let app2 = app.clone();
    tokio::spawn(async move {
        let res = warm_run(&app2, id, &base_top, &target_top, priority).await;
        let mut runs = app2.warm_runs.lock().unwrap();
        if let Some(r) = runs.iter_mut().find(|r| r.id == id) {
            if let Err(e) = res {
                r.state = "failed".into();
                r.error = Some(format!("{e:#}"));
            }
        }
    });
    id
}

async fn warm_run(
    app: &Arc<App>,
    id: u64,
    base_top: &str,
    target_top: &str,
    priority: bool,
) -> Result<()> {
    let (base_sph, _) = split_store_path(base_top).unwrap();
    let (target_sph, _) = split_store_path(target_top).unwrap();

    let base_closure = closure(app, &base_sph).await?;
    let target_closure = closure(app, &target_sph).await?;
    if target_closure.is_empty() {
        bail!("target toplevel not in attic");
    }
    let base_set: HashSet<&String> = base_closure.iter().collect();

    let mut by_stem: HashMap<String, Vec<String>> = HashMap::new();
    for p in &base_closure {
        if let Some((_, name)) = split_store_path(p) {
            by_stem.entry(stem(&name)).or_default().push(p.clone());
        }
    }

    let (mut paired, mut skipped, mut unpaired) = (0usize, 0usize, 0usize);
    for target in &target_closure {
        if base_set.contains(target) {
            continue; // device already holds it — nix downloads nothing
        }
        let Some((_, name)) = split_store_path(target) else { continue };
        let Some(cands) = by_stem.get(&stem(&name)) else {
            unpaired += 1;
            continue;
        };
        let job = Job { target: target.clone(), bases: cands.clone(), source: "warm" };
        // A device-started run uses the demand lane up to half its cap, so
        // the device's own /delta calls still find room.
        let demand_lane =
            priority && app.demand.lock().unwrap().len() < DEMAND_QUEUE_CAP / 2;
        match enqueue(app, job, demand_lane) {
            "queued" => paired += 1,
            "exists" | "inflight" => skipped += 1,
            _ => unpaired += 1,
        }
    }
    {
        let mut runs = app.warm_runs.lock().unwrap();
        if let Some(r) = runs.iter_mut().find(|r| r.id == id) {
            r.state = "queued".into();
            r.paired = paired;
            r.skipped_existing = skipped;
            r.unpaired = unpaired;
        }
    }
    log(&format!(
        "warm {id}: {paired} queued, {skipped} already known, {unpaired} unpaired"
    ));
    Ok(())
}

async fn get_pairs(State(app): State<Arc<App>>) -> Response {
    let mut pairs: Vec<PairMeta> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&app.meta_dir) {
        for entry in rd.flatten() {
            if let Ok(raw) = std::fs::read(entry.path()) {
                if let Ok(m) = serde_json::from_slice::<PairMeta>(&raw) {
                    pairs.push(m);
                }
            }
        }
    }
    pairs.sort_by(|a, b| b.created_unix.cmp(&a.created_unix));
    let kept: Vec<&PairMeta> = pairs.iter().filter(|p| !p.rejected).collect();
    let total_patch: u64 = kept.iter().map(|p| p.patch_size).sum();
    let total_nar: u64 = kept.iter().map(|p| p.nar_size).sum();
    Json(serde_json::json!({
        "pairs": pairs,
        "summary": {
            "kept": kept.len(),
            "rejected": pairs.len() - kept.len(),
            "total_patch_bytes": total_patch,
            "total_target_nar_bytes": total_nar,
            "overall_ratio": if total_nar > 0 {
                total_patch as f64 / total_nar as f64
            } else { 0.0 },
        }
    }))
    .into_response()
}

fn dir_bytes(dir: &Path) -> (usize, u64) {
    let Ok(rd) = std::fs::read_dir(dir) else { return (0, 0) };
    rd.flatten()
        .filter_map(|e| e.metadata().ok())
        .fold((0, 0), |(n, b), md| (n + 1, b + md.len()))
}

/// Prometheus text format. Counters from `metrics`, gauges read now.
async fn get_metrics(State(app): State<Arc<App>>) -> Response {
    let (nc_files, nc_bytes) = nar_cache_stats(&app);
    let (blob_files, blob_bytes) = dir_bytes(&app.blob_dir);
    let mut out = String::new();
    let mut series: Vec<(String, f64)> =
        app.metrics.lock().unwrap().iter().map(|(k, v)| (k.clone(), *v)).collect();
    series.sort_by(|a, b| a.0.cmp(&b.0));
    let mut typed: HashSet<String> = HashSet::new();
    for (name, value) in series {
        let base = name.split('{').next().unwrap_or(&name).to_string();
        if typed.insert(base.clone()) {
            out.push_str(&format!("# TYPE {base} counter\n"));
        }
        out.push_str(&format!("{name} {value}\n"));
    }
    let gauges = [
        ("pifinder_differ_queue_length{lane=\"demand\"}", app.demand.lock().unwrap().len() as f64),
        ("pifinder_differ_queue_length{lane=\"warm\"}", app.warm.lock().unwrap().len() as f64),
        ("pifinder_differ_queue_length{lane=\"refine\"}", app.refine.lock().unwrap().len() as f64),
        ("pifinder_differ_inflight", app.inflight.lock().unwrap().len() as f64),
        ("pifinder_differ_active_sessions", app.sessions.lock().unwrap().len() as f64),
        ("pifinder_differ_nar_cache_bytes", nc_bytes as f64),
        ("pifinder_differ_nar_cache_files", nc_files as f64),
        ("pifinder_differ_blob_bytes", blob_bytes as f64),
        ("pifinder_differ_blob_files", blob_files as f64),
        ("pifinder_differ_upstream_known", app.upstream_mem.lock().unwrap().len() as f64),
    ];
    let mut typed_g: HashSet<&str> = HashSet::new();
    for (name, value) in gauges {
        let base = name.split('{').next().unwrap_or(name);
        if typed_g.insert(base) {
            out.push_str(&format!("# TYPE {base} gauge\n"));
        }
        out.push_str(&format!("{name} {value}\n"));
    }
    ([("content-type", "text/plain; version=0.0.4")], out).into_response()
}

async fn get_status(State(app): State<Arc<App>>) -> Response {
    let (nc_files, nc_bytes) = nar_cache_stats(&app);
    Json(serde_json::json!({
        "nar_cache": { "files": nc_files, "bytes": nc_bytes, "max_bytes": app.nar_cache_max },
        "demand_queue": app.demand.lock().unwrap().len(),
        "warm_queue": app.warm.lock().unwrap().len(),
        "refine_queue": app.refine.lock().unwrap().len(),
        "upstream_known": app.upstream_mem.lock().unwrap().len(),
        "inflight": app.inflight.lock().unwrap().len(),
        "jobs_done": app.jobs_done.load(Ordering::Relaxed),
        "jobs_failed": app.jobs_failed.load(Ordering::Relaxed),
        "rate_limited": app.rate_limited.load(Ordering::Relaxed),
        "active_sessions": app.sessions.lock().unwrap().len(),
        "warm_runs": *app.warm_runs.lock().unwrap(),
    }))
    .into_response()
}

/// Blob names are "<32hash>_<32hash>.zst".
fn blob_name_ok(name: &str) -> bool {
    name.len() == 69
        && name.ends_with(".zst")
        && name.as_bytes()[..65]
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}

async fn get_blob(State(app): State<Arc<App>>, AxPath(name): AxPath<String>) -> Response {
    if !blob_name_ok(&name) {
        return StatusCode::NOT_FOUND.into_response();
    }
    match tokio::fs::File::open(app.blob_dir.join(&name)).await {
        Ok(f) => {
            let stream = tokio_util::io::ReaderStream::new(f);
            (
                [("content-type", "application/zstd")],
                axum::body::Body::from_stream(stream),
            )
                .into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn get_health() -> &'static str {
    "ok"
}

// ---------------------------------------------------------------- main

#[tokio::main]
async fn main() -> Result<()> {
    let listen = std::env::var("DIFFER_LISTEN").unwrap_or_else(|_| "127.0.0.1:8090".into());
    let state_dir = PathBuf::from(
        std::env::var("DIFFER_STATE_DIR").unwrap_or_else(|_| "/var/lib/pifinder-differ".into()),
    );
    let workers: usize = std::env::var("DIFFER_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism().map(|n| n.get().saturating_sub(1).max(1)).unwrap_or(1)
        });
    let attic_url =
        std::env::var("DIFFER_ATTIC_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let attic_db = PathBuf::from(
        std::env::var("DIFFER_ATTIC_DB").unwrap_or_else(|_| "/var/lib/atticd/server.db".into()),
    );
    let caches: Vec<String> = std::env::var("DIFFER_CACHES")
        .unwrap_or_else(|_| "pifinder pifinder-release".into())
        .split_whitespace()
        .map(String::from)
        .collect();
    let upstream_url = std::env::var("DIFFER_UPSTREAM_URL")
        .unwrap_or_else(|_| "https://cache.nixos.org".into())
        .trim_end_matches('/')
        .to_string();
    let nar_cache_max: u64 = std::env::var("DIFFER_NAR_CACHE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10 * 1024 * 1024 * 1024); // 10 GiB

    let app = Arc::new(App {
        blob_dir: state_dir.join("blobs"),
        meta_dir: state_dir.join("meta"),
        tmp_dir: state_dir.join("tmp"),
        nar_cache_dir: state_dir.join("narcache"),
        nar_cache_max,
        attic_url,
        attic_db,
        caches,
        upstream_url,
        upstream_dir: state_dir.join("upstream-narinfo"),
        upstream_mem: Mutex::new(HashMap::new()),
        fetch_seq: AtomicU64::new(0),
        db: Mutex::new(None),
        demand: Mutex::new(VecDeque::new()),
        warm: Mutex::new(VecDeque::new()),
        refine: Mutex::new(VecDeque::new()),
        inflight: Mutex::new(HashSet::new()),
        priority_warms: Mutex::new(HashMap::new()),
        warm_runs: Mutex::new(Vec::new()),
        jobs_done: AtomicU64::new(0),
        jobs_failed: AtomicU64::new(0),
        job_done: tokio::sync::Notify::new(),
        metrics: Mutex::new(HashMap::new()),
        warm_seq: AtomicU64::new(0),
        rate: Mutex::new(HashMap::new()),
        sessions: Mutex::new(HashMap::new()),
        closures: Mutex::new(HashMap::new()),
        rate_limited: AtomicU64::new(0),
    });
    for d in [&app.blob_dir, &app.meta_dir, &app.tmp_dir, &app.nar_cache_dir, &app.upstream_dir] {
        std::fs::create_dir_all(d)?;
    }
    // Stale workdirs from a previous crash.
    if let Ok(rd) = std::fs::read_dir(&app.tmp_dir) {
        for entry in rd.flatten() {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }

    let refines = requeue_refines(&app);
    for _ in 0..workers {
        tokio::spawn(worker_loop(app.clone()));
    }
    log(&format!(
        "v0.4 listening on {listen}, {workers} workers, state {}, attic {}, upstream {}, {refines} refines queued",
        state_dir.display(),
        app.attic_db.display(),
        if app.upstream_url.is_empty() { "off" } else { &app.upstream_url },
    ));

    let router = Router::new()
        .route("/health", get(get_health))
        .route("/status", get(get_status))
        .route("/metrics", get(get_metrics))
        .route("/pairs", get(get_pairs))
        .route("/delta", post(post_delta))
        .route("/deltas", post(post_deltas))
        .route("/update-start", post(post_update_start))
        .route("/warm", post(post_warm))
        .route("/blobs/:name", get(get_blob))
        .layer(axum::middleware::from_fn_with_state(app.clone(), rate_limit_mw))
        .with_state(app);

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: &str = "1xm0hcqksxfy24p8m2xsfdas7wvyga76";

    #[test]
    fn split_store_path_accepts_a_store_path() {
        let (hash, name) = split_store_path(&format!("/nix/store/{H}-testpkg-1.1")).unwrap();
        assert_eq!(hash, H);
        assert_eq!(name, "testpkg-1.1");
    }

    #[test]
    fn split_store_path_rejects_bad_input() {
        for bad in [
            format!("/nix/store/{H}"),
            format!("/nix/store/{H}-"),
            format!("/nix/store/{H}-x/etc/passwd"),
            format!("/tmp/{H}-x"),
            "/nix/store/UPPERCASEUPPERCASEUPPERCASEUPPER-x".to_string(),
            "/nix/store/é".to_string(),
        ] {
            assert!(split_store_path(&bad).is_none(), "{bad}");
        }
    }

    fn narinfo(store_hash: &str, url: &str) -> String {
        format!(
            "StorePath: /nix/store/{store_hash}-glibc-2.42-84\n\
             URL: {url}\n\
             Compression: xz\n\
             FileHash: sha256:0000\n\
             FileSize: 100\n\
             NarHash: sha256:1111\n\
             NarSize: 31457280\n\
             References: {store_hash}-glibc-2.42-84 abcd0000abcd0000abcd0000abcd0000-libidn2-2.3.8\n\
             Deriver: 5b0v0000abcd0000abcd0000abcd0000-glibc-2.42-84.drv\n\
             Sig: cache.nixos.org-1:xyz\n"
        )
    }

    #[test]
    fn parse_narinfo_reads_upstream_fields() {
        let obj = parse_narinfo(&narinfo(H, "nar/0abc.nar.xz"), H).unwrap();
        assert_eq!(obj.store_path, format!("/nix/store/{H}-glibc-2.42-84"));
        assert_eq!(obj.nar_size, 31457280);
        assert_eq!(obj.references.len(), 2);
        assert_eq!(obj.deriver.as_deref(), Some("5b0v0000abcd0000abcd0000abcd0000-glibc-2.42-84.drv"));
        assert_eq!(
            obj.origin,
            Origin::Upstream { url: "nar/0abc.nar.xz".into(), compression: "xz".into() }
        );
    }

    #[test]
    fn parse_narinfo_rejects_other_path_or_bad_url() {
        let other = "abcd0000abcd0000abcd0000abcd0000";
        assert!(parse_narinfo(&narinfo(H, "nar/0abc.nar.xz"), other).is_none());
        for url in ["../x.nar", "/nar/x.nar", "https://evil/x.nar", ""] {
            assert!(parse_narinfo(&narinfo(H, url), H).is_none(), "{url}");
        }
    }

    #[test]
    fn final_level_splits_on_target_size() {
        assert_eq!(final_level(1024), FINAL_LEVEL_SMALL);
        assert_eq!(final_level(LARGE_TARGET_BYTES), FINAL_LEVEL_LARGE);
        assert!(FAST_LEVEL < FINAL_LEVEL_LARGE);
    }

    #[test]
    fn blob_name_ok_accepts_pair_names() {
        assert!(blob_name_ok(&format!("{H}_{H}.zst")));
    }

    #[test]
    fn blob_name_ok_rejects_other_names() {
        let non_ascii = format!("{}é.zst", "a".repeat(63));
        assert_eq!(non_ascii.len(), 69);
        for bad in [
            non_ascii,
            format!("{H}_{H}.txt"),
            format!("../{H}_{H}.zst"),
            format!("{H}-{H}.zst"),
        ] {
            assert!(!blob_name_ok(&bad), "{bad}");
        }
    }
}
