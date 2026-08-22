// pifinder-differ v0.2 — on-demand + self-warming binary delta server for the
// PiFinder NixOS update transport.
//
// Runs beside atticd and works from the cache itself, not from a nix store:
//   - metadata (closures, references, NAR sizes) comes from atticd's SQLite
//     database, opened read-only;
//   - candidate bases are ranked by FastCDC chunk overlap (Jaccard) straight
//     from the DB — no bytes fetched to pick a winner;
//   - NAR bytes are fetched from atticd over loopback and patched NAR-to-NAR
//     with zstd --patch-from. NARs are canonical on both ends (`nix-store
//     --dump` on the device), so no export-stream/deriver nondeterminism.
//
// Endpoints:
//   POST /delta  — a device names a target and the bases it holds (demand)
//   POST /warm   — enqueue every stem-paired path between two toplevels
//   GET  /pairs  — every computed pair with sizes/ratios
//   GET  /status — queues, counters, warm-run progress
//   GET  /blobs/<base>_<target>.zst
//
// Demand jobs always run before warm jobs. All compute runs at the unit's
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
use axum::http::StatusCode;
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
const DEMAND_QUEUE_CAP: usize = 64;
const WARM_QUEUE_CAP: usize = 10_000;

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
    db: Mutex<Option<Connection>>,
    demand: Mutex<VecDeque<Job>>,
    warm: Mutex<VecDeque<Job>>,
    inflight: Mutex<HashSet<String>>, // target hashes being computed
    warm_runs: Mutex<Vec<WarmRun>>,
    jobs_done: AtomicU64,
    jobs_failed: AtomicU64,
    warm_seq: AtomicU64,
    rate: Mutex<HashMap<String, TokenBucket>>,
    sessions: Mutex<HashMap<String, Session>>,
    rate_limited: AtomicU64,
}

struct TokenBucket {
    tokens: f64,
    last: Instant,
}

struct Session {
    budget: i64,
    expires: Instant,
}

#[derive(Clone)]
struct Job {
    target: String,       // full /nix/store/... path
    bases: Vec<String>,   // candidate bases, full paths, best guess first
    source: &'static str, // "demand" | "warm"
}

#[derive(Clone, Serialize)]
struct WarmRun {
    id: u64,
    base_toplevel: String,
    target_toplevel: String,
    state: String, // pairing | queued | failed
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
    #[serde(default)]
    chunk_overlap: f64, // Jaccard overlap of the chosen base, for observability
    compute_ms: u64,
    rank_ms: u64,
    candidates_ranked: usize,
    source: String,
    created_unix: u64,
    rejected: bool, // true: patch exceeded KEEP_RATIO, no blob kept
}

// Row from attic's `object` × `nar` tables.
struct AtticObject {
    store_path: String,
    references: Vec<String>, // basenames ("<hash>-<name>")
    deriver: Option<String>,
    nar_id: i64,
    nar_size: u64,
    cache: String,
}

// ---------------------------------------------------------------- helpers

fn log(msg: &str) {
    eprintln!("[pifinder-differ] {msg}");
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
fn attic_object(app: &App, sph: &str) -> Result<Option<AtticObject>> {
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
        let mut best: Option<AtticObject> = None;
        for (store_path, refs_json, deriver, nar_id, nar_size, cache) in rows {
            let rank = app.caches.iter().position(|c| *c == cache);
            let Some(rank) = rank else { continue };
            let current_rank = best
                .as_ref()
                .and_then(|b| app.caches.iter().position(|c| *c == b.cache))
                .unwrap_or(usize::MAX);
            if rank < current_rank {
                let references: Vec<String> =
                    serde_json::from_str(&refs_json).unwrap_or_default();
                best = Some(AtticObject {
                    store_path,
                    references,
                    deriver,
                    nar_id,
                    nar_size: nar_size.max(0) as u64,
                    cache,
                });
            }
        }
        best
    })
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

/// Full runtime closure of a toplevel, walked via `references` in the DB.
/// Returns full store paths. Paths whose object rows are missing (GC holes)
/// are skipped — they can't be patched or fetched anyway.
fn attic_closure(app: &App, toplevel_sph: &str) -> Result<Vec<String>> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut order: Vec<String> = Vec::new();
    let mut queue: VecDeque<String> = VecDeque::from([toplevel_sph.to_string()]);
    let mut missing = 0usize;
    while let Some(sph) = queue.pop_front() {
        if !seen.insert(sph.clone()) {
            continue;
        }
        match attic_object(app, &sph)? {
            None => missing += 1,
            Some(obj) => {
                order.push(obj.store_path.clone());
                for r in &obj.references {
                    if let Some((h, _)) = split_store_path(&format!("/nix/store/{r}")) {
                        if !seen.contains(&h) {
                            queue.push_back(h);
                        }
                    }
                }
            }
        }
    }
    if missing > 0 {
        log(&format!("closure of {toplevel_sph}: {missing} paths missing from attic (GC holes), skipped"));
    }
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

/// Get a decompressed NAR, from the local LRU cache or from atticd over
/// loopback. Returns the cache path and the NAR size.
async fn get_nar(app: &App, cache: &str, sph: &str) -> Result<(PathBuf, u64)> {
    let cached = app.nar_cache_dir.join(format!("{sph}.nar"));
    if let Ok(md) = tokio::fs::metadata(&cached).await {
        // Bump mtime so LRU keeps hot NARs.
        let _ = run(Command::new("touch").arg(&cached).stdin(Stdio::null())).await;
        return Ok((cached, md.len()));
    }
    let tmp = app.tmp_dir.join(format!("fetch-{sph}.nar"));
    let size = fetch_nar(app, cache, sph, &tmp).await?;
    tokio::fs::rename(&tmp, &cached).await?;
    evict_nar_cache(app);
    Ok((cached, size))
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

async fn compute(app: &App, job: &Job) -> Result<PairMeta> {
    let (t_hash, t_name) = split_store_path(&job.target)
        .ok_or_else(|| anyhow!("bad target {}", job.target))?;

    if free_bytes(&app.tmp_dir).await? < MIN_FREE_BYTES {
        bail!("low disk, skipping {t_name}");
    }

    let work = app.tmp_dir.join(&t_hash);
    tokio::fs::create_dir_all(&work).await?;
    let result = compute_inner(app, job, &t_hash, &work).await;
    let _ = tokio::fs::remove_dir_all(&work).await;
    result
}

async fn compute_inner(app: &App, job: &Job, t_hash: &str, work: &Path) -> Result<PairMeta> {
    let started = Instant::now();

    let target_obj = attic_object(app, t_hash)?
        .ok_or_else(|| anyhow!("target {} not in attic", job.target))?;

    // Rank candidates by chunk overlap — DB only, no bytes fetched.
    let rank_started = Instant::now();
    let target_chunks = chunk_set(app, target_obj.nar_id)?;
    let mut ranked: Vec<(f64, String, AtticObject)> = Vec::new();
    for base in &job.bases {
        let Some((b_hash, _)) = split_store_path(base) else { continue };
        let Some(obj) = attic_object(app, &b_hash)? else { continue };
        let overlap = jaccard(&target_chunks, &chunk_set(app, obj.nar_id)?);
        ranked.push((overlap, b_hash, obj));
    }
    let rank_ms = rank_started.elapsed().as_millis() as u64;
    let candidates_ranked = ranked.len();
    let overlap_meaningful = target_chunks.len() >= MIN_CHUNKS_FOR_OVERLAP;
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
    if overlap_meaningful
        && ranked.first().map(|(o, _, _)| *o < MIN_CHUNK_OVERLAP).unwrap_or(false)
    {
        let best_overlap = ranked.first().map(|(o, _, _)| *o).unwrap_or(0.0);
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
                chunk_overlap: *overlap,
                compute_ms: started.elapsed().as_millis() as u64,
                rank_ms,
                candidates_ranked,
                source: job.source.into(),
                created_unix: now_unix(),
                rejected: true,
            };
            let tmp_meta = work.join(format!("meta-{key}.json"));
            tokio::fs::write(&tmp_meta, serde_json::to_vec_pretty(&meta)?).await?;
            tokio::fs::rename(&tmp_meta, meta_path(app, &key)).await?;
        }
        bail!("best chunk overlap {best_overlap:.3} below floor {MIN_CHUNK_OVERLAP} — full download, nothing fetched");
    }

    let (overlap, b_hash, base_obj) = ranked
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no candidate base present in attic"))?;

    let wlog = window_log_for(base_obj.nar_size, target_obj.nar_size);
    if (1u64 << wlog) < base_obj.nar_size.max(target_obj.nar_size) {
        bail!(
            "NAR too large for max window ({} B > 2^{MAX_WINDOW_LOG})",
            base_obj.nar_size.max(target_obj.nar_size)
        );
    }

    // NARs come from the local LRU cache, falling back to atticd/S3.
    let (base_nar, _) = get_nar(app, &base_obj.cache, &b_hash).await?;
    let (target_nar, target_size) = get_nar(app, &target_obj.cache, t_hash).await?;

    let level = if target_size < LARGE_TARGET_BYTES { FINAL_LEVEL_SMALL } else { FINAL_LEVEL_LARGE };
    let patch_tmp = work.join("patch.zst");
    let patch_size = zstd_patch(level, wlog, &base_nar, &target_nar, &patch_tmp).await?;
    let nar_sha256 = sha256_file(&target_nar).await?;

    let key = pair_key(&b_hash, t_hash);
    let rejected = (patch_size as f64) > (target_size as f64) * KEEP_RATIO;
    if rejected {
        let _ = tokio::fs::remove_file(&patch_tmp).await;
    } else {
        tokio::fs::rename(&patch_tmp, blob_path(app, &key)).await?;
    }

    let meta = PairMeta {
        base: base_obj.store_path,
        target: job.target.clone(),
        algo: ALGO.into(),
        window_log: wlog,
        level,
        patch_size,
        nar_size: target_size,
        nar_sha256,
        references: target_obj
            .references
            .iter()
            .map(|r| format!("/nix/store/{r}"))
            .collect(),
        deriver: target_obj.deriver.clone(),
        chunk_overlap: overlap,
        compute_ms: started.elapsed().as_millis() as u64,
        rank_ms,
        candidates_ranked,
        source: job.source.into(),
        created_unix: now_unix(),
        rejected,
    };
    let tmp_meta = work.join("meta.json");
    tokio::fs::write(&tmp_meta, serde_json::to_vec_pretty(&meta)?).await?;
    tokio::fs::rename(&tmp_meta, meta_path(app, &key)).await?;
    Ok(meta)
}

// ---------------------------------------------------------------- workers

fn pop_job(app: &App) -> Option<Job> {
    if let Some(j) = app.demand.lock().unwrap().pop_front() {
        return Some(j);
    }
    app.warm.lock().unwrap().pop_front()
}

async fn worker_loop(app: Arc<App>) {
    loop {
        let Some(job) = pop_job(&app) else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        let t = job.target.clone();
        match compute(&app, &job).await {
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
        if let Some((h, _)) = split_store_path(&t) {
            app.inflight.lock().unwrap().remove(&h);
        }
    }
}

/// Queue a job unless the target is already computed against one of the given
/// bases, already inflight, or the queue is full. Returns what happened.
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
    if !app.inflight.lock().unwrap().insert(t_hash) {
        return "inflight";
    }
    let (q, cap) = if demand {
        (&app.demand, DEMAND_QUEUE_CAP)
    } else {
        (&app.warm, WARM_QUEUE_CAP)
    };
    let mut q = q.lock().unwrap();
    if q.len() >= cap {
        if let Some((h, _)) = split_store_path(&job.target) {
            app.inflight.lock().unwrap().remove(&h);
        }
        return "full";
    }
    q.push_back(job);
    "queued"
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

fn new_session(app: &App, budget: i64) -> String {
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
    sessions.insert(token.clone(), Session { budget, expires: now + SESSION_TTL });
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
    let closure = match attic_closure(&app, &sph) {
        Ok(c) if !c.is_empty() => c,
        Ok(_) => return (StatusCode::NOT_FOUND, "toplevel not in cache").into_response(),
        Err(e) => {
            log(&format!("update-start failed: {e:#}"));
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let budget = 2 * closure.len() as i64 + SESSION_SLACK;
    let token = new_session(&app, budget);
    Json(serde_json::json!({
        "session": token,
        "budget": budget,
        "expires_in": SESSION_TTL.as_secs(),
    }))
    .into_response()
}

// ---------------------------------------------------------------- HTTP

#[derive(Deserialize)]
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

async fn post_delta(State(app): State<Arc<App>>, Json(req): Json<DeltaReq>) -> Response {
    let Some((t_hash, _)) = split_store_path(&req.target) else {
        return (StatusCode::BAD_REQUEST, "target is not a store path").into_response();
    };
    let bases: Vec<String> =
        req.bases.iter().filter(|b| split_store_path(b).is_some()).cloned().collect();
    if bases.is_empty() {
        return (StatusCode::BAD_REQUEST, "no valid bases").into_response();
    }

    let mut all_rejected = true;
    for base in &bases {
        let (b_hash, _) = split_store_path(base).unwrap();
        let key = pair_key(&b_hash, &t_hash);
        if let Some(meta) = load_meta(&app, &key) {
            if meta.rejected {
                continue;
            }
            return Json(DeltaHit {
                algo: meta.algo,
                basis: vec![meta.base],
                window_log: meta.window_log,
                url: format!("/blobs/{key}.zst"),
                size: meta.patch_size,
                nar_size: meta.nar_size,
                nar_sha256: meta.nar_sha256,
                references: meta.references,
                deriver: meta.deriver,
            })
            .into_response();
        }
        all_rejected = false;
    }
    if all_rejected {
        return StatusCode::NO_CONTENT.into_response();
    }

    match enqueue(&app, Job { target: req.target, bases, source: "demand" }, true) {
        "full" => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        // queued | inflight | exists-under-other-base: tell the device to retry
        _ => (StatusCode::ACCEPTED, [("retry-after", "15")], "computing").into_response(),
    }
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
    let id = app.warm_seq.fetch_add(1, Ordering::Relaxed) + 1;
    app.warm_runs.lock().unwrap().push(WarmRun {
        id,
        base_toplevel: req.base_toplevel.clone(),
        target_toplevel: req.target_toplevel.clone(),
        state: "pairing".into(),
        paired: 0,
        skipped_existing: 0,
        unpaired: 0,
        error: None,
        started_unix: now_unix(),
    });
    let app2 = app.clone();
    tokio::spawn(async move {
        let res = warm_run(&app2, id, &req.base_toplevel, &req.target_toplevel).await;
        let mut runs = app2.warm_runs.lock().unwrap();
        if let Some(r) = runs.iter_mut().find(|r| r.id == id) {
            if let Err(e) = res {
                r.state = "failed".into();
                r.error = Some(format!("{e:#}"));
            }
        }
    });
    (StatusCode::ACCEPTED, Json(serde_json::json!({ "warm_id": id }))).into_response()
}

async fn warm_run(app: &Arc<App>, id: u64, base_top: &str, target_top: &str) -> Result<()> {
    let (base_sph, _) = split_store_path(base_top).unwrap();
    let (target_sph, _) = split_store_path(target_top).unwrap();

    // Closure walks are pure DB reads — cheap enough to run inline.
    let base_closure = attic_closure(app, &base_sph)?;
    let target_closure = attic_closure(app, &target_sph)?;
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
        match enqueue(app, job, false) {
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

async fn get_status(State(app): State<Arc<App>>) -> Response {
    let (nc_files, nc_bytes) = nar_cache_stats(&app);
    Json(serde_json::json!({
        "nar_cache": { "files": nc_files, "bytes": nc_bytes, "max_bytes": app.nar_cache_max },
        "demand_queue": app.demand.lock().unwrap().len(),
        "warm_queue": app.warm.lock().unwrap().len(),
        "inflight": app.inflight.lock().unwrap().len(),
        "jobs_done": app.jobs_done.load(Ordering::Relaxed),
        "jobs_failed": app.jobs_failed.load(Ordering::Relaxed),
        "rate_limited": app.rate_limited.load(Ordering::Relaxed),
        "active_sessions": app.sessions.lock().unwrap().len(),
        "warm_runs": *app.warm_runs.lock().unwrap(),
    }))
    .into_response()
}

async fn get_blob(State(app): State<Arc<App>>, AxPath(name): AxPath<String>) -> Response {
    // Names are "<32hash>_<32hash>.zst" — reject anything else.
    let ok = name.len() == 69
        && name.ends_with(".zst")
        && name[..65]
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !ok {
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
        db: Mutex::new(None),
        demand: Mutex::new(VecDeque::new()),
        warm: Mutex::new(VecDeque::new()),
        inflight: Mutex::new(HashSet::new()),
        warm_runs: Mutex::new(Vec::new()),
        jobs_done: AtomicU64::new(0),
        jobs_failed: AtomicU64::new(0),
        warm_seq: AtomicU64::new(0),
        rate: Mutex::new(HashMap::new()),
        sessions: Mutex::new(HashMap::new()),
        rate_limited: AtomicU64::new(0),
    });
    for d in [&app.blob_dir, &app.meta_dir, &app.tmp_dir, &app.nar_cache_dir] {
        std::fs::create_dir_all(d)?;
    }
    // Stale workdirs from a previous crash.
    if let Ok(rd) = std::fs::read_dir(&app.tmp_dir) {
        for entry in rd.flatten() {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }

    for _ in 0..workers {
        tokio::spawn(worker_loop(app.clone()));
    }
    log(&format!(
        "v0.2 listening on {listen}, {workers} workers, state {}, attic {}",
        state_dir.display(),
        app.attic_db.display()
    ));

    let router = Router::new()
        .route("/health", get(get_health))
        .route("/status", get(get_status))
        .route("/pairs", get(get_pairs))
        .route("/delta", post(post_delta))
        .route("/update-start", post(post_update_start))
        .route("/warm", post(post_warm))
        .route("/blobs/:name", get(get_blob))
        .layer(axum::middleware::from_fn_with_state(app.clone(), rate_limit_mw))
        .with_state(app);

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    axum::serve(listener, router).await?;
    Ok(())
}
