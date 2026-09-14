//! File-backed durable stores for the attempt and gate clients (D1/D2, P3-438).
//!
//! This is the durable implementation the module header of [`crate::durable`] defers to: with it
//! the flags stop being inert, a restart can be modelled by closing and reopening the same
//! directory, and the two contracts the in-memory store only *states* become enforced:
//! [`AttemptStore::try_claim`] classifies and claims as one durable operation, and
//! [`FileGateStore::init`] creates a gate row that is absent instead of failing open.
//!
//! ## On-disk format
//!
//! One JSONL file per store (`<dir>/attempts.jsonl`, `<dir>/gate.jsonl`): one JSON object per line,
//! field names as written by the `*Record` structs below, phase and gate state as the uppercase wire
//! names the Java rows use (`PREPARED`, `ENABLED`, …). Every append is `write_all` followed by
//! `sync_data` before the call returns, so a record a caller has seen acknowledged is on disk. The
//! log *is* the primary key space: replay rebuilds the view and the latest record per key wins.
//!
//! Two failure shapes are distinguished deliberately, because they have opposite readings:
//!
//! * a **torn final line** — the process died inside its own append, so the write never returned and
//!   no caller ever proceeded on it — is dropped with a warning;
//! * a corrupt line *anywhere else*, and a file that cannot be read at all, is a hard error.
//!   Treating an unreadable store as an empty one is the fail-open shape the Java store documents:
//!   reading "unavailable" as "absent" mints a duplicate PREPARED attempt.
//!
//! ## One actor per log
//!
//! `try_claim` is atomic *within* one process: one mutex spans classify-and-append, so a concurrent
//! second claim of the same identity observes the first one's record.
//! Two processes sharing a log would each classify against their own view and could both mint a
//! PREPARED attempt for the same `(instruction_id, request_hash)`, so each store takes an exclusive
//! lock file (`<log>.lock`, holding its pid) and refuses to open a log whose lock names a live
//! `/proc` entry — including a second store in the same process. A pid that is gone is a stale lock
//! from a `kill -9` and is taken over, so a crash never leaves the service unable to start.
//! NOTE: this is Linux-only (it reads `/proc`) and single-host. A store shared between hosts is the
//! Fluss-backed client's job, which the Java side already implements; this file does not pretend to
//! that guarantee.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::executiongate::{
    Attempt, AttemptPhase, AttemptStore, Claim, GateRow, GateState, GateStateStore,
};

/// File names inside the durable directory. The names are part of the operator-visible layout
/// (`DURABLE_DIR`), so changing one is a migration, not a rename.
pub const ATTEMPTS_LOG: &str = "attempts.jsonl";
pub const GATE_LOG: &str = "gate.jsonl";

/// Locks a store's state, turning poisoning into an error.
///
/// A poisoned mutex means a previous holder panicked mid-mutation, so the store cannot know whether
/// its indexes still match its log. Every caller that can refuse, refuses — rather than panicking
/// into a 500 (P3-454 keeps `expect` out of the request path) or reporting an all-clear.
fn lock<T>(state: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    state
        .lock()
        .map_err(|_| anyhow::anyhow!("durable store lock poisoned by an earlier panic"))
}

// ── Records: the on-disk contract ──────────────────────────────────────────────────────────────

/// One `attempts.jsonl` line. Field names are the file format: renaming a field is a format change.
#[derive(Debug, Serialize, Deserialize)]
struct AttemptRecord {
    attempt_id: String,
    instruction_id: String,
    request_hash: String,
    client_order_ref: String,
    phase: String,
    #[serde(default)]
    broker_order_id: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

impl AttemptRecord {
    fn of(attempt: &Attempt) -> Self {
        Self {
            attempt_id: attempt.attempt_id.clone(),
            instruction_id: attempt.instruction_id.clone(),
            request_hash: attempt.request_hash.clone(),
            client_order_ref: attempt.client_order_ref.clone(),
            phase: attempt.phase.as_str().to_string(),
            broker_order_id: attempt.broker_order_id.clone(),
            reason: attempt.reason.clone(),
        }
    }

    fn into_attempt(self) -> Result<Attempt> {
        Ok(Attempt {
            attempt_id: self.attempt_id,
            instruction_id: self.instruction_id,
            request_hash: self.request_hash,
            client_order_ref: self.client_order_ref,
            // An unknown phase is not guessed at: a record this build cannot interpret means the
            // log was written by something else, and resuming from a guessed phase is how a
            // duplicate bridge call happens.
            phase: match self.phase.as_str() {
                "PREPARED" => AttemptPhase::Prepared,
                "SUBMITTING" => AttemptPhase::Submitting,
                "ACCEPTED" => AttemptPhase::Accepted,
                "REJECTED" => AttemptPhase::Rejected,
                "UNKNOWN" => AttemptPhase::Unknown,
                other => bail!("attempt log holds an unknown phase {other:?}"),
            },
            broker_order_id: self.broker_order_id,
            reason: self.reason,
        })
    }
}

/// One `gate.jsonl` line.
#[derive(Debug, Serialize, Deserialize)]
struct GateRecord {
    partition: String,
    owner: String,
    state: String,
    epoch: u64,
    fence_token: u64,
}

impl GateRecord {
    fn of(row: &GateRow) -> Self {
        Self {
            partition: row.partition.clone(),
            owner: row.owner.clone(),
            state: gate_state_str(row.state).to_string(),
            epoch: row.epoch,
            fence_token: row.fence_token,
        }
    }

    fn into_row(self) -> Result<GateRow> {
        Ok(GateRow {
            partition: self.partition,
            owner: self.owner,
            state: match self.state.as_str() {
                "ENABLED" => GateState::Enabled,
                "HALTED" => GateState::Halted,
                other => bail!("gate log holds an unknown state {other:?}"),
            },
            epoch: self.epoch,
            fence_token: self.fence_token,
        })
    }
}

fn gate_state_str(state: GateState) -> &'static str {
    match state {
        GateState::Enabled => "ENABLED",
        GateState::Halted => "HALTED",
    }
}

// ── The log ───────────────────────────────────────────────────────────────────────────────────

/// Creates the directory a store file lives in, if it is not there yet.
///
/// Both the lock and the log need it, and the lock is taken first — the live smoke of the flag path
/// caught the version that only did this in the log ("writing lock …: No such file or directory"),
/// which made every first start with a fresh `DURABLE_DIR` fail.
fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating durable store directory {}", dir.display()))?;
        }
    }
    Ok(())
}

/// Append-only JSONL log with a replay-on-open and an fsync'd append.
struct JsonlLog {
    path: PathBuf,
    file: File,
    /// True when the previous process died inside an append and its torn line was dropped.
    dropped_torn_tail: bool,
}

impl JsonlLog {
    fn open(path: &Path) -> Result<(Self, Vec<serde_json::Value>)> {
        ensure_parent_dir(path)?;
        let (records, dropped_torn_tail) = replay(path)?;
        // `append(true)` + `create(true)`: every write goes to the end of the file, so a record this
        // process writes can never overwrite an earlier one.
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .with_context(|| format!("opening durable log {}", path.display()))?;
        Ok((
            Self {
                path: path.to_path_buf(),
                file,
                dropped_torn_tail,
            },
            records,
        ))
    }

    /// Appends one record and does not return until it is on disk.
    fn append<T: Serialize>(&mut self, record: &T) -> Result<()> {
        let mut line = serde_json::to_string(record).context("encoding a durable record")?;
        line.push('\n');
        use std::io::Write;
        self.file
            .write_all(line.as_bytes())
            .with_context(|| format!("appending to {}", self.path.display()))?;
        self.file
            .sync_data()
            .with_context(|| format!("syncing {}", self.path.display()))?;
        Ok(())
    }
}

/// Reads a log back. Returns the decoded records and whether a torn final line was dropped.
fn replay(path: &Path) -> Result<(Vec<serde_json::Value>, bool)> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        // Only a *missing* file is an empty log. Anything else (permission, I/O) propagates.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), false)),
        Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
    };
    if bytes.is_empty() {
        return Ok((Vec::new(), false));
    }
    let text = std::str::from_utf8(&bytes)
        .with_context(|| format!("{} is not UTF-8", path.display()))?;
    let mut parts: Vec<&str> = text.split('\n').collect();
    // `split` yields a trailing "" exactly when the file ends with a newline. Pop only that
    // terminator — popping unconditionally would discard the very torn line this checks for.
    let last_line_terminated = parts.last().is_some_and(|last| last.is_empty());
    if last_line_terminated {
        parts.pop();
    }
    let total = parts.len();
    let mut records = Vec::new();
    for (index, raw) in parts.iter().enumerate() {
        if raw.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(raw) {
            Ok(value) => records.push(value),
            Err(err) => {
                if index + 1 == total && !last_line_terminated {
                    // The process died inside this append, so no caller ever saw it acknowledged.
                    return Ok((records, true));
                }
                bail!(
                    "{}: line {} is not a record this store wrote ({err})",
                    path.display(),
                    index + 1
                );
            }
        }
    }
    Ok((records, false))
}

// ── One actor per log ─────────────────────────────────────────────────────────────────────────

/// Exclusive lock on one log, released when the store is dropped.
struct WriterLock {
    path: PathBuf,
}

impl WriterLock {
    fn acquire(log: &Path) -> Result<Self> {
        ensure_parent_dir(log)?;
        let path = log.with_extension("lock");
        if let Ok(existing) = std::fs::read_to_string(&path) {
            let holder = existing.trim();
            match holder.parse::<u32>() {
                Ok(pid) if Path::new(&format!("/proc/{pid}")).exists() => bail!(
                    "{} is locked by live pid {pid}: one actor per durable log (P3-201)",
                    log.display()
                ),
                // A lock whose process is gone is a `kill -9` leftover and is taken over; a lock we
                // cannot read is not ours to take.
                Ok(_) => {}
                Err(_) => bail!(
                    "{} exists but does not name a pid, refusing to take it over",
                    path.display()
                ),
            }
        }
        std::fs::write(&path, format!("{}\n", std::process::id()))
            .with_context(|| format!("writing lock {}", path.display()))?;
        Ok(Self { path })
    }
}

impl Drop for WriterLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

// ── Attempt store ─────────────────────────────────────────────────────────────────────────────

/// Durable [`AttemptStore`] backed by `<dir>/attempts.jsonl`.
///
/// The log and the three indexes sit behind one mutex: classify-and-claim is atomic, and no lock
/// ordering has to be reasoned about. The mutex is held across `write_all` + `sync_data`, so a claim
/// is never visible to another caller before it is on disk.
pub struct FileAttemptStore {
    inner: Mutex<AttemptInner>,
    _lock: WriterLock,
}

struct AttemptInner {
    log: JsonlLog,
    // Same three indexes as `InMemoryAttemptStore`, kept in step with the log. `by_id` is the
    // primary key; `dup` is `instruction_id -> request hashes`; `by_instruction` is the
    // `instruction_id` existence index. Nested sets rather than tuple keys so the lookups borrow.
    by_id: HashMap<String, Attempt>,
    dup: HashMap<String, HashSet<String>>,
    by_instruction: HashMap<String, ()>,
}

impl AttemptInner {
    /// Folds one record into the indexes. A later record for the same `attempt_id` supersedes the
    /// earlier one (that is how a phase transition is stored); a *second claim* of an
    /// `(instruction_id, request_hash)` already in the log means two actors wrote this file, which
    /// is exactly what the lock exists to prevent, so it is refused rather than replayed.
    fn fold(&mut self, attempt: Attempt) -> Result<()> {
        if !self.by_id.contains_key(&attempt.attempt_id) {
            if self
                .dup
                .get(&attempt.instruction_id)
                .is_some_and(|hashes| hashes.contains(&attempt.request_hash))
            {
                bail!(
                    "attempt log records two attempts for instruction {} and hash {}: \
                     this file was written by more than one actor",
                    attempt.instruction_id,
                    attempt.request_hash
                );
            }
            self.dup
                .entry(attempt.instruction_id.clone())
                .or_default()
                .insert(attempt.request_hash.clone());
            self.by_instruction
                .insert(attempt.instruction_id.clone(), ());
        }
        self.by_id.insert(attempt.attempt_id.clone(), attempt);
        Ok(())
    }
}

impl FileAttemptStore {
    /// Opens (or creates) the log and replays it into memory.
    pub fn open(path: &Path) -> Result<Self> {
        let lock = WriterLock::acquire(path)?;
        let (log, records) = JsonlLog::open(path)?;
        let mut inner = AttemptInner {
            log,
            by_id: HashMap::new(),
            dup: HashMap::new(),
            by_instruction: HashMap::new(),
        };
        for value in records {
            let record: AttemptRecord = serde_json::from_value(value)
                .with_context(|| format!("decoding a record from {}", path.display()))?;
            inner.fold(record.into_attempt().with_context(|| {
                format!("decoding a record from {}", path.display())
            })?)?;
        }
        Ok(Self {
            inner: Mutex::new(inner),
            _lock: lock,
        })
    }

    /// True when the previous process died inside an append and the torn line was dropped.
    pub fn dropped_torn_tail(&self) -> bool {
        // Drives a warning line at boot only. A poisoned lock cannot answer the question, and
        // reporting the all-clear is the wrong direction, so it reports the tail as dropped.
        lock(&self.inner)
            .map(|inner| inner.log.dropped_torn_tail)
            .unwrap_or(true)
    }
}

impl AttemptStore for FileAttemptStore {
    fn get(&self, attempt_id: &str) -> Option<Attempt> {
        lock(&self.inner)
            .ok()
            .and_then(|inner| inner.by_id.get(attempt_id).cloned())
    }

    fn put(&self, attempt: &Attempt) -> Result<()> {
        // Durable first, inside the lock: if the append fails the in-memory view is untouched, and
        // the next open replays whatever actually reached the disk.
        let mut inner = lock(&self.inner)?;
        inner.log.append(&AttemptRecord::of(attempt))?;
        inner.fold(attempt.clone())
    }

    // A poisoned lock has no safe `false` here — "no duplicate" would authorise a second send — so
    // these two answer `true` (refuse) when they cannot read the index. `try_claim` below never
    // guesses: it fails outright.
    fn has_duplicate(&self, instruction_id: &str, request_hash: &str) -> bool {
        lock(&self.inner)
            .map(|inner| {
                inner
                    .dup
                    .get(instruction_id)
                    .is_some_and(|hashes| hashes.contains(request_hash))
            })
            .unwrap_or(true)
    }

    fn has_instruction(&self, instruction_id: &str) -> bool {
        lock(&self.inner)
            .map(|inner| inner.by_instruction.contains_key(instruction_id))
            .unwrap_or(true)
    }

    fn try_claim(
        &self,
        attempt_id: &str,
        instruction_id: &str,
        request_hash: &str,
        client_order_ref: &str,
    ) -> Result<Claim> {
        // One lock spans the whole classification: no other caller can observe "nothing durable"
        // and then also claim. The append below is the only thing that makes the claim true, and it
        // is on disk before this returns.
        let mut inner = lock(&self.inner)?;
        if let Some(existing) = inner.by_id.get(attempt_id) {
            return Ok(Claim::Existing(existing.clone()));
        }
        if inner
            .dup
            .get(instruction_id)
            .is_some_and(|hashes| hashes.contains(request_hash))
        {
            return Ok(Claim::Duplicate);
        }
        if inner.by_instruction.contains_key(instruction_id) {
            return Ok(Claim::ContractViolation);
        }
        let attempt = Attempt::new(
            attempt_id,
            instruction_id,
            request_hash,
            client_order_ref,
            AttemptPhase::Prepared,
        );
        inner.log.append(&AttemptRecord::of(&attempt))?;
        inner.fold(attempt.clone())?;
        Ok(Claim::Claimed(attempt))
    }
}

// ── Gate store ────────────────────────────────────────────────────────────────────────────────

/// Durable [`GateStateStore`] backed by `<dir>/gate.jsonl`.
///
/// This is the store that makes D2 possible: `init` **creates** a gate row that is absent, so a
/// deliberate halt can persist "this partition is HALTED" instead of writing nothing. The
/// incidental halts in [`crate::executiongate::ExecutionGate`] must not come through here — a halt
/// raised *because* no row vouches for the partition must not then invent one.
pub struct FileGateStore {
    inner: Mutex<GateInner>,
    _lock: WriterLock,
}

struct GateInner {
    log: JsonlLog,
    by_partition: HashMap<String, GateRow>,
}

impl FileGateStore {
    pub fn open(path: &Path) -> Result<Self> {
        let lock = WriterLock::acquire(path)?;
        let (log, records) = JsonlLog::open(path)?;
        let mut by_partition = HashMap::new();
        for value in records {
            let record: GateRecord = serde_json::from_value(value)
                .with_context(|| format!("decoding a gate record from {}", path.display()))?;
            let row = record
                .into_row()
                .with_context(|| format!("decoding a gate record from {}", path.display()))?;
            // Latest row per partition wins: that is how a transition is stored.
            by_partition.insert(row.partition.clone(), row);
        }
        Ok(Self {
            inner: Mutex::new(GateInner { log, by_partition }),
            _lock: lock,
        })
    }

}

impl GateStateStore for FileGateStore {
    fn read(&self, partition: &str) -> Option<GateRow> {
        lock(&self.inner)
            .ok()
            .and_then(|inner| inner.by_partition.get(partition).cloned())
    }

    fn write(&self, row: &GateRow) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        inner.log.append(&GateRecord::of(row))?;
        inner.by_partition.insert(row.partition.clone(), row.clone());
        Ok(())
    }

    /// Create-if-absent, on disk (see the trait contract for why this exists and who may call it).
    /// Only the create path appends, so a row that already exists is not written again and a fenced
    /// gate survives a restart untouched.
    fn init(&self, row: &GateRow) -> Result<GateRow> {
        let mut inner = lock(&self.inner)?;
        if let Some(existing) = inner.by_partition.get(&row.partition) {
            return Ok(existing.clone());
        }
        inner.log.append(&GateRecord::of(row))?;
        inner.by_partition.insert(row.partition.clone(), row.clone());
        Ok(row.clone())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A fresh directory per test: no `tempfile` dependency, and no clock in the name.
    fn scratch_dir() -> PathBuf {
        static N: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "nautilus-durable-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn attempts_log(dir: &Path) -> PathBuf {
        dir.join("attempts.jsonl")
    }

    // ── Restart recovery: a *reopen*, which the in-memory store cannot model (P3-438) ───────────

    #[test]
    fn an_attempt_survives_close_and_reopen() {
        let dir = scratch_dir();
        let path = attempts_log(&dir);
        let claimed = {
            let store = FileAttemptStore::open(&path).unwrap();
            let Claim::Claimed(attempt) =
                store.try_claim("a-1", "ins-1", "h-1", "c-1").unwrap()
            else {
                panic!("a fresh identity must claim");
            };
            store
                .put(&Attempt {
                    phase: AttemptPhase::Submitting,
                    ..attempt.clone()
                })
                .unwrap();
            attempt
        };
        drop(claimed);

        // A genuinely new store over the same file — the restart, not an Rc alias.
        let reopened = FileAttemptStore::open(&path).unwrap();
        let recovered = reopened.get("a-1").expect("the attempt survived the reopen");
        assert_eq!(recovered.instruction_id, "ins-1");
        assert_eq!(
            recovered.phase,
            AttemptPhase::Submitting,
            "the latest phase for the attempt wins on replay"
        );
        assert!(!reopened.dropped_torn_tail());

        // And the recovered view still classifies: same id resumes, same identity duplicates.
        assert_eq!(
            reopened.try_claim("a-1", "ins-1", "h-1", "c-1").unwrap(),
            Claim::Existing(recovered)
        );
        assert_eq!(
            reopened.try_claim("a-2", "ins-1", "h-1", "c-2").unwrap(),
            Claim::Duplicate
        );
        assert_eq!(
            reopened.try_claim("a-3", "ins-1", "h-9", "c-3").unwrap(),
            Claim::ContractViolation
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_gate_row_survives_close_and_reopen() {
        let dir = scratch_dir();
        let path = dir.join("gate.jsonl");
        {
            let store = FileGateStore::open(&path).unwrap();
            let halted = store
                .init(&GateRow {
                    partition: "p-1".into(),
                    owner: "unbound".into(),
                    state: GateState::Halted,
                    epoch: 0,
                    fence_token: 0,
                })
                .unwrap();
            assert_eq!(halted.state, GateState::Halted);
            store
                .write(&GateRow {
                    state: GateState::Enabled,
                    epoch: 5,
                    fence_token: 7,
                    ..halted
                })
                .unwrap();
        } // the process "exits": the store is dropped, the lock released and the file closed

        let reopened = FileGateStore::open(&path).unwrap();
        let row = reopened.read("p-1").expect("the gate row survived the reopen");
        assert_eq!(row.state, GateState::Enabled);
        assert_eq!(row.epoch, 5);
        assert_eq!(row.fence_token, 7);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── D2: a deliberate halt creates the row, and never clobbers a bound one ───────────────────

    #[test]
    fn init_creates_a_missing_row_once_and_never_clobbers() {
        let dir = scratch_dir();
        let path = dir.join("gate.jsonl");
        let store = FileGateStore::open(&path).unwrap();
        assert_eq!(store.read("p-1"), None);

        let boot = GateRow {
            partition: "p-1".into(),
            owner: "unbound".into(),
            state: GateState::Halted,
            epoch: 0,
            fence_token: 0,
        };
        assert_eq!(store.init(&boot).unwrap(), boot);

        // A bound row is returned untouched: a restart must not reset the fence (P3-151).
        let bound = GateRow {
            owner: "worker-1".into(),
            epoch: 5,
            fence_token: 7,
            ..boot.clone()
        };
        store.write(&bound).unwrap();
        assert_eq!(store.init(&boot).unwrap(), bound);

        // Exactly two records were appended: the create and the explicit write. The second `init`
        // wrote nothing.
        let lines = std::fs::read_to_string(&path).unwrap().lines().count();
        assert_eq!(lines, 2, "init must not append when a row already exists");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Failure shapes ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn a_torn_final_line_is_dropped_but_a_corrupt_one_is_refused() {
        let dir = scratch_dir();
        let path = attempts_log(&dir);
        {
            let store = FileAttemptStore::open(&path).unwrap();
            store.try_claim("a-1", "ins-1", "h-1", "c-1").unwrap();
        }
        // A death inside the next append: a half-written line with no trailing newline.
        {
            use std::io::Write;
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(br#"{"attempt_id":"a-2","instruction"#)
                .unwrap();
        }
        let store = FileAttemptStore::open(&path).unwrap();
        assert!(store.dropped_torn_tail(), "the torn tail must be reported");
        assert_eq!(store.get("a-1").unwrap().phase, AttemptPhase::Prepared);
        assert_eq!(store.get("a-2"), None, "a torn append never claimed anything");
        drop(store);

        // A corrupt line that is *not* the tail means the file is not what this store wrote.
        {
            let mut content = std::fs::read_to_string(&path).unwrap();
            content.push_str("{\"attempt_id\": broken}\n");
            content.push_str("{\"attempt_id\":\"a-3\"}\n");
            std::fs::write(&path, content).unwrap();
        }
        let Err(err) = FileAttemptStore::open(&path) else {
            panic!("a corrupt log must not open");
        };
        let err = err.to_string();
        assert!(
            err.contains("is not a record this store wrote"),
            "unexpected error: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_second_actor_is_refused_and_a_stale_lock_is_taken_over() {
        let dir = scratch_dir();
        let path = attempts_log(&dir);
        let first = FileAttemptStore::open(&path).unwrap();
        let Err(err) = FileAttemptStore::open(&path) else {
            panic!("a second store on the same log must be refused");
        };
        let err = err.to_string();
        assert!(
            err.contains("one actor per durable log"),
            "a second store on the same log must be refused: {err}"
        );
        drop(first);

        // A lock left by a process that no longer exists must not block the service forever.
        std::fs::write(path.with_extension("lock"), "4000000\n").unwrap();
        let recovered = FileAttemptStore::open(&path).unwrap();
        assert_eq!(recovered.get("missing"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The first start with a fresh `DURABLE_DIR` has no directory yet, and the lock is written
    /// before the log: both have to create it (found live — every such start failed with
    /// "writing lock …: No such file or directory").
    #[test]
    fn a_store_creates_the_directory_it_lives_in() {
        let dir = scratch_dir();
        let nested = dir.join("not").join("there").join("yet");
        let store = FileAttemptStore::open(&nested.join("attempts.jsonl")).unwrap();
        store.try_claim("a-1", "ins-1", "h-1", "c-1").unwrap();
        let gate = FileGateStore::open(&nested.join("gate.jsonl")).unwrap();
        assert!(gate.read("p-1").is_none());
        assert!(nested.is_dir(), "the store created its own directory");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The live HTTP path shares one store across tokio worker threads, so this is the property the
    /// Workstream-D swap rests on. A compile-time assertion, not a runtime one.
    #[test]
    fn the_store_is_shareable_across_threads() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FileAttemptStore>();
        assert_send_sync::<FileGateStore>();
    }

    /// Two workers racing the same identity must produce one claim and one refusal. One mutex over
    /// log *and* indexes is what makes that true; per-index locks could interleave between the
    /// classification and the append.
    #[test]
    fn a_racing_second_claim_is_refused() {
        let dir = scratch_dir();
        let path = dir.join("attempts.jsonl");
        let store = std::sync::Arc::new(FileAttemptStore::open(&path).unwrap());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let claims: Vec<Claim> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..2)
                .map(|i| {
                    let store = std::sync::Arc::clone(&store);
                    let barrier = std::sync::Arc::clone(&barrier);
                    scope.spawn(move || {
                        // Both threads are inside the store at the same time; neither can be first
                        // by accident of scheduling.
                        barrier.wait();
                        store
                            .try_claim(&format!("a-{i}"), "ins-1", "h-1", &format!("c-{i}"))
                            .unwrap()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let claimed = claims.iter().filter(|c| matches!(c, Claim::Claimed(_))).count();
        let duplicates = claims.iter().filter(|c| matches!(c, Claim::Duplicate)).count();
        assert_eq!((claimed, duplicates), (1, 1), "claims: {claims:?}");

        // And the loser left nothing behind: exactly one attempt is in the log on disk.
        drop(store);
        let reopened = FileAttemptStore::open(&path).unwrap();
        let present = (0..2)
            .filter(|i| reopened.get(&format!("a-{i}")).is_some())
            .count();
        assert_eq!(present, 1, "only the winning claim is durable");
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
