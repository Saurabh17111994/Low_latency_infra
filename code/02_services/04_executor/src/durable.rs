//! Durable write path — four permanent clients and their enablement flags
//! (plan Task B7 / CHG-065).
//!
//! The Nautilus service keeps its duplicate-send guard in-process
//! (`InMemoryAttemptStore` / `InMemoryGateStateStore`). Four durable clients are designed;
//! without them an in-process crash can lose the "did I send this?" guard even though
//! Fluss-backed gateway stores exist on the Java side. This module is where they are built:
//!
//! 1. **Gate store** — durable `GateState` (HALTED/ENABLED + epoch/fence), reuses
//!    `executiongate::{GateStateStore, InMemoryGateStateStore}`.
//! 2. **Attempt store** — durable `Attempt` (PREPARED→terminal), reuses
//!    `executiongate::{AttemptStore, InMemoryAttemptStore}`.
//!    **This is the store that must claim atomically (P3-201):** the old `get` →
//!    `has_duplicate` → `put` sequence in `ExecutionGate::execute` was safe only for a single
//!    actor. The entry point landed with the store (D1): [`AttemptStore::try_claim`], implemented
//!    by both the in-memory and the file-backed store, with the key layout stated on the trait.
//! 3. **Local journal** — append-only event journal (file-backed in production, memory
//!    in the offline slice), used for engine history/replay.
//! 4. **Audit sink** — durable audit/OTel feed (Fluss `Execution_Audit` LOG in
//!    production, memory in the offline slice).
//!
//! Each client is behind a dedicated env flag defaulting to OFF.
//!
//! **Wiring status (D1/D2 + the Workstream-D swap):** the flags are read by [`open_for_service`],
//! which a service start calls — ON selects the file-backed store under the configured directory,
//! OFF keeps the in-memory one, and a flag whose client has no durable implementation is refused
//! rather than quietly substituted. The gate store is written at boot (the D2 row); the attempt
//! store now has its live caller: the `/v1/intents` forward leg claims and records the attempt
//! through [`LiveAttemptStore`] before the bridge sees the order, and answers a later attempt from
//! the record instead of sending again (`crate::http::claim_for_send`). With the attempts flag OFF
//! that guard is absent — no *in-memory* substitute, which would be a guard the server could not
//! share across its workers. [`DurableClients::new_in_memory`] remains the in-memory bundle the
//! flag-off tests pin.
//!
//! The file-backed stores live in [`crate::durable_file`]; the remaining swap plugs Fluss-backed /
//! R2 / OTel implementations in behind the same traits — identical to the `clockwatch` swap
//! pattern. Enabling the flags in compose requires explicit user approval (B7.5).

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{bail, Result};

use crate::durable_file::{FileAttemptStore, FileGateStore, ATTEMPTS_LOG, GATE_LOG};
use crate::executiongate::{
    AttemptStore, GateRow, GateState, GateStateStore, InMemoryAttemptStore, InMemoryGateStateStore,
    BOOT_HALTED_OWNER,
};

/// Which durable clients are enabled (all OFF by default).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableFlags {
    pub gate: bool,
    pub attempts: bool,
    pub journal: bool,
    pub audit: bool,
}

impl DurableFlags {
    pub fn all_off() -> Self {
        Self {
            gate: false,
            attempts: false,
            journal: false,
            audit: false,
        }
    }
    pub fn all_on() -> Self {
        Self {
            gate: true,
            attempts: true,
            journal: true,
            audit: true,
        }
    }
    /// True when any flag is on — the start-up guard for opening the bundle at all.
    pub fn any_on(&self) -> bool {
        self.gate || self.attempts || self.journal || self.audit
    }
}

// ── Local journal (append-only) ─────────────────────────────────────────────

/// One journal entry — opaque bytes with an ordering key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    pub seq: u64,
    pub payload: Vec<u8>,
}

pub trait JournalStore {
    fn append(&self, payload: &[u8]) -> u64;
    /// Point-in-time snapshot of every entry (P3-436).
    ///
    /// This is an O(n) copy — the in-memory implementation clones the whole journal — meant for
    /// verification and inspection (tests, tooling). Do not call it per append: a loop that
    /// writes and inspects each time copies the journal once per entry.
    fn entries(&self) -> Vec<JournalEntry>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Default)]
pub struct InMemoryJournalStore {
    inner: RefCell<Vec<JournalEntry>>,
}

impl InMemoryJournalStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl JournalStore for InMemoryJournalStore {
    fn append(&self, payload: &[u8]) -> u64 {
        let mut v = self.inner.borrow_mut();
        let seq = v.len() as u64 + 1;
        v.push(JournalEntry {
            seq,
            payload: payload.to_vec(),
        });
        seq
    }
    fn entries(&self) -> Vec<JournalEntry> {
        self.inner.borrow().clone()
    }
    fn len(&self) -> usize {
        self.inner.borrow().len()
    }
}

// ── Audit sink (append-only, never queried for correctness) ─────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    pub seq: u64,
    pub kind: String,
    pub payload: Vec<u8>,
}

pub trait AuditSink {
    fn record(&self, kind: &str, payload: &[u8]) -> u64;
    /// Point-in-time snapshot of every record — an O(n) clone with the same contract as
    /// [`JournalStore::entries`] (P3-436): inspection only, never per append. The audit feed is
    /// never read back for correctness decisions.
    fn records(&self) -> Vec<AuditRecord>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Default)]
pub struct InMemoryAuditSink {
    inner: RefCell<Vec<AuditRecord>>,
}

impl InMemoryAuditSink {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AuditSink for InMemoryAuditSink {
    fn record(&self, kind: &str, payload: &[u8]) -> u64 {
        let mut v = self.inner.borrow_mut();
        let seq = v.len() as u64 + 1;
        v.push(AuditRecord {
            seq,
            kind: kind.to_string(),
            payload: payload.to_vec(),
        });
        seq
    }
    fn records(&self) -> Vec<AuditRecord> {
        self.inner.borrow().clone()
    }
    fn len(&self) -> usize {
        self.inner.borrow().len()
    }
}

// ── Bundle: the four clients as a unit ──────────────────────────────────────

/// The attempt store as the live request path needs it.
///
/// [`AttemptStore`] is deliberately thread-agnostic — the in-memory store is `Rc`-based, which is
/// all a single-actor boot or a test needs. The live forward leg runs on the HTTP server, so it asks
/// for this narrower view instead: a store safe to hand to `ServerState` and use from any tokio
/// worker. Only a store whose mutations are serialised internally qualifies; `FileAttemptStore`
/// holds one mutex over its log and indexes.
pub trait LiveAttemptStore: AttemptStore + Send + Sync {}

impl<T: AttemptStore + Send + Sync + ?Sized> LiveAttemptStore for T {}

/// The four durable clients. The in-memory handles are `Rc`-shared, so two `ExecutionGate`
/// instances built from the same bundle see the same store — an aliasing property, deliberately
/// *not* a modelled process restart. A modelled restart is [`DurableClients::open_for_service`]
/// over the same directory (P3-438), which the file-backed stores make real.
pub struct DurableClients {
    /// Parsed enablement flags, as chosen by [`DurableClients::open_for_service`] or the in-memory
    /// constructor. An ON flag means the matching store below is the file-backed one. For the
    /// attempt store that also means [`DurableClients::live_attempts`] is `Some`, so the flag is
    /// evidence of a live writer: the forward leg is the caller (see the module header).
    pub flags: DurableFlags,
    pub gate_store: Rc<dyn GateStateStore>,
    pub attempt_store: Arc<dyn AttemptStore>,
    /// The same attempt store in the shape the live forward leg needs, when the attempts flag is ON.
    /// `None` is not a degraded store — it is the flag-OFF behaviour, where the live path has no
    /// durable guard at all (the gateway's own `Execution_Intent_Processed` dedup is the guard).
    pub live_attempts: Option<Arc<dyn LiveAttemptStore>>,
    pub journal: Rc<dyn JournalStore>,
    pub audit: Rc<dyn AuditSink>,
    // Concrete in-memory handles, kept for test introspection only (P3-437): `#[cfg(test)]`
    // keeps production code from reaching in and sharing these stores across handles — the
    // trait-object fields above are the production surface.
    #[cfg(test)]
    gate_mem: Rc<InMemoryGateStateStore>,
    #[cfg(test)]
    attempt_mem: Arc<dyn AttemptStore>,
    #[cfg(test)]
    journal_mem: Rc<InMemoryJournalStore>,
    #[cfg(test)]
    audit_mem: Rc<InMemoryAuditSink>,
}

impl DurableClients {
    /// Builds the four in-memory clients. `flags` is stored verbatim and branched on by
    /// nothing (P3-192): every flag combination yields the same in-memory stores.
    pub fn new_in_memory(flags: DurableFlags) -> Self {
        let gate_mem = Rc::new(InMemoryGateStateStore::new());
        // An unsized trait object on purpose: the in-memory store is single-actor (its contract says
        // so), and `Arc<dyn AttemptStore>` cannot cross a thread boundary, whereas `Arc<the concrete
        // store>` would claim a thread-safety this store does not have.
        let attempt_mem: Arc<dyn AttemptStore> =
            Arc::from(Box::new(InMemoryAttemptStore::new()) as Box<dyn AttemptStore>);
        let journal_mem = Rc::new(InMemoryJournalStore::new());
        let audit_mem = Rc::new(InMemoryAuditSink::new());
        Self {
            flags,
            gate_store: gate_mem.clone() as Rc<dyn GateStateStore>,
            attempt_store: attempt_mem.clone(),
            // An in-memory store is not `Send + Sync`, so it can never be the live path's guard:
            // flag OFF means no durable guard, which is what the flag documents.
            live_attempts: None,
            journal: journal_mem.clone() as Rc<dyn JournalStore>,
            audit: audit_mem.clone() as Rc<dyn AuditSink>,
            #[cfg(test)]
            gate_mem,
            #[cfg(test)]
            attempt_mem,
            #[cfg(test)]
            journal_mem,
            #[cfg(test)]
            audit_mem,
        }
    }

    /// Convenience: all OFF (today's behavior).
    pub fn offline() -> Self {
        Self::new_in_memory(DurableFlags::all_off())
    }

    /// Builds the durable clients for a real service start (D1/D2), and creates the partition's gate
    /// row when the gate client is durable.
    ///
    /// A flag that is ON selects its file-backed store under `dir`, opened (and the directory
    /// created) here — so a start that cannot reach its durable store fails instead of running
    /// without it. A flag that is OFF keeps the in-memory store, which is the default and today's
    /// behaviour. `partition` names the partition this executor owns and is required exactly when
    /// the gate client is durable: without it there is no row to create and no partition to own.
    ///
    /// The journal and audit clients have no durable implementation yet, so enabling either is an
    /// error rather than a quiet in-memory stand-in (the fail-closed reading of "an operator's
    /// failed attempt to enable a durable flag must not pass unnoticed", P3-434).
    pub fn open_for_service(
        dir: &Path,
        flags: DurableFlags,
        partition: Option<&str>,
    ) -> Result<Self> {
        if flags.journal {
            bail!(
                "DURABLE_JOURNAL_ENABLED is set, but the local journal has no durable client yet \
                 (D1/D2 cover the attempt and gate stores)"
            );
        }
        if flags.audit {
            bail!(
                "DURABLE_AUDIT_ENABLED is set, but the audit sink has no durable client yet \
                 (D1/D2 cover the attempt and gate stores)"
            );
        }
        // One store object, two trait-object views: the bundle's `attempt_store` and the live
        // handle the forward leg uses must be the same log, or the second `open` would be refused by
        // the one-actor-per-log lock (which is the point of that lock).
        let (attempt_store, live_attempts): (Arc<dyn AttemptStore>, _) = if flags.attempts {
            let file = Arc::new(FileAttemptStore::open(&dir.join(ATTEMPTS_LOG))?);
            (file.clone(), Some(file as Arc<dyn LiveAttemptStore>))
        } else {
            (
                Arc::from(Box::new(InMemoryAttemptStore::new()) as Box<dyn AttemptStore>),
                None,
            )
        };
        let gate_store: Rc<dyn GateStateStore> = if flags.gate {
            Rc::new(FileGateStore::open(&dir.join(GATE_LOG))?)
        } else {
            Rc::new(InMemoryGateStateStore::new())
        };

        // D2: the row must exist for a durable gate to be about anything. It is created HALTED and
        // unbound — this service's boot state — and `init` leaves an existing row alone, so a
        // restart cannot clear a fenced gate (P3-151).
        if flags.gate {
            let partition = partition.map(str::trim).filter(|p| !p.is_empty()).ok_or_else(|| {
                anyhow::anyhow!(
                    "DURABLE_GATE_ENABLED is set, so EXECUTION_PARTITION_ID must name the partition \
                     whose gate row this executor owns"
                )
            })?;
            gate_store.init(&GateRow {
                partition: partition.to_string(),
                owner: BOOT_HALTED_OWNER.to_string(),
                state: GateState::Halted,
                epoch: 0,
                fence_token: 0,
            })?;
        }

        Ok(Self {
            flags,
            gate_store,
            attempt_store,
            live_attempts,
            journal: Rc::new(InMemoryJournalStore::new()),
            audit: Rc::new(InMemoryAuditSink::new()),
            // Test-only handles. They exist so the in-memory bundle can be introspected; a bundle
            // built from flags hands out no in-memory alias of a durable store, and a test that
            // wants to prove durability reopens the directory instead.
            #[cfg(test)]
            gate_mem: Rc::new(InMemoryGateStateStore::new()),
            #[cfg(test)]
            attempt_mem: Arc::from(Box::new(InMemoryAttemptStore::new()) as Box<dyn AttemptStore>),
            #[cfg(test)]
            journal_mem: Rc::new(InMemoryJournalStore::new()),
            #[cfg(test)]
            audit_mem: Rc::new(InMemoryAuditSink::new()),
        })
    }

    // Introspection for tests: read back what a second handle onto the same store sees.
    #[cfg(test)]
    pub fn gate_mem(&self) -> &Rc<InMemoryGateStateStore> {
        &self.gate_mem
    }
    #[cfg(test)]
    pub fn attempt_mem(&self) -> &Arc<dyn AttemptStore> {
        &self.attempt_mem
    }
    #[cfg(test)]
    pub fn journal_mem(&self) -> &Rc<InMemoryJournalStore> {
        &self.journal_mem
    }
    #[cfg(test)]
    pub fn audit_mem(&self) -> &Rc<InMemoryAuditSink> {
        &self.audit_mem
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executiongate::{Attempt, AttemptPhase, Claim, GateRow, GateState};

    // P3-438: the `new_in_memory` tests below write through the trait object and read through a
    // SECOND handle onto the same Rc. That proves handle-sharing, not recovery after a crash — the
    // names say what they cover. Real restart recovery is covered by the `file_*` tests, which close
    // the store and reopen its directory, and by `durable_file`'s own reopen tests. What is still
    // absent is the Fluss-backed client (the Java side has one) and the live-slice caller.
    // ── Flag defaults ───────────────────────────────────────────────────────

    #[test]
    fn flags_default_all_off() {
        assert_eq!(
            DurableFlags::all_off(),
            DurableFlags {
                gate: false,
                attempts: false,
                journal: false,
                audit: false
            }
        );
        assert!(!DurableFlags::all_off().any_on());
        assert!(DurableFlags::all_on().any_on());
    }

    // ── Gate store: write through the trait object, read back through a second handle

    #[test]
    fn gate_state_is_shared_across_handles() {
        let clients = DurableClients::new_in_memory(DurableFlags::all_on());
        // Write via first handle.
        clients
            .gate_store
            .write(&GateRow {
                partition: "p".into(),
                owner: "w1".into(),
                state: GateState::Enabled,
                epoch: 5,
                fence_token: 7,
            })
            .unwrap();
        // A second handle onto the SAME memory (an Rc alias — not a process restart).
        let restarted_gate: Rc<dyn GateStateStore> =
            clients.gate_mem.clone() as Rc<dyn GateStateStore>;
        let row = restarted_gate.read("p").unwrap();
        assert_eq!(row.epoch, 5);
        assert_eq!(row.state, GateState::Enabled);
    }

    #[test]
    fn gate_flag_off_behavior_identical() {
        // OFF still uses the same in-memory store — no behavioral difference,
        // just the flag that controls whether production would swap in Fluss.
        let off = DurableClients::new_in_memory(DurableFlags::all_off());
        let on = DurableClients::new_in_memory(DurableFlags::all_on());
        // Both accept the same write/read cycle.
        for c in [&off, &on] {
            c.gate_store
                .write(&GateRow {
                    partition: "p".into(),
                    owner: "w1".into(),
                    state: GateState::Halted,
                    epoch: 1,
                    fence_token: 1,
                })
                .unwrap();
            assert_eq!(c.gate_store.read("p").unwrap().state, GateState::Halted);
        }
    }

    // ── Attempt store: write through the trait object, read back through a second handle

    #[test]
    fn attempt_state_is_shared_across_handles() {
        let clients = DurableClients::new_in_memory(DurableFlags::all_on());
        let a = Attempt::new("a-1", "ins-1", "h-1", "E-a-1", AttemptPhase::Prepared);
        clients.attempt_store.put(&a).unwrap();
        // Second handle reading the same attempt.
        let restarted: Arc<dyn AttemptStore> = clients.attempt_mem.clone();
        let got = restarted.get("a-1").unwrap();
        assert_eq!(got.phase, AttemptPhase::Prepared);
        assert!(restarted.has_duplicate("ins-1", "h-1"));
    }

    // ── Journal: append through the trait object, read back through a second handle

    #[test]
    fn journal_state_is_shared_across_handles() {
        let clients = DurableClients::new_in_memory(DurableFlags::all_on());
        clients.journal.append(b"event-1");
        clients.journal.append(b"event-2");
        let restarted: Rc<dyn JournalStore> = clients.journal_mem.clone() as Rc<dyn JournalStore>;
        let entries = restarted.entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].payload, b"event-1");
        assert_eq!(entries[1].seq, 2);
    }

    #[test]
    fn journal_flag_off_no_regression() {
        let clients = DurableClients::offline();
        assert_eq!(clients.journal.len(), 0);
        clients.journal.append(b"x");
        assert_eq!(clients.journal.len(), 1);
    }

    // ── Audit sink: record through the trait object, read back through a second handle

    #[test]
    fn audit_state_is_shared_across_handles() {
        let clients = DurableClients::new_in_memory(DurableFlags::all_on());
        clients.audit.record("order_accepted", b"{\"id\":\"a-1\"}");
        let restarted: Rc<dyn AuditSink> = clients.audit_mem.clone() as Rc<dyn AuditSink>;
        let recs = restarted.records();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].kind, "order_accepted");
    }

    #[test]
    fn audit_flag_off_no_regression() {
        let clients = DurableClients::offline();
        clients.audit.record("x", b"y");
        assert_eq!(clients.audit.len(), 1);
    }

    // ── Flag-driven construction: a real service start puts the flag in charge ──────────────────

    fn scratch_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static N: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "nautilus-durable-clients-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn only_the_attempts_flag_gives_the_live_path_a_store() {
        let dir = scratch_dir();
        // Flag OFF: no durable guard in the live path, which is today's behaviour and not a
        // degraded store. Asserting the absence keeps a future change from quietly substituting an
        // in-memory guard the server cannot share across its workers.
        let off = DurableClients::open_for_service(&dir, DurableFlags::all_off(), None).unwrap();
        assert!(off.live_attempts.is_none());
        drop(off);
        let on = DurableClients::open_for_service(
            &dir,
            DurableFlags {
                attempts: true,
                ..DurableFlags::all_off()
            },
            None,
        )
        .unwrap();
        assert!(on.live_attempts.is_some());
        drop(on);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_attempts_flag_selects_a_store_that_survives_a_process_restart() {
        let dir = scratch_dir();
        let flags = DurableFlags {
            attempts: true,
            ..DurableFlags::all_off()
        };
        {
            let clients = DurableClients::open_for_service(&dir, flags, None).unwrap();
            let claim = clients
                .attempt_store
                .try_claim("a-1", "ins-1", "h-1", "E-a-1")
                .unwrap();
            assert!(matches!(claim, Claim::Claimed(_)));
        } // the process "exits": the store is dropped and the lock released

        // A new start over the same directory — what a container restart does.
        let restarted = DurableClients::open_for_service(&dir, flags, None).unwrap();
        // The live forward leg's handle is the same log as the bundle's store — not a second store
        // (a second `open` of one log is refused by the writer lock, so this is the only shape that
        // can work).
        let live = restarted
            .live_attempts
            .as_ref()
            .expect("the attempts flag ON gives the live path a store");
        assert!(live.get("a-1").is_some(), "same log, two views");
        let recovered = restarted
            .attempt_store
            .get("a-1")
            .expect("the attempt outlived the process that wrote it");
        assert_eq!(recovered.instruction_id, "ins-1");
        assert_eq!(recovered.phase, AttemptPhase::Prepared);
        // And the recovered store still refuses to mint a second attempt for that order.
        assert_eq!(
            restarted
                .attempt_store
                .try_claim("a-2", "ins-1", "h-1", "E-a-2")
                .unwrap(),
            Claim::Duplicate
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_gate_flag_creates_the_boot_row_and_a_restart_keeps_the_bound_one() {
        let dir = scratch_dir();
        let flags = DurableFlags {
            gate: true,
            ..DurableFlags::all_off()
        };
        {
            let clients = DurableClients::open_for_service(&dir, flags, Some("p-1")).unwrap();
            let row = clients.gate_store.read("p-1").expect("D2: the row exists");
            assert_eq!(row.state, GateState::Halted);
            assert_eq!(row.owner, "unbound");
            assert_eq!((row.epoch, row.fence_token), (0, 0));
            // An operator approval binds the row.
            clients
                .gate_store
                .write(&GateRow {
                    owner: "worker-1".into(),
                    state: GateState::Enabled,
                    epoch: 5,
                    fence_token: 7,
                    ..row
                })
                .unwrap();
        }

        // The restart must not reset a fenced gate back to the boot row.
        let restarted = DurableClients::open_for_service(&dir, flags, Some("p-1")).unwrap();
        let row = restarted.gate_store.read("p-1").unwrap();
        assert_eq!(row.state, GateState::Enabled);
        assert_eq!(row.owner, "worker-1");
        assert_eq!((row.epoch, row.fence_token), (5, 7));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_durable_gate_without_a_partition_is_refused() {
        let dir = scratch_dir();
        let flags = DurableFlags {
            gate: true,
            ..DurableFlags::all_off()
        };
        let Err(err) = DurableClients::open_for_service(&dir, flags, None) else {
            panic!("a durable gate with no partition must not start");
        };
        assert!(
            err.to_string().contains("EXECUTION_PARTITION_ID"),
            "unexpected error: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn flags_whose_client_is_not_durable_yet_are_refused() {
        let dir = scratch_dir();
        for (flag, name) in [
            (
                DurableFlags {
                    journal: true,
                    ..DurableFlags::all_off()
                },
                "DURABLE_JOURNAL_ENABLED",
            ),
            (
                DurableFlags {
                    audit: true,
                    ..DurableFlags::all_off()
                },
                "DURABLE_AUDIT_ENABLED",
            ),
        ] {
            let Err(err) = DurableClients::open_for_service(&dir, flag, None) else {
                panic!("{name} must be refused while its client is in-memory only");
            };
            assert!(err.to_string().contains(name), "unexpected error: {err}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn all_off_touches_no_files() {
        let dir = scratch_dir();
        let clients = DurableClients::open_for_service(&dir, DurableFlags::all_off(), None).unwrap();
        clients.gate_store.write(&GateRow {
            partition: "p".into(),
            owner: "w1".into(),
            state: GateState::Halted,
            epoch: 1,
            fence_token: 1,
        })
        .unwrap();
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "with every flag OFF nothing may be created on disk"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
