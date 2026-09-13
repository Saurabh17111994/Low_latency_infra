//! Durable write path — four permanent clients and their (not-yet-wired) enablement flags
//! (plan Task B7 / CHG-065).
//!
//! The Nautilus service currently keeps its duplicate-send guard in-process
//! (`InMemoryAttemptStore` / `InMemoryGateStateStore`). Four durable clients are
//! designed but were flag-gated unbuilt: without them an in-process crash can lose
//! the "did I send this?" guard even though Fluss-backed gateway stores exist on the
//! Java side. This module closes that gap in the offline slice:
//!
//! 1. **Gate store** — durable `GateState` (HALTED/ENABLED + epoch/fence), reuses
//!    `executiongate::{GateStateStore, InMemoryGateStateStore}`.
//! 2. **Attempt store** — durable `Attempt` (PREPARED→terminal), reuses
//!    `executiongate::{AttemptStore, InMemoryAttemptStore}`.
//!    **This is the store that must claim atomically (P3-201):** the `get` →
//!    `has_duplicate` → `put` sequence in `ExecutionGate::execute` is only safe for a single
//!    actor, so before two executors can share it the durable implementation needs an atomic
//!    classify+claim entry point and the trait needs the matching method. The required shape
//!    and the key-design consequence are stated on the trait's contract.
//! 3. **Local journal** — append-only event journal (file-backed in production, memory
//!    in the offline slice), used for engine history/replay.
//! 4. **Audit sink** — durable audit/OTel feed (Fluss `Execution_Audit` LOG in
//!    production, memory in the offline slice).
//!
//! Each client is behind a dedicated env flag defaulting to OFF.
//!
//! **Wiring status (P3-192, verified by inspection):** the flags are recorded on
//! [`DurableClients`] and read by nothing — `new_in_memory` builds all four in-memory stores
//! unconditionally, so an all-ON client is behavior-identical to an all-OFF client *by
//! construction*, not merely because a branch happens to agree. No `flags.*` branch exists yet
//! and [`DurableFlags::any_on`] has no caller: the flags are the seam for the swap below, never
//! evidence that a durable path is active. Pin the inertness with the `*_flag_off_*` tests.
//!
//! When the swap lands, the live slice (Workstream D) plugs Fluss-backed / file / R2 / OTel
//! implementations in behind the same traits — identical to the `clockwatch` swap pattern.
//! Enabling the flags in compose requires explicit user approval (B7.5).

use std::cell::RefCell;
use std::rc::Rc;

use crate::executiongate::{
    AttemptStore, GateStateStore, InMemoryAttemptStore, InMemoryGateStateStore,
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
    /// True when any flag is on. No caller yet (P3-192) — kept as the B7 wiring seam.
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

/// The four durable clients. Handles onto one of these are `Rc`-shared, so two
/// `ExecutionGate` instances built from the same bundle see the same store — an aliasing
/// property, deliberately *not* a modelled process restart (P3-438; no durable store impl
/// exists yet, see the module header).
pub struct DurableClients {
    /// Parsed enablement flags. Stored for the B7 swap; **no code branches on them yet**
    /// (P3-192) — see the module header. Reading them is not evidence of a durable path.
    pub flags: DurableFlags,
    pub gate_store: Rc<dyn GateStateStore>,
    pub attempt_store: Rc<dyn AttemptStore>,
    pub journal: Rc<dyn JournalStore>,
    pub audit: Rc<dyn AuditSink>,
    // Concrete in-memory handles, kept for test introspection only (P3-437): `#[cfg(test)]`
    // keeps production code from reaching in and sharing these stores across handles — the
    // trait-object fields above are the production surface.
    #[cfg(test)]
    gate_mem: Rc<InMemoryGateStateStore>,
    #[cfg(test)]
    attempt_mem: Rc<InMemoryAttemptStore>,
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
        let attempt_mem = Rc::new(InMemoryAttemptStore::new());
        let journal_mem = Rc::new(InMemoryJournalStore::new());
        let audit_mem = Rc::new(InMemoryAuditSink::new());
        Self {
            flags,
            gate_store: gate_mem.clone() as Rc<dyn GateStateStore>,
            attempt_store: attempt_mem.clone() as Rc<dyn AttemptStore>,
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

    // Introspection for tests: read back what a second handle onto the same store sees.
    #[cfg(test)]
    pub fn gate_mem(&self) -> &Rc<InMemoryGateStateStore> {
        &self.gate_mem
    }
    #[cfg(test)]
    pub fn attempt_mem(&self) -> &Rc<InMemoryAttemptStore> {
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
    use crate::executiongate::{Attempt, AttemptPhase, GateRow, GateState};

    // GAP (P3-438): no durable (file- or Fluss-backed) store implementation exists in this
    // crate, so these tests cannot model a process restart. They write through the trait object
    // and read through a SECOND handle onto the same Rc, which proves handle-sharing — not
    // recovery after a crash. Real restart recovery is unverified until the B7 swap lands; the
    // names below say what they actually cover.
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
        let restarted: Rc<dyn AttemptStore> = clients.attempt_mem.clone() as Rc<dyn AttemptStore>;
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
}
