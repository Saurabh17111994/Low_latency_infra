package com.trading.common.workitem;

/**
 * A tracked work item carrying its lifecycle state and, when blocked, the
 * owner, missing evidence, and unblock condition required by the spec.
 */
public record WorkItem(
        String id,
        WorkItemState state,
        WorkItemState blockedFrom,
        String owner,
        String missingEvidence,
        String unblockCondition) {

    /** Creates a new work item in the OPEN state. */
    public static WorkItem create(String id, String owner) {
        return new WorkItem(id, WorkItemState.OPEN, null, owner, null, null);
    }

    /**
     * Returns a copy in {@code next} state, enforcing
     * {@link WorkItemLifecycle#canTransition}. When entering BLOCKED the
     * prior state is recorded so it can be resumed.
     *
     * <p>R-269: the spec requires a blocked item to record the owner, missing
     * evidence, and unblock condition — entering BLOCKED without all three is
     * rejected here instead of silently recorded.
     */
    public WorkItem transitionTo(WorkItemState next,
                                 String owner,
                                 String missingEvidence,
                                 String unblockCondition) {
        if (!WorkItemLifecycle.canTransition(this.state, next)) {
            throw new IllegalStateException(
                    "Illegal transition " + this.state + " -> " + next);
        }
        if (next == WorkItemState.BLOCKED) {
            if (isBlank(owner)) {
                throw new IllegalStateException(
                        "BLOCKED requires an owner; got null/blank");
            }
            if (isBlank(missingEvidence)) {
                throw new IllegalStateException(
                        "BLOCKED requires missingEvidence; got null/blank");
            }
            if (isBlank(unblockCondition)) {
                throw new IllegalStateException(
                        "BLOCKED requires unblockCondition; got null/blank");
            }
        }
        // blockedFrom is only meaningful for the transition INTO BLOCKED: re-blocking an
        // already-BLOCKED item (canTransition allows it) must not record BLOCKED as its own
        // predecessor.
        WorkItemState blockedFrom =
                (next == WorkItemState.BLOCKED && this.state != WorkItemState.BLOCKED)
                        ? this.state : null;
        // Blocking metadata is meaningful only while BLOCKED. Clearing it on every other
        // target state keeps the new item consistent with its state instead of relying on
        // the caller to pass nulls when unblocking.
        String effectiveMissingEvidence =
                (next == WorkItemState.BLOCKED) ? missingEvidence : null;
        String effectiveUnblockCondition =
                (next == WorkItemState.BLOCKED) ? unblockCondition : null;
        return new WorkItem(
                this.id, next, blockedFrom, owner, effectiveMissingEvidence,
                effectiveUnblockCondition);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
