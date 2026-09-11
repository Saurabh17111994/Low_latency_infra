package com.trading.common.schema.projection;

/** Row mirror of Postback_Quarantine (16_postback_quarantine.sql). */
public record QuarantinedPostback(
        String quarantineId,
        String postbackEventId,
        QuarantineReason reason,
        byte[] originalPayload,
        String payloadHash,
        String brokerOrderId,
        String instructionId,
        String correlationAttempt,
        String disposition,
        String dispositionReason,
        long quarantinedTs,
        Long dispositionTs,
        String schemaVersion) {
    // P3-411: records use reference equality for arrays — two rows with
    // identical bytes would compare unequal and print as [B@hash, and the
    // accessor would expose the mutable internal array. Copy in and out, and
    // compare by content.
    public QuarantinedPostback {
        originalPayload = originalPayload == null ? null : originalPayload.clone();
    }
    @Override public byte[] originalPayload() {
        return originalPayload == null ? null : originalPayload.clone();
    }
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QuarantinedPostback q)) return false;
        return java.util.Objects.equals(quarantineId, q.quarantineId)
                && java.util.Objects.equals(postbackEventId, q.postbackEventId)
                && reason == q.reason
                && java.util.Arrays.equals(originalPayload, q.originalPayload)
                && java.util.Objects.equals(payloadHash, q.payloadHash)
                && java.util.Objects.equals(brokerOrderId, q.brokerOrderId)
                && java.util.Objects.equals(instructionId, q.instructionId)
                && java.util.Objects.equals(correlationAttempt, q.correlationAttempt)
                && java.util.Objects.equals(disposition, q.disposition)
                && java.util.Objects.equals(dispositionReason, q.dispositionReason)
                && quarantinedTs == q.quarantinedTs
                && java.util.Objects.equals(dispositionTs, q.dispositionTs)
                && java.util.Objects.equals(schemaVersion, q.schemaVersion);
    }
    @Override public int hashCode() {
        int h = java.util.Objects.hash(quarantineId, postbackEventId, reason, payloadHash,
                brokerOrderId, instructionId, correlationAttempt, disposition,
                dispositionReason, quarantinedTs, dispositionTs, schemaVersion);
        return 31 * h + java.util.Arrays.hashCode(originalPayload);
    }
    @Override public String toString() {
        return "QuarantinedPostback[quarantineId=" + quarantineId
                + ", postbackEventId=" + postbackEventId + ", reason=" + reason
                + ", originalPayload=" + java.util.Arrays.toString(originalPayload)
                + ", payloadHash=" + payloadHash + ", brokerOrderId=" + brokerOrderId
                + ", instructionId=" + instructionId + ", correlationAttempt=" + correlationAttempt
                + ", disposition=" + disposition + ", dispositionReason=" + dispositionReason
                + ", quarantinedTs=" + quarantinedTs + ", dispositionTs=" + dispositionTs
                + ", schemaVersion=" + schemaVersion + "]";
    }
}
