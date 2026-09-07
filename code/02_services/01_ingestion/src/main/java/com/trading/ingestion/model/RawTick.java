package com.trading.ingestion.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable raw tick: original wire bytes + calculated metadata.
 * Stored before fingerprinting and validity classification.
 *
 * <p>R-252: {@link Builder#build()} validates required inputs — a null
 * {@code rawPayload} previously NPE'd at {@code builder.rawPayload.clone()}
 * and a null {@code payloadHash} NPE'd in {@link #toString()}; the builder
 * also defaulted {@code receiveTime} to {@link Instant#EPOCH}, silently
 * fabricating a 1970 receive timestamp for a tick that was never stamped.
 */
public final class RawTick {
    private final byte[] rawPayload;
    private final String payloadHash;         // SHA-256 of rawPayload
    private final String hashAlgorithm;       // "SHA-256"
    private final String protocolVersion;     // broker protocol version
    private final String decoderVersion;      // decoder implementation version
    private final Instant receiveTime;        // local receipt UTC
    private final long receiveTimeNanos;      // monotonic receive instant

    private RawTick(Builder builder) {
        this.rawPayload = builder.rawPayload.clone();
        this.payloadHash = builder.payloadHash;
        this.hashAlgorithm = builder.hashAlgorithm;
        this.protocolVersion = builder.protocolVersion;
        this.decoderVersion = builder.decoderVersion;
        this.receiveTime = builder.receiveTime;
        this.receiveTimeNanos = builder.receiveTimeNanos;
    }

    public byte[] rawPayload() { return rawPayload.clone(); }

    /**
     * P1-087: byte length WITHOUT copying and WITHOUT exposing the mutable
     * array (for hot-path size estimates). The former
     * {@code rawPayloadUnsafe()} is deleted: package-private visibility could
     * not hold (the three cross-package hot-path callers need access), so the
     * bug class is removed instead — retention points take a defensive copy
     * via {@link #rawPayload()}, length probes use this (Batch-4 #34 accepts
     * the copy cost: ~196 B/tick vs SHA-256 + append per tick).
     */
    public int rawPayloadLength() { return rawPayload.length; }

    public String payloadHash() { return payloadHash; }
    public String hashAlgorithm() { return hashAlgorithm; }
    public String protocolVersion() { return protocolVersion; }
    public String decoderVersion() { return decoderVersion; }
    public Instant receiveTime() { return receiveTime; }
    public long receiveTimeNanos() { return receiveTimeNanos; }

    @Override
    public String toString() {
        return "RawTick{hash=" + payloadHash.substring(0, Math.min(12, payloadHash.length()))
                + ", bytes=" + rawPayload.length + ", proto=" + protocolVersion + "}";
    }

    public static class Builder {
        private byte[] rawPayload;
        private String payloadHash;
        private String hashAlgorithm = "SHA-256";
        private String protocolVersion = "";
        private String decoderVersion = "0.1.0";
        private Instant receiveTime;
        private long receiveTimeNanos;

        // P1-088: defensive copy on input — the caller may mutate or reuse
        // its buffer between rawPayload(v) and build() (TOCTOU against the
        // separately supplied payloadHash). The ctor keeps its clone (belt
        // and braces on the immutable contract; Batch-4 #34 accepts the cost).
        public Builder rawPayload(byte[] v) { this.rawPayload = (v == null ? null : v.clone()); return this; }
        public Builder payloadHash(String v) { this.payloadHash = v; return this; }
        public Builder hashAlgorithm(String v) { this.hashAlgorithm = v; return this; }
        public Builder protocolVersion(String v) { this.protocolVersion = v; return this; }
        public Builder decoderVersion(String v) { this.decoderVersion = v; return this; }
        public Builder receiveTime(Instant v) { this.receiveTime = v; return this; }
        public Builder receiveTimeNanos(long v) { this.receiveTimeNanos = v; return this; }

        /**
         * R-252: fail fast with a descriptive message instead of a deferred
         * NPE inside the constructor, and require an explicit receive time —
         * a tick with {@link Instant#EPOCH} receive time is evidence that the
         * timestamp was never recorded.
         */
        public RawTick build() {
            Objects.requireNonNull(rawPayload, "rawPayload is required");
            if (rawPayload.length == 0) {
                throw new IllegalArgumentException("rawPayload must not be empty");
            }
            Objects.requireNonNull(payloadHash, "payloadHash is required");
            Objects.requireNonNull(hashAlgorithm, "hashAlgorithm is required");
            Objects.requireNonNull(protocolVersion, "protocolVersion is required");
            Objects.requireNonNull(decoderVersion, "decoderVersion is required");
            Objects.requireNonNull(receiveTime, "receiveTime is required — a RawTick must be stamped");
            // P1-250: System.nanoTime() has an arbitrary origin and per
            // Javadoc may be negative — only 0 (unset) is suspect.
            if (receiveTimeNanos == 0) {
                throw new IllegalArgumentException(
                        "receiveTimeNanos is required — must be a System.nanoTime() instant");
            }
            return new RawTick(this);
        }
    }
}
