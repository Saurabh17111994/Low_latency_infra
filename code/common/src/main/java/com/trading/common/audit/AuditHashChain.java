package com.trading.common.audit;

import com.trading.common.schema.ImmutabilityProtocol;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Policy-controlled audit hash chain
 * (docs/08_implementation/01-foundation.md &rarr; "Approved audit-retention boundary";
 * docs/02_requirements/03-non-functional.md &sect;3.4.1 "Reconstruction integrity").
 *
 * <p>Every audit event carries a content hash. Each per-day manifest hashes its
 * events in order, and each manifest hash links to the previous manifest,
 * forming a chain whose root hash fingerprints the whole retained audit set.
 * Reconstruction verifies the chain and therefore detects a tampered, reordered,
 * or missing event.
 *
 * <p>Fields are validated at construction so the canonical byte form is
 * injective in its inputs: header fields and event ids reject line breaks and
 * the {@code :}/{@code =} delimiters (P6-263), a trading date must be ISO-8601
 * {@code yyyy-MM-dd} so lexicographic order is chronological order (P6-654), and
 * a content hash must be {@value #HASH_HEX_LENGTH}-character SHA-256 hex
 * (P6-262). The canonical bytes of valid input are unchanged, so the byte-exact
 * Python port in {@code 01_platform/04_scripts/r2_legal_hold_check.py} and its
 * golden Java-parity hashes still hold.
 */
public final class AuditHashChain {

    private AuditHashChain() {}

    /** SHA-256 hex length — every hash in the chain has this shape. */
    public static final int HASH_HEX_LENGTH = 64;

    private static final Pattern SHA256_HEX =
            Pattern.compile("[0-9a-fA-F]{" + HASH_HEX_LENGTH + "}");
    private static final Pattern ISO_DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    public enum Verification {
        VALID,
        TAMPERED,
        MISSING_EVENT,
        REORDERED,
        DUPLICATE_EVENT,
        BROKEN_LINK
    }

    /** One retained audit record: identity plus the content hash of its canonical bytes. */
    public record AuditEvent(String eventId, String contentHash) {
        public AuditEvent {
            requireCanonicalField(eventId, "eventId");
            requireNonBlank(contentHash, "contentHash");
            if (!SHA256_HEX.matcher(contentHash).matches()) {
                throw new IllegalArgumentException(
                        "contentHash must be " + HASH_HEX_LENGTH + "-char SHA-256 hex");
            }
        }
    }

    /** One trading day of events for one table — the hashing unit of the chain. */
    public record Manifest(String tradingDate, String table, String schemaVersion,
                           List<AuditEvent> events) {
        public Manifest {
            requireIsoDate(tradingDate);
            requireCanonicalField(table, "table");
            requireCanonicalField(schemaVersion, "schemaVersion");
            if (events == null) {
                throw new IllegalArgumentException("events must be non-null");
            }
            events = List.copyOf(events);
        }

        /** Deterministic canonical serialization — the byte source of {@link #hash()}. */
        public String canonical() {
            StringBuilder sb = new StringBuilder();
            sb.append("manifest-v1\n");
            sb.append("date=").append(tradingDate).append('\n');
            sb.append("table=").append(table).append('\n');
            sb.append("schema=").append(schemaVersion).append('\n');
            sb.append("count=").append(events.size()).append('\n');
            for (AuditEvent e : events) {
                sb.append("event=").append(e.eventId()).append(':')
                        .append(e.contentHash()).append('\n');
            }
            return sb.toString();
        }

        public String hash() {
            return ImmutabilityProtocol.canonicalHash(canonical());
        }
    }

    /** Accumulates events for one manifest. */
    public static final class ManifestBuilder {
        private final String tradingDate;
        private final String table;
        private final String schemaVersion;
        private final List<AuditEvent> events = new ArrayList<>();

        public ManifestBuilder(String tradingDate, String table, String schemaVersion) {
            this.tradingDate = tradingDate;
            this.table = table;
            this.schemaVersion = schemaVersion;
        }

        public ManifestBuilder addEvent(String eventId, String contentHash) {
            events.add(new AuditEvent(eventId, contentHash));
            return this;
        }

        public Manifest build() {
            return new Manifest(tradingDate, table, schemaVersion, events);
        }
    }

    /**
     * Verifies a manifest against the events observed at reconstruction time.
     * VALID only when every event is present, in manifest order, with an
     * identical content hash.
     */
    public static Verification verifyManifestAgainstSource(Manifest manifest,
                                                           List<AuditEvent> observed) {
        if (manifest == null || observed == null) {
            return Verification.MISSING_EVENT;
        }
        Set<String> manifestIds = new HashSet<>();
        for (AuditEvent e : manifest.events()) {
            if (!manifestIds.add(e.eventId())) {
                return Verification.DUPLICATE_EVENT;
            }
        }
        Set<String> observedIds = new HashSet<>();
        for (AuditEvent e : observed) {
            if (e == null) {
                // A hole in the reconstruction input is missing evidence, not a crash.
                return Verification.MISSING_EVENT;
            }
            if (!observedIds.add(e.eventId())) {
                return Verification.DUPLICATE_EVENT;
            }
        }
        // No element is null past this point, so the positional reads below are safe.
        if (!observedIds.equals(manifestIds)) {
            return Verification.MISSING_EVENT;
        }
        List<String> expectedOrder = manifest.events().stream().map(AuditEvent::eventId).toList();
        List<String> observedOrder = observed.stream().map(AuditEvent::eventId).toList();
        if (!expectedOrder.equals(observedOrder)) {
            return Verification.REORDERED;
        }
        for (int i = 0; i < manifest.events().size(); i++) {
            if (!manifest.events().get(i).contentHash().equals(observed.get(i).contentHash())) {
                return Verification.TAMPERED;
            }
        }
        return Verification.VALID;
    }

    /**
     * Linked per-manifest hashes: each manifest's hash commits to the previous
     * one, so a change anywhere in the chain changes every subsequent link.
     */
    public static List<String> linkedHashes(List<Manifest> manifests) {
        Objects.requireNonNull(manifests, "manifests");
        List<String> out = new ArrayList<>();
        String previous = "";
        for (Manifest m : manifests) {
            Objects.requireNonNull(m, "manifest");
            String link = ImmutabilityProtocol.canonicalHash(m.canonical() + "prev=" + previous);
            out.add(link);
            previous = link;
        }
        return out;
    }

    /** Root hash of the whole chain — fingerprints the retained audit set. */
    public static String rootHash(List<Manifest> manifests) {
        Objects.requireNonNull(manifests, "manifests");
        List<String> links = linkedHashes(manifests);
        return links.isEmpty()
                ? ImmutabilityProtocol.canonicalHash("")
                : links.get(links.size() - 1);
    }

    /**
     * Verifies the chain against the expected root hash: strictly increasing
     * trading dates, no event id repeated across the chain, and the recomputed
     * root must equal the expected root.
     */
    public static Verification verifyChain(List<Manifest> manifests, String expectedRootHash) {
        if (manifests == null) {
            return Verification.BROKEN_LINK;
        }
        for (Manifest m : manifests) {
            if (m == null) {
                return Verification.BROKEN_LINK;
            }
        }
        for (int i = 1; i < manifests.size(); i++) {
            // Dates are validated ISO-8601 at construction, so lexicographic
            // order here is chronological order (P6-654).
            if (manifests.get(i - 1).tradingDate().compareTo(manifests.get(i).tradingDate()) >= 0) {
                return Verification.BROKEN_LINK;
            }
        }
        Set<String> seen = new HashSet<>();
        for (Manifest m : manifests) {
            for (AuditEvent e : m.events()) {
                if (!seen.add(e.eventId())) {
                    return Verification.DUPLICATE_EVENT;
                }
            }
        }
        if (expectedRootHash == null || !expectedRootHash.equals(rootHash(manifests))) {
            return Verification.TAMPERED;
        }
        return Verification.VALID;
    }

    private static void requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must be non-blank");
        }
    }

    /**
     * A field that is rendered into the canonical bytes must also be free of line
     * breaks and the {@code :}/{@code =} delimiters: a newline forges extra
     * {@code key=value} lines and a delimiter shifts a field boundary, either of
     * which would let two logically different manifests share one canonical byte
     * string (and therefore one root hash).
     */
    private static void requireCanonicalField(String value, String field) {
        requireNonBlank(value, field);
        if (value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0
                || value.indexOf(':') >= 0 || value.indexOf('=') >= 0) {
            throw new IllegalArgumentException(
                    field + " must not contain a line break, ':' or '='");
        }
    }

    /** ISO-8601 {@code yyyy-MM-dd}, so date ordering is not locale/padding dependent. */
    private static void requireIsoDate(String value) {
        if (value == null || !ISO_DATE.matcher(value).matches()) {
            throw new IllegalArgumentException("tradingDate must be ISO-8601 yyyy-MM-dd");
        }
        try {
            LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("tradingDate must be a real date: " + value);
        }
    }
}
