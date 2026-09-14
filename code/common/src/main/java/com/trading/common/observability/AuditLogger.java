package com.trading.common.observability;

import java.util.Set;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Immutable audit logging with mandatory redaction
 * (docs/08_implementation/01-foundation.md &rarr; "Observability invariant", orig L727).
 */
public final class AuditLogger {

    private AuditLogger() {}

    private static final String REDACTED = "***REDACTED***";

    /**
     * Sensitive name roots. A field is redacted when its <em>normalized</em> name
     * contains one of these tokens (case-insensitive, non-alphanumerics stripped),
     * which is what makes the concrete contract spellings match:
     * {@code broker_order_id_secret}, {@code auth_token}/{@code clientToken},
     * {@code api_key}/{@code accessKey}, {@code password}.
     *
     * <p>The roots are the generic words, not the concrete field names: the
     * concrete names normalize to {@code authtoken}/{@code apikey}, so a field
     * named exactly {@code token} or {@code key} matched nothing and leaked
     * verbatim. The cost is deliberate over-redaction (a field such as
     * {@code monkey} contains {@code key}) — losing a harmless telemetry field is
     * cheaper than emitting a credential.
     */
    public static final Set<String> REDACTED_FIELDS = Set.of("secret", "token", "key", "password");

    /** Non-alphanumerics are stripped before matching ({@code api_key} &rarr; {@code apikey}). */
    private static final Pattern NON_ALNUM = Pattern.compile("[^A-Za-z0-9]");

    /** {@link #REDACTED_FIELDS} in normalized form; computed once, not per event. */
    private static final Set<String> NORMALIZED_FIELDS =
            REDACTED_FIELDS.stream().map(AuditLogger::normalize).collect(Collectors.toUnmodifiableSet());

    private static String normalize(String field) {
        return NON_ALNUM.matcher(field).replaceAll("").toLowerCase(Locale.ROOT);
    }

    /**
     * R-134: redaction is case-insensitive and token-contained. The old
     * exact-match {@code REDACTED_FIELDS.contains(field)} leaked data for any
     * casing/format variant (apiKey, authToken, API_KEY, api-key ...). We
     * normalize by stripping non-alphanumerics and compare case-insensitively.
     *
     * <p>An unknown ({@code null}) field name is redacted rather than passed
     * through: mandatory redaction must fail closed, and a null name is a caller
     * bug either way.
     */
    public static String redact(String field, String value) {
        if (value == null) {
            return null;
        }
        if (field == null) {
            return REDACTED;
        }
        String normalized = normalize(field);
        for (String token : NORMALIZED_FIELDS) {
            if (normalized.contains(token)) {
                return REDACTED;
            }
        }
        return value;
    }

    /** An audit record is append-only; mutation in place is forbidden. */
    public static boolean isMutableUpdateAllowed() {
        return false;
    }
}
