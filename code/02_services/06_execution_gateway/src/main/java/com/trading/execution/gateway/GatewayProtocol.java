package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Versioned authenticated private envelope between this gateway and Nautilus. */
public final class GatewayProtocol {
    public record Envelope(String protocolVersion, String messageType, String requestId,
                           String accountScopeId, String executionPartitionId, String payloadHash,
                           long gateEpoch, String fenceToken, long deadlineEpochMs, JsonNode payload,
                           String authentication) {}

    public record Verification(boolean accepted, String reason, Envelope envelope) {}

    private static final String HMAC = "HmacSHA256";
    /**
     * Canonical-form generations (P3-079). v1 joins fields with a bare newline; v2 length-prefixes
     * them. Both are accepted — the version field selects the form, so an upgrade never has to be
     * atomic across the gateway, the executor and the sandbox client.
     */
    static final String PROTOCOL_V1 = "execution-gateway.v1";
    static final String PROTOCOL_V2 = "execution-gateway.v2";
    /**
     * P3-078: the only accepted intent message types. An unknown message_type
     * verifies the HMAC fine but must never drive execution — allowlist at
     * verify, not trust at the call site.
     */
    static final java.util.Set<String> ALLOWED_MESSAGE_TYPES =
            java.util.Set.of("EXECUTION_INTENT", "EXECUTION_EVENT");
    /**
     * P3-078: freshness bounds (all in epoch-ms). TTL caps how long a stolen
     * envelope stays replayable; the future-skew bound caps a far-future
     * deadline that would otherwise act as an unexpiring bearer. Generous
     * during the offline/parity phase (fixtures use fixed far-future
     * deadlines decades out); tighten toward minutes before live money.
     */
    static final long MAX_ENVELOPE_TTL_MS = 100L * 365 * 24 * 60 * 60 * 1000;
    static final long MAX_FUTURE_SKEW_MS = 100L * 365 * 24 * 60 * 60 * 1000;
    /**
     * P3-078: bounded replay window. The server is the replay boundary: every
     * accepted (request_id, payload_hash) pair is remembered until its own
     * deadline, and a verbatim re-POST is rejected even with a valid HMAC
     * (transport redelivery reuses the same envelope bytes ~16x in the flood
     * soak, so the soak mints one envelope per POST instead). Bounded
     * (evict-at-insert) — never an unbounded seen-set.
     */
    private final java.util.Map<String, Long> seenRequests = new java.util.LinkedHashMap<>() {
        @Override protected boolean removeEldestEntry(java.util.Map.Entry<String, Long> eldest) {
            return size() > 8192;
        }
    };
    private final ObjectMapper mapper;
    private final String secret;

    public GatewayProtocol(String secret) {
        if (secret == null || secret.isBlank()) throw new IllegalArgumentException("secret required");
        this.secret = secret;
        this.mapper = new ObjectMapper();
    }

    /**
     * P3-004: bearer check for the /control plane. The control endpoint is a
     * human-operator action (no envelope to HMAC), so the shared secret travels
     * as "Bearer &lt;secret&gt;" — same loopback-secret trust as the bridge
     * transport. Constant-time compare; null-safe.
     */
    public boolean authorizedBearer(String header) {
        if (header == null) return false;
        String prefix = "Bearer ";
        if (!header.startsWith(prefix)) return false;
        String presented = header.substring(prefix.length()).trim();
        return MessageDigest.isEqual(presented.getBytes(StandardCharsets.UTF_8),
                secret.getBytes(StandardCharsets.UTF_8));
    }

    public String encode(Envelope unsigned) throws Exception {
        // P3-297: String.join throws raw NPE on null parts and a null payload
        // signs bytes that never round-trip verify — fail fast with the field.
        requireEnvelope(unsigned);
        // P3-296: never sign a caller-supplied hash blind — derive and fail
        // fast so the bug surfaces at encode, not as a peer-side mismatch.
        String actualHash = sha256(mapper.writeValueAsBytes(unsigned.payload()));
        Envelope signed = unsigned;
        if (unsigned.payloadHash() == null || unsigned.payloadHash().isBlank()) {
            signed = new Envelope(unsigned.protocolVersion(), unsigned.messageType(),
                    unsigned.requestId(), unsigned.accountScopeId(),
                    unsigned.executionPartitionId(), actualHash, unsigned.gateEpoch(),
                    unsigned.fenceToken(), unsigned.deadlineEpochMs(), unsigned.payload(),
                    unsigned.authentication());
        } else if (!Objects.equals(unsigned.payloadHash(), actualHash)) {
            throw new IllegalArgumentException("payload_hash does not match payload");
        }
        // P3-079: newline-bearing identity fields make the \n-joined v1 canonical
        // form ambiguous across field boundaries — reject before signing. v2 is
        // length-prefixed and needs no such guard.
        requireNoNewlinesIfNotV2(signed);
        String canonical = canonical(signed);
        String auth = sign(canonical);
        ObjectNode node = mapper.createObjectNode();
        node.put("protocol_version", unsigned.protocolVersion());
        node.put("message_type", unsigned.messageType());
        node.put("request_id", unsigned.requestId());
        node.put("account_scope_id", unsigned.accountScopeId());
        node.put("execution_partition_id", unsigned.executionPartitionId());
        node.put("payload_hash", signed.payloadHash());
        node.put("gate_epoch", unsigned.gateEpoch());
        node.put("fence_token", unsigned.fenceToken());
        node.put("deadline_epoch_ms", unsigned.deadlineEpochMs());
        node.set("payload", unsigned.payload());
        node.put("authentication", auth);
        return mapper.writeValueAsString(node);
    }

    public Verification verify(String json, String expectedVersion, long nowMs) {
        final JsonNode n;
        final Envelope e;
        try {
            n = mapper.readTree(json);
            e = new Envelope(text(n, "protocol_version"), text(n, "message_type"),
                    text(n, "request_id"), text(n, "account_scope_id"),
                    text(n, "execution_partition_id"), text(n, "payload_hash"),
                    n.path("gate_epoch").asLong(Long.MIN_VALUE), text(n, "fence_token"),
                    n.path("deadline_epoch_ms").asLong(Long.MIN_VALUE), n.path("payload"),
                    text(n, "authentication"));
        } catch (com.fasterxml.jackson.core.JsonProcessingException | IllegalArgumentException ex) {
            // P3-299: only peer-input failures map to a rejection — local
            // crypto misconfiguration below throws fail-closed instead.
            return reject("malformed envelope");
        } catch (Exception ex) {
            throw new IllegalStateException("gateway verification failure", ex);
        }
        try {
            if (!acceptsVersion(expectedVersion, e.protocolVersion())) return reject("unsupported version");
            // P3-078: fence_token/gate_epoch are HMAC-bound here but are NOT the
            // fencing authority on this (inbound) leg — verify() only proves who
            // wrote the envelope. The handoff consumes envelope().payload() alone
            // (GatewayHttpServer); the fence that authorizes execution is re-read
            // from the durable Execution_Gate row and substituted for it
            // (NautilusIntentClient), where monotonicity is structural
            // (fenceSequence.incrementAndGet). Do NOT add a monotonicity/replay
            // check on the envelope's fence_token: it guards an unused field, and
            // a process-local cache could not guarantee monotonicity across a
            // restart. Likewise gate_epoch is a generation counter (0, +1 per
            // transition), never a timestamp — see the freshness rule below.
            if (e.requestId().isBlank() || e.accountScopeId().isBlank() || e.executionPartitionId().isBlank()
                    || e.payloadHash().isBlank() || e.fenceToken().isBlank()) return reject("missing identity");
            // P3-298: MissingNode payload must fail as absent, not as a hash
            // mismatch — and only AFTER the presence check so crypto runs on a
            // real node. (Signer's writeValueAsBytes(MissingNode) would
            // otherwise accept a payload-less envelope.)
            if (!n.has("payload") || n.path("payload").isMissingNode() || n.path("payload").isNull())
                return reject("missing payload");
            if (e.authentication() == null || e.authentication().isBlank()) return reject("missing authentication");
            // P3-078: presence first — a missing gate_epoch decoded as
            // Long.MIN_VALUE was silently accepted as a "fresh" envelope.
            if (!n.hasNonNull("gate_epoch") || !n.hasNonNull("deadline_epoch_ms"))
                return reject("missing freshness");
            if (!ALLOWED_MESSAGE_TYPES.contains(e.messageType())) return reject("unsupported message type");
            if (e.deadlineEpochMs() < nowMs) return reject("deadline expired");
            // Bounded lifetime: a stolen envelope must not stay replayable via
            // a far-future deadline. Absolute cap only (no gate_epoch coupling:
            // gate_epoch is the durable fence generation, not a timestamp, and
            // must never be mixed into freshness arithmetic).
            if (e.deadlineEpochMs() - nowMs > MAX_ENVELOPE_TTL_MS + MAX_FUTURE_SKEW_MS)
                return reject("deadline too far in future");
            // P3-079: same boundary rule as encode — verify what was signed. v1 only:
            // a v2 signature over a newline-bearing field is unambiguous.
            requireNoNewlinesIfNotV2(e);
            if (!MessageDigest.isEqual(e.authentication().getBytes(StandardCharsets.UTF_8),
                    sign(canonical(e)).getBytes(StandardCharsets.UTF_8))) return reject("authentication failed");
            if (!Objects.equals(e.payloadHash(), sha256(mapper.writeValueAsBytes(e.payload())))) {
                return reject("payload hash mismatch");
            }
            // P3-078: verbatim replay until deadline — reject a seen
            // (request_id, payload_hash) pair even when its HMAC is valid.
            // Keyed on hash too (not request_id alone): a client retry of the
            // same logical intent carries a fresh request_id, while a byte-copy
            // attacker replay keeps both — narrowing to the pair avoids turning
            // legitimate per-intent retries into duplicates.
            synchronized (seenRequests) {
                String replayKey = e.requestId() + "\n" + e.payloadHash();
                Long seenExpiry = seenRequests.get(replayKey);
                if (seenExpiry != null && nowMs < seenExpiry) return reject("duplicate request");
                seenRequests.put(replayKey, e.deadlineEpochMs());
            }
            return new Verification(true, "accepted", e);
        } catch (java.security.NoSuchAlgorithmException | java.security.InvalidKeyException ex) {
            // P3-299: local misconfiguration, not peer input — fail closed and
            // loud so a fully-broken auth path pages instead of reading as 401s.
            throw new IllegalStateException("gateway crypto unavailable", ex);
        } catch (com.fasterxml.jackson.core.JsonProcessingException | IllegalArgumentException ex) {
            return reject("malformed envelope");
        } catch (Exception ex) {
            throw new IllegalStateException("gateway verification failure", ex);
        }
    }

    private Verification reject(String reason) { return new Verification(false, reason, null); }

    /**
     * Picks the canonical form from the envelope's version. Deterministic, never a guess: a given
     * version string has exactly one form, and the operator's config pins which versions are
     * accepted, so a peer cannot choose the encoding.
     *
     * <p>v2 is strictly opt-in by name. Every other version — including a custom generation string
     * an operator may have configured — keeps the legacy newline join <b>byte-for-byte</b>, so
     * nothing that signed or verified before this change signs or verifies differently now. Making
     * an unrecognised version fail here would have broken those callers for no security gain: the
     * form is not ambiguous, only the version-to-form mapping has to be total and stable.
     *
     * <p>P3-080: insertion-order canonical bytes are the cross-language contract
     * (Java Jackson ObjectNode, Rust preserve_order, Python separators) — the
     * parity fixtures pin byte-identical HMACs, so a sorting canonicalizer
     * here would break every peer. Kept, documented, not "fixed".
     */
    private String canonical(Envelope e) throws Exception {
        return PROTOCOL_V2.equals(e.protocolVersion()) ? canonicalV2(e) : canonicalV1(e);
    }

    /**
     * v1: fields joined by a bare newline. Ambiguous across field boundaries — {@code request_id}
     * "a\nb" with scope "c" signs identically to "a" with "b\nc" — which is why
     * {@link #requireNoNewlines} exists. Retained byte-for-byte so existing peers keep verifying.
     */
    private String canonicalV1(Envelope e) throws Exception {
        return String.join("\n", e.protocolVersion(), e.messageType(), e.requestId(), e.accountScopeId(),
                e.executionPartitionId(), e.payloadHash(), Long.toString(e.gateEpoch()), e.fenceToken(),
                Long.toString(e.deadlineEpochMs()), mapper.writeValueAsString(e.payload()));
    }

    /**
     * v2 (P3-079): length-prefixed, so the encoding is <b>injective by construction</b> — no field
     * content can forge a boundary, because the parser never has to guess where a field ends. This
     * supersedes v1's {@code requireNoNewlines} backstop rather than relying on it; that backstop
     * stays in place for v1 peers only, and is deliberately not applied to v2.
     */
    private String canonicalV2(Envelope e) throws Exception {
        return lengthPrefixed(e.protocolVersion(), e.messageType(), e.requestId(), e.accountScopeId(),
                e.executionPartitionId(), e.payloadHash(), Long.toString(e.gateEpoch()), e.fenceToken(),
                Long.toString(e.deadlineEpochMs()), mapper.writeValueAsString(e.payload()));
    }

    /**
     * THE canonical v2 rule. Java, Rust and Python must produce these bytes identically — a
     * divergence is not a cosmetic bug, it is a silently rejected (or wrongly accepted) signature.
     *
     * <p>For each field in order, append {@code decimal(UTF-8 byte length)} + {@code ':'} + the
     * field, concatenated with no separator. E.g. {@code ["a", "bc"]} → {@code "1:a2:bc"}.
     *
     * <p>It is the <b>byte</b> length, not the char length: counting chars would be correct for
     * ASCII and wrong the moment any field leaves it, and it would be wrong in the direction that
     * makes two languages disagree.
     *
     * <p>Null fields are not mapped to empty here — they throw, exactly as v1's {@code String.join}
     * does, because {@code requireEnvelope} has already rejected them before signing. Quietly
     * substituting "" would invent an acceptance v1 never had.
     */
    static String lengthPrefixed(String... fields) {
        StringBuilder sb = new StringBuilder();
        for (String f : fields) {
            sb.append(f.getBytes(StandardCharsets.UTF_8).length).append(':').append(f);
        }
        return sb.toString();
    }

    private String sign(String value) throws Exception {
        Mac mac = Mac.getInstance(HMAC);
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC));
        return HexFormat.of().formatHex(mac.doFinal(value.getBytes(StandardCharsets.UTF_8)));
    }

    public static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (Exception e) { throw new IllegalStateException("SHA-256 unavailable", e); }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return "";
        // P3-480: asText() coerces 123/true/{...} to "123"/"true"/"" — a numeric
        // request_id would verify under a different type than a strict peer
        // enforces. Only textual nodes count; anything else is missing.
        if (!v.isTextual()) return "";
        return v.asText();
    }

    /** P3-297: every signed part must be present — name the missing field. */
    private static void requireEnvelope(Envelope e) {
        if (e.protocolVersion() == null) throw new IllegalArgumentException("protocolVersion required");
        if (e.messageType() == null) throw new IllegalArgumentException("messageType required");
        if (e.requestId() == null) throw new IllegalArgumentException("requestId required");
        if (e.accountScopeId() == null) throw new IllegalArgumentException("accountScopeId required");
        if (e.executionPartitionId() == null) throw new IllegalArgumentException("executionPartitionId required");
        if (e.fenceToken() == null) throw new IllegalArgumentException("fenceToken required");
        if (e.payload() == null || e.payload().isMissingNode() || e.payload().isNull())
            throw new IllegalArgumentException("payload required");
    }

    /**
     * P3-079: the v1 canonical form joins fields with a bare newline, so a
     * request_id of "a\nb" plus scope "c" signs identically to "a" plus "b\nc"
     * (cross-field shift, valid HMAC, wrong binding). Refuse newline-bearing fields on both
     * sign and verify paths so v1 has no silent mis-binding either side.
     *
     * <p>v2 removes the ambiguity structurally (see {@link #canonicalV2}), so this is applied
     * wherever the legacy form is still in use — see {@link #requireNoNewlinesIfNotV2}. Keeping a
     * v1-era restriction on the v2 path would leave a field unreachable for no reason.
     */
    private static void requireNoNewlines(Envelope e) {
        for (String part : new String[]{e.protocolVersion(), e.messageType(), e.requestId(),
                e.accountScopeId(), e.executionPartitionId(), e.payloadHash(), e.fenceToken()}) {
            if (part != null && (part.indexOf('\n') >= 0 || part.indexOf('\r') >= 0))
                throw new IllegalArgumentException("identity field must not contain newline");
        }
    }

    /**
     * Applies the v1 boundary backstop wherever the legacy form is in use — i.e. everywhere except
     * v2. Keyed on the form, not on the literal {@code v1}: a custom version string still gets the
     * newline join (see {@link #canonical}) and therefore still needs the backstop.
     */
    private static void requireNoNewlinesIfNotV2(Envelope e) {
        if (!PROTOCOL_V2.equals(e.protocolVersion())) requireNoNewlines(e);
    }

    /**
     * P3-079 rollout: the configured version may name more than one accepted generation
     * ({@code "execution-gateway.v1,execution-gateway.v2"}), so a mixed fleet migrates without a
     * flag day. A single value behaves exactly as before — this widens nothing by itself, and the
     * operator opts in explicitly.
     *
     * <p>Deliberately does <b>not</b> whitelist the known generation literals. Pinning them here
     * would reject an operator's custom version string that verified perfectly well before this
     * change, and it would buy no security: {@link #canonical} maps a version to its form
     * deterministically, so there is no form for a peer to guess at, and the accepted set is the
     * operator's config — which a peer cannot influence. Accepting two forms is likewise not a
     * downgrade risk, because the version is the first field of the canonical string and is
     * therefore HMAC-bound: relabelling a v2 envelope as v1 changes the signed bytes and fails.
     */
    private static boolean acceptsVersion(String configured, String presented) {
        if (configured == null || presented == null) return false;
        for (String candidate : configured.split(",")) {
            if (candidate.trim().equals(presented)) return true;
        }
        return false;
    }
}
