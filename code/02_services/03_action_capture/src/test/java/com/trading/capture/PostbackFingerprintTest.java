package com.trading.capture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PostbackFingerprintTest {

    /** The 7 identity fields Go fingerprints (go-bridge/postback.go:209). */
    private static Map<String, String> identityFields() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("id", "BRK-1");
        m.put("remarks", "REF-1");
        m.put("status", "COMPLETE");
        m.put("report_type", "Fill");
        m.put("fill_shares", "2");
        m.put("average_price", "15050");
        m.put("exchange_update_time", "2026-08-19T10:00:00Z");
        return m;
    }

    @Test
    void fingerprintIsDeterministicAndSorted() {
        Map<String, String> a = new LinkedHashMap<>();
        a.put("id", "BRK-1");
        a.put("remarks", "REF-1");
        a.put("status", "COMPLETE");
        a.put("report_type", "Fill");
        a.put("fill_shares", "2");
        a.put("average_price", "15050");
        a.put("exchange_update_time", "2026-08-19T10:00:00Z");

        // same content but different insertion order
        Map<String, String> b = new LinkedHashMap<>();
        b.put("exchange_update_time", "2026-08-19T10:00:00Z");
        b.put("average_price", "15050");
        b.put("fill_shares", "2");
        b.put("report_type", "Fill");
        b.put("status", "COMPLETE");
        b.put("remarks", "REF-1");
        b.put("id", "BRK-1");

        String f1 = PostbackFingerprint.fingerprint(a);
        String f2 = PostbackFingerprint.fingerprint(b);
        assertThat(f1).isEqualTo(f2);
        assertThat(f1).hasSize(64);
        assertThat(f1).matches("[0-9a-f]{64}");
    }

    @Test
    void fingerprintDiffersOnValueChange() {
        Map<String, String> base = Map.of(
                "id", "BRK-1",
                "remarks", "REF-1",
                "status", "COMPLETE",
                "report_type", "Fill",
                "fill_shares", "2",
                "average_price", "15050",
                "exchange_update_time", "2026-08-19T10:00:00Z");
        Map<String, String> altered = new LinkedHashMap<>(base);
        altered.put("fill_shares", "3");
        assertThat(PostbackFingerprint.fingerprint(base))
                .isNotEqualTo(PostbackFingerprint.fingerprint(altered));
    }

    @Test
    void fingerprintCanonicalIsGoJsonMarshalForm() {
        // Go canonical: json.Marshal(map[string]string) — sorted keys, "k":"v", no spaces.
        Map<String, String> m = Map.of(
                "id", "BRK-1",
                "remarks", "R1",
                "status", "OPEN",
                "report_type", "Fill",
                "fill_shares", "1",
                "average_price", "100",
                "exchange_update_time", "T");
        String canonical = "{\"average_price\":\"100\",\"exchange_update_time\":\"T\","
                + "\"fill_shares\":\"1\",\"id\":\"BRK-1\",\"remarks\":\"R1\","
                + "\"report_type\":\"Fill\",\"status\":\"OPEN\"}";
        assertThat(PostbackFingerprint.sha256Hex(canonical)).isEqualTo(PostbackFingerprint.fingerprint(m));
    }

    @Test
    void fingerprintMatchesGoBridgeKnownVector() {
        // Vector computed by the Go bridge's own fingerprint() implementation
        // (json.Marshal + sha256, go-bridge/models.go:200) for identityFields().
        assertThat(PostbackFingerprint.fingerprint(identityFields()))
                .isEqualTo("01fc1cd7d1ffd97cc763a067669077e82415d947981a4c9f5beb1ef5069ca8ac");
    }

    @Test
    void fingerprintMatchesGoBridgeEscapingVector() {
        // Pins Go's default EscapeHTML behaviour (json.Marshal escapes < > &) plus
        // quote/backslash handling. Vector computed by the Go implementation.
        Map<String, String> m = Map.of(
                "id", "BRK<2>",
                "remarks", "A&B \"Q\" \\Z",
                "status", "", "report_type", "", "fill_shares", "",
                "average_price", "", "exchange_update_time", "");
        assertThat(PostbackFingerprint.fingerprint(m))
                .isEqualTo("3934d20619eddb30f04849a1bf1e4ebd953d3a9eda951f8f973814d692623510");
    }

    @Test
    void fingerprintIgnoresNonIdentityKeys() {
        // Go's fingerprint literal only carries the 7 identity keys, so token /
        // fillPrice / etc. must not change the digest.
        Map<String, String> withExtras = new LinkedHashMap<>(identityFields());
        withExtras.put("token", "99999");
        withExtras.put("fillPrice", "1.5");
        withExtras.put("exchangeOrderID", "EX-1");
        assertThat(PostbackFingerprint.fingerprint(withExtras))
                .isEqualTo(PostbackFingerprint.fingerprint(identityFields()));
    }

    @Test
    void fingerprintTreatsMissingKeysAsEmptyAndTrimsValues() {
        // Go stringField: missing → "", string → TrimSpace.
        Map<String, String> partial = Map.of("id", "  BRK-1  ", "remarks", "REF-1");
        Map<String, String> full = new LinkedHashMap<>(identityFields());
        full.put("status", "");
        full.put("report_type", "");
        full.put("fill_shares", "");
        full.put("average_price", "");
        full.put("exchange_update_time", "");
        assertThat(PostbackFingerprint.fingerprint(partial))
                .isEqualTo(PostbackFingerprint.fingerprint(full));
    }

    @Test
    void sha256HexKnownVectors() {
        // sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assertThat(PostbackFingerprint.sha256Hex(""))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertThat(PostbackFingerprint.sha256Hex(null))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        // sha256("hello")
        assertThat(PostbackFingerprint.sha256Hex("hello"))
                .isEqualTo("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
    }

    @Test
    void fingerprintHandlesNullValuesAsEmpty() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("id", null);
        m.put("remarks", "R1");
        Map<String, String> expected = Map.of("id", "", "remarks", "R1");
        assertThat(PostbackFingerprint.fingerprint(m))
                .isEqualTo(PostbackFingerprint.fingerprint(expected));
    }

    @Test
    void fingerprintRejectsEmptyInput() {
        // Was: silently hashed "" — an identity-less postback got a valid-looking digest.
        assertThatThrownBy(() -> PostbackFingerprint.fingerprint(Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PostbackFingerprint.fingerprint(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
