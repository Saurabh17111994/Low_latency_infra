package com.trading.capture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PostbackDecoderTest {

    @Test
    void decodeExtractsAllFields() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("id", "BRK-1");
        raw.put("exchangeOrderID", "EX-1");
        raw.put("remarks", "INS123");
        raw.put("orderStatus", "COMPLETE");
        raw.put("reportType", "Fill");
        raw.put("fillShares", "2");
        raw.put("averagePrice", "15050");
        raw.put("fillPrice", "15050");
        raw.put("fillQuantity", "2");
        raw.put("fillTime", "2026-08-19T10:00:00Z");
        raw.put("token", "3045");
        raw.put("exchangeUpdateTime", "2026-08-19T10:00:00Z");
        PostbackDecoder.DecodedPostback d = PostbackDecoder.decode(raw);
        assertThat(d.brokerOrderId()).isEqualTo("BRK-1");
        assertThat(d.exchangeOrderId()).isEqualTo("EX-1");
        assertThat(d.clientOrderRef()).isEqualTo("INS123");
        assertThat(d.orderStatus()).isEqualTo("COMPLETE");
        assertThat(d.reportType()).isEqualTo("Fill");
        assertThat(d.fillShares()).isEqualTo("2");
        assertThat(d.averagePrice()).isEqualTo("15050");
        assertThat(d.fillPrice()).isEqualTo("15050");
        assertThat(d.fillQuantity()).isEqualTo("2");
        assertThat(d.fillTime()).isEqualTo("2026-08-19T10:00:00Z");
        assertThat(d.instrumentToken()).isEqualTo("3045");
        assertThat(d.exchangeUpdateTime()).isEqualTo("2026-08-19T10:00:00Z");
        assertThat(d.receivedTsMs()).isGreaterThan(0);
    }

    @Test
    void decodeHandlesStringTrimAndNumberAndNull() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("id", "  BRK-1  ");
        raw.put("exchangeOrderID", "  EX-9  ");
        raw.put("remarks", null);
        raw.put("fillShares", 2); // Number
        raw.put("averagePrice", 15050L);
        raw.put("token", 3045);
        raw.put("orderStatus", "  OPEN ");
        // missing reportType etc -> empty

        PostbackDecoder.DecodedPostback d = PostbackDecoder.decode(raw);
        assertThat(d.brokerOrderId()).isEqualTo("BRK-1");
        assertThat(d.exchangeOrderId()).isEqualTo("EX-9");
        assertThat(d.clientOrderRef()).isEqualTo("");
        assertThat(d.fillShares()).isEqualTo("2");
        assertThat(d.averagePrice()).isEqualTo("15050");
        assertThat(d.instrumentToken()).isEqualTo("3045");
        assertThat(d.orderStatus()).isEqualTo("OPEN");
        assertThat(d.reportType()).isEqualTo("");
        assertThat(d.exchangeUpdateTime()).isEqualTo("");
    }

    @Test
    void decodeRejectsIdentityLessPayload() {
        // P2-205: null or empty identity must fail closed, not yield a phantom record.
        assertThatThrownBy(() -> PostbackDecoder.decode(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("without identity");
        assertThatThrownBy(() -> PostbackDecoder.decode(Map.of("id", " ", "remarks", "")))
                .isInstanceOf(IllegalArgumentException.class);
        // remarks alone is enough identity.
        assertThat(PostbackDecoder.decode(Map.of("remarks", "R-1")).clientOrderRef()).isEqualTo("R-1");
    }

    @Test
    void decodeOverloadIsDeterministic() {
        // P2-205: explicit receive timestamp — pure, no wall clock.
        Map<String, Object> raw = Map.of("id", "B-1", "remarks", "R-1");
        PostbackDecoder.DecodedPostback a = PostbackDecoder.decode(raw, 1234L);
        PostbackDecoder.DecodedPostback b = PostbackDecoder.decode(raw, 1234L);
        assertThat(a.receivedTsMs()).isEqualTo(1234L);
        assertThat(b).isEqualTo(a);
    }

    @Test
    void goPercentGMatchesGoReferenceTable() {
        // Reference table produced by Go: fmt.Sprintf("%g", v) (strconv shortest).
        // P2-094: Double/Float must reproduce Go's %g, not Java toString.
        Map<Double, String> expected = Map.ofEntries(
                Map.entry(15050.0, "15050"),
                Map.entry(15050.25, "15050.25"),
                Map.entry(0.1, "0.1"),
                Map.entry(2.0, "2"),
                Map.entry(0.0001, "0.0001"),
                Map.entry(1e-5, "1e-05"),
                Map.entry(1e-7, "1e-07"),
                Map.entry(1e-8, "1e-08"),
                Map.entry(100000.0, "100000"),
                Map.entry(1000000.0, "1e+06"),
                Map.entry(1234567.0, "1.234567e+06"),
                Map.entry(123456789.5, "1.234567895e+08"),
                Map.entry(1e21, "1e+21"),
                Map.entry(1e23, "1e+23"));
        for (Map.Entry<Double, String> e : expected.entrySet()) {
            PostbackDecoder.DecodedPostback d =
                    PostbackDecoder.decode(Map.of("id", "B-1", "averagePrice", e.getKey()));
            assertThat(d.averagePrice()).as("averagePrice for %s", e.getKey())
                    .isEqualTo(e.getValue());
        }
        // Negative sign and float path.
        PostbackDecoder.DecodedPostback neg =
                PostbackDecoder.decode(Map.of("id", "B-1", "averagePrice", -0.5));
        assertThat(neg.averagePrice()).isEqualTo("-0.5");
        PostbackDecoder.DecodedPostback flt =
                PostbackDecoder.decode(Map.of("id", "B-1", "averagePrice", 2.0f));
        assertThat(flt.averagePrice()).isEqualTo("2");
    }

    @Test
    void goFormatGNonFiniteMatchesGoSpelling() {
        assertThat(PostbackDecoder.goFormatG(Double.NaN)).isEqualTo("NaN");
        assertThat(PostbackDecoder.goFormatG(Double.POSITIVE_INFINITY)).isEqualTo("+Inf");
        assertThat(PostbackDecoder.goFormatG(Double.NEGATIVE_INFINITY)).isEqualTo("-Inf");
        assertThat(PostbackDecoder.goFormatG(-0.0)).isEqualTo("-0");
    }
}
