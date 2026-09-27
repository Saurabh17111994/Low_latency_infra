package com.trading.compute.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.tools.FeatureRegistryDump;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Append-only guard for the feature registry (DEC-057).
 *
 * <p>This is the tripwire that keeps "adding a feature is one registry line"
 * safe for humans and agents: the ledger pins every id/name pair, the registry
 * must match it exactly (append a new line when you add a feature), and any
 * rename, renumber, reorder or delete fails here with the fix in the message.
 * The runtime validator in {@link FeatureRegistry#validate} and the routing
 * exclusion of RETIRED entries are the other half of the guard.
 */
class FeatureRegistryPinTest {

    private static final String LEDGER = "/feature-registry-pins.tsv";

    private static List<String[]> ledgerRows() throws Exception {
        List<String[]> pins = new ArrayList<>();
        try (InputStream in = FeatureRegistryPinTest.class.getResourceAsStream(LEDGER)) {
            assertTrue(in != null, "missing pin ledger resource " + LEDGER);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                String[] parts = trimmed.split("\t");
                assertEquals(2, parts.length, "pin ledger line must be <id>\\t<name>: " + line);
                pins.add(parts);
            }
        }
        return pins;
    }

    @Test
    void ledgerPinsEveryFeatureInIdOrder() throws Exception {
        List<String[]> pins = ledgerRows();
        assertEquals(
                FeatureRegistry.SIZE,
                pins.size(),
                "the pin ledger has " + pins.size() + " rows but the registry has "
                        + FeatureRegistry.SIZE + " features. Adding a feature: append its "
                        + "<id>\\t<name> line to " + LEDGER + ". Removing one: set the registry "
                        + "status to RETIRED and keep the ledger line (DEC-057).");
        for (int i = 0; i < pins.size(); i++) {
            assertEquals(
                    String.valueOf(i),
                    pins.get(i)[0],
                    "pin ledger rows must be 0..N-1 in order — ids are never reordered (DEC-057)");
        }
    }

    @Test
    void pinnedIdsKeepTheirNames() throws Exception {
        for (String[] pin : ledgerRows()) {
            int id = Integer.parseInt(pin[0]);
            FeatureDef def = FeatureRegistry.byId(id);
            assertEquals(
                    pin[1],
                    def.name(),
                    "feature id " + id + " is pinned as '" + pin[1] + "' but the registry says '"
                            + def.name() + "'. Renaming, renumbering or reusing an id is forbidden "
                            + "(DEC-057): keep the old line (status RETIRED when unused) and add a "
                            + "new feature with the next id.");
        }
    }

    @Test
    void ledgerAgreesWithTheRegistryDump() throws Exception {
        List<String> ledger = new ArrayList<>();
        for (String[] pin : ledgerRows()) {
            ledger.add(pin[0] + "\t" + pin[1]);
        }
        assertEquals(
                ledger,
                FeatureRegistryDump.pinLines(),
                "the pin ledger and FeatureRegistryDump disagree — regenerate the ledger with "
                        + "'FeatureRegistryDump' and commit both");
    }
}
