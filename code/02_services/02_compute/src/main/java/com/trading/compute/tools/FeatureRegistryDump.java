package com.trading.compute.tools;

import com.trading.compute.feature.FeatureDef;
import com.trading.compute.feature.FeatureRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Prints the feature registry so external readers (and the pin ledger) have one
 * machine-readable source for id → name (DEC-057).
 *
 * <p>Usage:
 *
 * <pre>
 *   FeatureRegistryDump            # pin ledger rows: &lt;id&gt;\t&lt;name&gt;
 *   FeatureRegistryDump --full     # + status, cadence, declared timeframes
 * </pre>
 */
public final class FeatureRegistryDump {

    private FeatureRegistryDump() {}

    public static void main(String[] args) {
        boolean full = args.length > 0 && "--full".equals(args[0]);
        System.out.print(full ? fullDump() : String.join("\n", pinLines()) + "\n");
    }

    /** Pin-ledger rows ({@code id\tname}) in id order — the format of the test ledger. */
    public static List<String> pinLines() {
        List<String> lines = new ArrayList<>(FeatureRegistry.SIZE);
        for (FeatureDef def : FeatureRegistry.all()) {
            lines.add(def.id() + "\t" + def.name());
        }
        return lines;
    }

    /** Full dump: id, name, status, cadence, declared timeframes. */
    public static String fullDump() {
        StringBuilder sb = new StringBuilder();
        sb.append("# id\tname\tstatus\tcadence\ttimeframes\n");
        for (FeatureDef def : FeatureRegistry.all()) {
            StringJoiner timeframes = new StringJoiner(",");
            def.timeframes().forEach(tf -> timeframes.add(tf.code()));
            sb.append(def.id())
                    .append('\t')
                    .append(def.name())
                    .append('\t')
                    .append(def.status())
                    .append('\t')
                    .append(def.cadence())
                    .append('\t')
                    .append(timeframes)
                    .append('\n');
        }
        return sb.toString();
    }
}
