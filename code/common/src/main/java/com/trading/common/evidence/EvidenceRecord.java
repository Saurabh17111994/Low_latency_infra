package com.trading.common.evidence;

import java.util.ArrayList;
import java.util.List;

/**
 * Evidence record format
 * (docs/08_implementation/01-foundation.md &rarr; "Evidence record format", orig L159).
 *
 * <p>One record per verified capability; supports audit and the release-gate review. The spec
 * table gives every field required content, so the canonical constructor refuses a null or blank
 * value: an incomplete record is not evidence, and storing one silently undermines the audit and
 * release-gate decisions that read it (P6-272). The requirement list is copied on the way in, so
 * a caller cannot keep a reference and mutate the linkage of a record that was already accepted
 * (P6-273).
 *
 * <p>{@code limitations} is required like the rest — a record with no known limitation says so
 * ("none") rather than leaving the field to be read as "not checked".
 */
public record EvidenceRecord(
        String workItemId,
        List<String> requirementIds,
        String artifact,
        String version,
        String environment,
        String workload,
        String clock,
        String result,
        String owner,
        String date,
        String limitations) {

    public EvidenceRecord {
        workItemId = required("workItemId", workItemId);
        requirementIds = copyOfIds(requirementIds);
        artifact = required("artifact", artifact);
        version = required("version", version);
        environment = required("environment", environment);
        workload = required("workload", workload);
        clock = required("clock", clock);
        result = required("result", result);
        owner = required("owner", owner);
        date = required("date", date);
        limitations = required("limitations", limitations);
    }

    private static String required(String field, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "evidence record field '" + field + "' must not be null or blank");
        }
        return value;
    }

    /** Defensive copy: an empty list stays empty, elements must be usable references. */
    private static List<String> copyOfIds(List<String> ids) {
        if (ids == null) {
            throw new IllegalArgumentException(
                    "evidence record field 'requirementIds' must not be null");
        }
        List<String> copy = new ArrayList<>(ids.size());
        for (String id : ids) {
            copy.add(required("requirementIds element", id));
        }
        return List.copyOf(copy);
    }
}
