package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.Test;

// P1-008 guard: subscribes must carry partition IDs on a partitioned table
// (the finding's partition-name overload does not exist) and every admin
// wait must be bounded. Fakes are JDK proxies — Admin/LogScanner are
// interfaces, no new test libraries needed.
final class TickTableViewerSubscribeTest {

    private static PartitionInfo partition(long id, String day) {
        return new PartitionInfo(
                id, new ResolvedPartitionSpec(List.of("event_day"), List.of(day)));
    }

    private static ListOffsetsResult offsetsFor(Collection<Integer> buckets) {
        Map<Integer, CompletableFuture<Long>> futs = new HashMap<>();
        for (int b : buckets) {
            futs.put(b, CompletableFuture.completedFuture(50L + b));
        }
        return new ListOffsetsResult(futs);
    }

    private static Admin fakeAdmin(List<PartitionInfo> partitions) {
        return (Admin) Proxy.newProxyInstance(
                TickTableViewerSubscribeTest.class.getClassLoader(),
                new Class<?>[] {Admin.class},
                (px, m, a) -> {
                    switch (m.getName()) {
                        case "listPartitionInfos":
                            return CompletableFuture.completedFuture(partitions);
                        case "listOffsets":
                            // 3-arg (tp, buckets, spec) and 4-arg
                            // (tp, name, buckets, spec): buckets second-last.
                            @SuppressWarnings("unchecked")
                            Collection<Integer> buckets = (Collection<Integer>) a[a.length - 2];
                            return offsetsFor(buckets);
                        default:
                            throw new UnsupportedOperationException(m.getName());
                    }
                });
    }

    private static LogScanner recordingScanner(List<String> subs) {
        return (LogScanner) Proxy.newProxyInstance(
                TickTableViewerSubscribeTest.class.getClassLoader(),
                new Class<?>[] {LogScanner.class},
                (px, m, a) -> {
                    if (m.getName().equals("subscribe") && a.length == 3) {
                        subs.add(a[0] + ":" + a[1] + ":" + a[2]);
                        return null;
                    }
                    if (m.getName().equals("subscribe") && a.length == 2) {
                        subs.add("plain:" + a[0] + ":" + a[1]);
                        return null;
                    }
                    throw new UnsupportedOperationException(m.getName());
                });
    }

    @Test
    void partitionedSubscribesCarryPartitionIds() throws Exception {
        List<String> subs = new ArrayList<>();
        TickTableViewer.subscribeFromLatest(
                fakeAdmin(List.of(partition(101L, "20260907"), partition(102L, "20260908"))),
                recordingScanner(subs),
                TablePath.of("default", "raw_table_1"),
                List.of(0, 1),
                true);
        assertEquals(List.of("101:0:50", "101:1:51", "102:0:50", "102:1:51"), subs);
    }

    @Test
    void plainTableUsesBucketSubscribe() throws Exception {
        List<String> subs = new ArrayList<>();
        TickTableViewer.subscribeFromLatest(
                fakeAdmin(List.of()),
                recordingScanner(subs),
                TablePath.of("default", "raw_table_1"),
                List.of(0, 1),
                false);
        assertEquals(List.of("plain:0:50", "plain:1:51"), subs);
    }

    @Test
    void adminWaitsAreBounded() {
        assertTrue(
                TickTableViewer.ADMIN_TIMEOUT.toSeconds() <= 10,
                "admin waits must be bounded — an untimed get() hangs forever on stall");
    }
}
