// SafetyLiveJobRun — host runner for SafetyHaltJob live consume path 2026-08-18 (Item E)
// See logs/tracker-14/safety-live-job-run-20260818.md + CHG-026; in-process MiniCluster, topology identical to SafetyHaltJob.main
// 2026-09-14 (P6-690): SafetyHaltJob.main reads its wiring from the environment only
// (FLUSS_BOOTSTRAP_SERVERS, FLUSS_DATABASE, FLUSS_TABLE, SAFETY_CHECKPOINT_INTERVAL_MS) and never
// inspects argv, so this runner passes no arguments — export the vars before running, e.g.
// FLUSS_BOOTSTRAP_SERVERS=localhost:9123, CHECKPOINTS_DIRECTORY=file:///tmp/safetyhalt-checkpoints
// and OffsetsInitializer.full() for a one-off full replay. The earlier argv was ignored then too,
// so the 2026-08-18 run recorded in the companion log is unaffected by this correction.
import org.apache.flink.runtime.minicluster.MiniCluster;
import com.trading.compute.safetyhalt.SafetyHaltJob;
public class SafetyLiveJobRun {
  public static void main(String[] args) throws Exception {
    // Reconstructs SafetyHaltJob topology exactly as SafetyHaltJob.main builds it; every knob
    // comes from the environment (see the header).
    SafetyHaltJob.main(new String[0]);
  }
}
