#!/usr/bin/env python3
# =============================================================================
# o2-provision.py — OpenObserve phase-structure provisioning (idempotent)
#
# Creates, in org `default`:
#   - INGESTION folder (via dashboards carrying folder="INGESTION")
#   - 4 dashboards: Overview, Slots, Resources, Quality
#   - 43 alert rules (18 ING- ingestion incl. the 11-rule 02-ingestion-alerting
#     contract + 16 SIGNAL- + 9 INFRA- infra/JVM/host; CHG-093)
#     (SIGNAL includes P8.3 15 + SIGNAL-crit-schema 1; INFRA 9: host CPU 80/90, JVM heap 85, GC 500ms,
#     disk 20%, disk IO 20ms, net 80%, O2 mem 14GB, collector export failed)
#   - one dev webhook destination ("dev-webhook" -> localhost:9999 in O2's
#     netns, served by the compose alert-consumer; replace with the real
#     delivery endpoint when alert routing is approved)
#
# Naming contract (user-approved Option A, 2026-08-08): one org, folders per
# phase, ING-/SIGNAL-/EXECUTOR- prefixes. Future phases add their own
# dashboards (folder=...) and alerts (prefix=...) — no structural change.
#
# Usage: O2_AUTH_BASIC=<base64 user:pass> python3 o2-provision.py [base_url]
#   base_url defaults to http://localhost:5080
# Stdlib only. Idempotent: existing dashboards/alerts are left untouched.
# =============================================================================
import json
import os
import sys
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5080"
ORG = "default"
AUTH = os.environ.get("O2_AUTH_BASIC", "")
if not AUTH:
    print("ERROR: O2_AUTH_BASIC env var required (base64 of user:password)")
    sys.exit(2)

HEADERS = {
    "Authorization": f"Basic {AUTH}",
    "Content-Type": "application/json",
}

METRIC_TYPES = {
    "bridge_slot_capacity_used_percent": "gauge",
    "bridge_slot_capacity_remaining": "gauge",
    "bridge_slot_safety_state": "gauge",
    "bridge_slot_unsafe_duration_ms": "gauge",
    "bridge_reconnect_consecutive": "gauge",
    "bridge_active_sockets": "gauge",
    "bridge_child_process_alive": "gauge",
    "process_open_fds": "gauge",
    "process_fd_limit": "gauge",
    "process_fd_usage_percent": "gauge",
    "process_rss_bytes": "gauge",
    "go_goroutines": "gauge",
    "jvm_threads_live": "gauge",
    "decode_errors_by_reason": "counter",
    "append_latency_ms": "histogram",
    "otelcol_exporter_send_failed_metric_points": "counter",
    # CHG-023 item 1 (2026-08-17): the schema-version rejection counter now
    # reaches O2 via the native flink-metrics-otel reporter as the Flink
    # MetricGroup series below (same flink_taskmanager_job_task_operator_*
    # shape as the dashboards already query). The hand-emitted
    # compute_invalid_byreason_schema_version stream is dead — retained as the
    # historical entry.
    "compute_invalid_byreason_schema_version": "counter",
    "flink_taskmanager_job_task_operator_compute_invalid_byreason_schema-version": "counter",
}


# OpenObserve renames metric streams: '.' -> '_' (verified 2026-08-08:
# bridge.slot.capacity_used_percent lands as bridge_slot_capacity_used_percent).
def stream(metric_name):
    return metric_name.replace(".", "_")


DASHBOARDS = [
    {
        "title": "INGESTION - Overview",
        "description": "Single-screen health: slot capacity, safety, resources, delivery.",
        "panels": [
            (
                "Slot capacity (all slots)",
                "gauge",
                "select value from \"bridge_slot_capacity_used_percent\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp desc limit 1",
                "bridge_slot_capacity_used_percent",
            ),
            (
                "Unsafe duration",
                "gauge",
                "select value from \"bridge_slot_unsafe_duration_ms\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp desc limit 1",
                "bridge_slot_unsafe_duration_ms",
            ),
            (
                "Reconnect streak",
                "gauge",
                "select value from \"bridge_reconnect_consecutive\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp desc limit 1",
                "bridge_reconnect_consecutive",
            ),
            (
                "FD usage %",
                "gauge",
                "select value from \"process_fd_usage_percent\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp desc limit 1",
                "process_fd_usage_percent",
            ),
            (
                "Capacity % timeline",
                "timeseries",
                "select _timestamp, value from \"bridge_slot_capacity_used_percent\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "bridge_slot_capacity_used_percent",
            ),
            (
                "Telemetry delivery failures",
                "timeseries",
                "select _timestamp, value from \"otelcol_exporter_send_failed_metric_points\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "otelcol_exporter_send_failed_metric_points",
            ),
        ],
    },
    {
        "title": "INGESTION - Slots",
        "description": "Per-slot capacity, safety state, active sockets.",
        "panels": [
            (
                "Capacity % per slot",
                "timeseries",
                "select _timestamp, value from \"bridge_slot_capacity_used_percent\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "bridge_slot_capacity_used_percent",
            ),
            (
                "Capacity remaining",
                "gauge",
                "select value from \"bridge_slot_capacity_remaining\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp desc limit 1",
                "bridge_slot_capacity_remaining",
            ),
            (
                "Safety state (1=safe 0=unsafe)",
                "timeseries",
                "select _timestamp, value from \"bridge_slot_safety_state\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "bridge_slot_safety_state",
            ),
            (
                "Unsafe duration ms",
                "timeseries",
                "select _timestamp, value from \"bridge_slot_unsafe_duration_ms\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "bridge_slot_unsafe_duration_ms",
            ),
            (
                "Active sockets",
                "gauge",
                "select value from \"bridge_active_sockets\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp desc limit 1",
                "bridge_active_sockets",
            ),
        ],
    },
    {
        "title": "INGESTION - Resources",
        "description": "FD/RSS/goroutines/threads — leak detection surface.",
        "panels": [
            (
                "Open FDs",
                "timeseries",
                "select _timestamp, value from \"process_open_fds\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "process_open_fds",
            ),
            (
                "FD limit",
                "gauge",
                "select value from \"process_fd_limit\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp desc limit 1",
                "process_fd_limit",
            ),
            (
                "FD usage %",
                "timeseries",
                "select _timestamp, value from \"process_fd_usage_percent\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "process_fd_usage_percent",
            ),
            (
                "RSS bytes",
                "timeseries",
                "select _timestamp, value from \"process_rss_bytes\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "process_rss_bytes",
            ),
            (
                "Go goroutines",
                "timeseries",
                "select _timestamp, value from \"go_goroutines\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "go_goroutines",
            ),
            (
                "JVM live threads",
                "timeseries",
                "select _timestamp, value from \"jvm_threads_live\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "jvm_threads_live",
            ),
        ],
    },
    {
        "title": "INGESTION - Quality",
        "description": "Decode errors by reason, append latency, orphan detection.",
        "panels": [
            (
                "Decode errors by reason (count)",
                "table",
                "select sum(value) as total from \"decode_errors_by_reason\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' group by reason order by total desc",
                "decode_errors_by_reason",
            ),
            (
                "Child process alive (0=orphan)",
                "gauge",
                "select value from \"bridge_child_process_alive\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp desc limit 1",
                "bridge_child_process_alive",
            ),
            (
                "Append latency p95 (ms)",
                "promql",
                "histogram_quantile(0.95, sum by (le) (rate(append_latency_ms_bucket[5m])))",
                "append_latency_ms",
            ),
        ],
    },
    # ---- P8.4 COMPUTE dashboards (tracker 14; metric names verified live
    # 2026-08-11 via the O2 PromQL label/__name__ values + P8.1 battery).
    # Panels use PromQL for filtered/aggregated series (label matchers) and SQL
    # for plain stream panels; all names are lowercase O2 stream names.
    {
        "title": "COMPUTE - SignalJob Overview",
        "description": "SignalJob health: startup mode, throughput, validation, dedup, candles, candidates, watermark, checkpoints, memory, backpressure (tracker 14 P8.4).",
        "folder": "COMPUTE",
        "panels": [
            (
                "Startup mode (1=FULL_REPLAY 0=RESTORE)",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_startup_mode)",
                "flink_taskmanager_job_task_operator_compute_startup_mode",
            ),
            (
                "Running jobs",
                "promql",
                "max(flink_jobmanager_numrunningjobs)",
                "flink_jobmanager_numrunningjobs",
            ),
            (
                "Throughput by operator (records/s)",
                "promql",
                "sum by (task_name) (flink_taskmanager_job_task_numrecordsinpersecond)",
                "flink_taskmanager_job_task_numrecordsinpersecond",
            ),
            (
                "Source rate (records/s)",
                "promql",
                'max(flink_taskmanager_job_task_numrecordsinpersecond{task_name="Source:_raw_table_1____raw_validation"})',
                "flink_taskmanager_job_task_numrecordsinpersecond",
            ),
            (
                "Candles emitted",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_candles_emitted)",
                "flink_taskmanager_job_task_operator_compute_candles_emitted",
            ),
            (
                "Candles late updates",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_candles_late_updates)",
                "flink_taskmanager_job_task_operator_compute_candles_late_updates",
            ),
            (
                "Signals detected",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_signals_detected)",
                "flink_taskmanager_job_task_operator_compute_signals_detected",
            ),
            (
                "Dedup duplicates",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_dedup_duplicates)",
                "flink_taskmanager_job_task_operator_compute_dedup_duplicates",
            ),
            (
                "Dedup state count",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_job_task_operator_compute_dedup_state_count\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_job_task_operator_compute_dedup_state_count",
            ),
            (
                "Invalid rows (validation)",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_invalid_rows)",
                "flink_taskmanager_job_task_operator_compute_invalid_rows",
            ),
            (
                "Input watermark (ms)",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_job_task_operator_currentinputwatermark\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_job_task_operator_currentinputwatermark",
            ),
            (
                "Checkpoint duration (ms)",
                "timeseries",
                "select _timestamp, value from \"flink_jobmanager_job_lastcheckpointduration\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_jobmanager_job_lastcheckpointduration",
            ),
            (
                "JM heap used (bytes)",
                "timeseries",
                "select _timestamp, value from \"flink_jobmanager_status_jvm_memory_heap_used\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_jobmanager_status_jvm_memory_heap_used",
            ),
            (
                "Backpressure time (ms/s)",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_job_task_backpressuredtimemspersecond\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_job_task_backpressuredtimemspersecond",
            ),
        ],
    },
    {
        "title": "COMPUTE - Candle Health",
        "description": "Candle KV sink health (user requirement 2026-08-13: candle tables are KV-only, no LOG+KV twin — the LOG-vs-KV divergence view was removed with the KV twin). Upserts = feature_candles_15s sink numRecordsIn (one upsert per closed window).",
        "folder": "COMPUTE",
        "panels": [
            (
                "Candle sink upserts (total)",
                "promql",
                'max(flink_taskmanager_job_task_numrecordsin{task_name="feature_candles_15s_sink:_Writer"})',
                "flink_taskmanager_job_task_numrecordsin",
            ),
            (
                "Candle sink rate (upserts/s)",
                "promql",
                'max(flink_taskmanager_job_task_numrecordsinpersecond{task_name="feature_candles_15s_sink:_Writer"})',
                "flink_taskmanager_job_task_numrecordsinpersecond",
            ),
            (
                "Candles emitted (total)",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_candles_emitted)",
                "flink_taskmanager_job_task_operator_compute_candles_emitted",
            ),
            (
                "Candles late updates",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_candles_late_updates)",
                "flink_taskmanager_job_task_operator_compute_candles_late_updates",
            ),
            (
                "KV filter noncanonical (emitter)",
                "promql",
                "max(compute_kv_filtered_noncanonical)",
                "compute_kv_filtered_noncanonical",
            ),
        ],
    },
    {
        "title": "COMPUTE - Checkpoints & State",
        "description": "Checkpoint duration/size/failures/restarts, dedup state, memory envelopes (tracker 14 P8.4). State-backend/checkpoint-URI details are in the job config log (signal-job: effective state backend = ...) — dev uses heap/HashMap + /tmp; production is RocksDB + S3 (P4 gate).",
        "folder": "COMPUTE",
        "panels": [
            (
                "Checkpoint duration (ms)",
                "timeseries",
                "select _timestamp, value from \"flink_jobmanager_job_lastcheckpointduration\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_jobmanager_job_lastcheckpointduration",
            ),
            (
                "Checkpoint size (bytes)",
                "timeseries",
                "select _timestamp, value from \"flink_jobmanager_job_lastcheckpointsize\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_jobmanager_job_lastcheckpointsize",
            ),
            (
                "Checkpoint full size (bytes)",
                "timeseries",
                "select _timestamp, value from \"flink_jobmanager_job_lastcheckpointfullsize\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_jobmanager_job_lastcheckpointfullsize",
            ),
            (
                "Persisted data (bytes)",
                "timeseries",
                "select _timestamp, value from \"flink_jobmanager_job_lastcheckpointpersisteddata\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_jobmanager_job_lastcheckpointpersisteddata",
            ),
            (
                "Completed checkpoints",
                "promql",
                "max(flink_jobmanager_job_numberofcompletedcheckpoints)",
                "flink_jobmanager_job_numberofcompletedcheckpoints",
            ),
            (
                "Failed checkpoints",
                "promql",
                "max(flink_jobmanager_job_numberoffailedcheckpoints)",
                "flink_jobmanager_job_numberoffailedcheckpoints",
            ),
            (
                "Restarts",
                "promql",
                "max(flink_jobmanager_job_numrestarts)",
                "flink_jobmanager_job_numrestarts",
            ),
            (
                "Restarting time (ms)",
                "promql",
                "max(flink_jobmanager_job_restartingtime)",
                "flink_jobmanager_job_restartingtime",
            ),
            (
                "Dedup state count",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_job_task_operator_compute_dedup_state_count\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_job_task_operator_compute_dedup_state_count",
            ),
            (
                "Dedup state bytes estimate",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_job_task_operator_compute_dedup_state_bytes_estimate\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_job_task_operator_compute_dedup_state_bytes_estimate",
            ),
            (
                "JM heap used (bytes)",
                "timeseries",
                "select _timestamp, value from \"flink_jobmanager_status_jvm_memory_heap_used\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_jobmanager_status_jvm_memory_heap_used",
            ),
            (
                "TM metaspace used (bytes)",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_status_jvm_memory_metaspace_used\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_status_jvm_memory_metaspace_used",
            ),
            (
                "TM direct memory used (bytes)",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_status_jvm_memory_direct_memoryused\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_status_jvm_memory_direct_memoryused",
            ),
        ],
    },
    {
        "title": "COMPUTE - Flink & Fluss Cluster",
        "description": "Cluster health, operator throughput/watermark/backpressure, Fluss client health, telemetry delivery (tracker 14 P8.4). Fluss tablet/coordinator processes are not directly scraped; their health is proxied by the Fluss client metrics below + Fluss server logs.",
        "folder": "COMPUTE",
        "panels": [
            (
                "Running jobs",
                "promql",
                "max(flink_jobmanager_numrunningjobs)",
                "flink_jobmanager_numrunningjobs",
            ),
            (
                "Registered TaskManagers",
                "promql",
                "max(flink_jobmanager_numregisteredtaskmanagers)",
                "flink_jobmanager_numregisteredtaskmanagers",
            ),
            ("Scrape targets up", "promql", "up", "up"),
            (
                "Scrape duration (s)",
                "timeseries",
                "select _timestamp, value from \"scrape_duration_seconds\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "scrape_duration_seconds",
            ),
            (
                "Throughput by operator (records/s)",
                "promql",
                "sum by (task_name) (flink_taskmanager_job_task_numrecordsinpersecond)",
                "flink_taskmanager_job_task_numrecordsinpersecond",
            ),
            (
                "Input watermark by operator (ms)",
                "promql",
                "max by (task_name) (flink_taskmanager_job_task_operator_currentinputwatermark)",
                "flink_taskmanager_job_task_operator_currentinputwatermark",
            ),
            (
                "Backpressure by operator (ms/s)",
                "timeseries",
                "sum by (task_name) (flink_taskmanager_job_task_backpressuredtimemspersecond)",
                "flink_taskmanager_job_task_backpressuredtimemspersecond",
            ),
            (
                "Busy time by operator (ms/s)",
                "timeseries",
                "sum by (task_name) (flink_taskmanager_job_task_busytimemspersecond)",
                "flink_taskmanager_job_task_busytimemspersecond",
            ),
            (
                "Fluss reader current offset (source lag proxy)",
                "promql",
                "max(flink_taskmanager_job_task_operator_fluss_reader_bucket_currentoffset)",
                "flink_taskmanager_job_task_operator_fluss_reader_bucket_currentoffset",
            ),
            (
                "Operator watermark lag (ms)",
                "promql",
                "max(flink_taskmanager_job_task_operator_watermarklag)",
                "flink_taskmanager_job_task_operator_watermarklag",
            ),
            (
                "Fluss client request latency avg (ms)",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_job_task_operator_fluss_client_client_id_requestlatencyms_avg\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_job_task_operator_fluss_client_client_id_requestlatencyms_avg",
            ),
            (
                "Fluss writer records sent/s",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_job_task_operator_fluss_client_writer_client_id_recordsendpersecond\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_job_task_operator_fluss_client_writer_client_id_recordsendpersecond",
            ),
            (
                "Fluss writer send latency (ms)",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_job_task_operator_fluss_client_writer_client_id_sendlatencyms\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_job_task_operator_fluss_client_writer_client_id_sendlatencyms",
            ),
            (
                "Fluss client records retried/s",
                "timeseries",
                "select _timestamp, value from \"flink_taskmanager_job_task_operator_fluss_client_writer_client_id_recordsretrypersecond\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "flink_taskmanager_job_task_operator_fluss_client_writer_client_id_recordsretrypersecond",
            ),
            (
                "Collector sent metric points",
                "timeseries",
                "select _timestamp, value from \"otelcol_exporter_sent_metric_points\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "otelcol_exporter_sent_metric_points",
            ),
            (
                "Collector accepted metric points",
                "timeseries",
                "select _timestamp, value from \"otelcol_receiver_accepted_metric_points\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "otelcol_receiver_accepted_metric_points",
            ),
            (
                "Collector send failed (0 = healthy)",
                "promql",
                "max(otelcol_exporter_send_failed_metric_points)",
                "otelcol_exporter_send_failed_metric_points",
            ),
        ],
    },
    {
        # 2026-09-05 Phase 5 multi-TF side-by-side: the one screen that
        # answers "is the NEW branch alive and agreeing with the old chain?"
        # Every series is registered by the Phase 2-4 operators and flows on
        # the existing Flink->Prometheus->remote-write path (verified live
        # 2026-09-04: post_close 3.3M -> 11.4M visible in O2 query_range;
        # task_name labels multi_tf_aggregator / *_sink:_Writer confirmed).
        "title": "COMPUTE - Multi-Timeframe",
        "description": "Multi-TF side-by-side branch health (Phase 5): branch throughput old-vs-new, aggregator input, session-filter drops, closed/live sink rates, signal emitted vs suppressed (design doc docs/plans/2026-09-05-multitimeframe-candle-aggregator-design.md).",
        "folder": "COMPUTE",
        "panels": [
            (
                "Branch throughput old vs new (records/s)",
                "promql",
                'sum by (task_name) (flink_taskmanager_job_task_numrecordsinpersecond{task_name=~"multi_tf_aggregator|candle_closed_first_write_wins|candle_15s_____candle_late_drop_counter__candle_invalid_quarantine_"})',
                "flink_taskmanager_job_task_numrecordsinpersecond",
            ),
            (
                "Aggregator input vs old-chain candles (records/s)",
                "promql",
                'sum(flink_taskmanager_job_task_numrecordsinpersecond{task_name="multi_tf_aggregator"}) / sum(flink_taskmanager_job_task_numrecordsinpersecond{task_name="Source:_raw_table_1____raw_validation"})',
                "flink_taskmanager_job_task_numrecordsinpersecond",
            ),
            (
                "Session-filtered drops (post-close, cumulative)",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_session_filtered_post_close)",
                "flink_taskmanager_job_task_operator_compute_session_filtered_post_close",
            ),
            (
                "Session-filtered drops (pre-open, cumulative)",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_session_filtered_pre_open)",
                "flink_taskmanager_job_task_operator_compute_session_filtered_pre_open",
            ),
            (
                "New closed-sink rate (upserts/s)",
                "promql",
                'max(flink_taskmanager_job_task_numrecordsinpersecond{task_name="candle_closed_sink:_Writer"})',
                "flink_taskmanager_job_task_numrecordsinpersecond",
            ),
            (
                "New live-sink rate (upserts/s)",
                "promql",
                'max(flink_taskmanager_job_task_numrecordsinpersecond{task_name="candle_live_sink:_Writer"})',
                "flink_taskmanager_job_task_numrecordsinpersecond",
            ),
            (
                "Old closed-sink rate (upserts/s)",
                "promql",
                'max(flink_taskmanager_job_task_numrecordsinpersecond{task_name="feature_candles_15s_sink:_Writer"})',
                "flink_taskmanager_job_task_numrecordsinpersecond",
            ),
            (
                "Multi-TF signals emitted (cumulative)",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_multitf_signal_emitted)",
                "flink_taskmanager_job_task_operator_compute_multitf_signal_emitted",
            ),
            (
                "Multi-TF signals suppressed by latch (cumulative)",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_multitf_signal_suppressed)",
                "flink_taskmanager_job_task_operator_compute_multitf_signal_suppressed",
            ),
            (
                "New signal-sink rate (rows/s)",
                "promql",
                'max(flink_taskmanager_job_task_numrecordsinpersecond{task_name="multitf_signal_candidates_sink:_Writer"})',
                "flink_taskmanager_job_task_numrecordsinpersecond",
            ),
            (
                "Duplicate-window guard hits (cumulative)",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_candles_multitf_duplicate_window)",
                "flink_taskmanager_job_task_operator_compute_candles_multitf_duplicate_window",
            ),
        ],
    },
    {
        "title": "COMPUTE - Quality",
        "description": "Validation quality, dedup efficiency, source health, collector delivery (tracker 14 P8.4, mirrors the INGESTION - Quality convention).",
        "folder": "COMPUTE",
        "panels": [
            (
                "Invalid rows (validation)",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_invalid_rows)",
                "flink_taskmanager_job_task_operator_compute_invalid_rows",
            ),
            (
                "Schema-version rejects (emitter)",
                "timeseries",
                "select _timestamp, value from \"compute_invalid_byreason_schema_version\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "compute_invalid_byreason_schema_version",
            ),
            (
                "KV filter noncanonical (emitter)",
                "timeseries",
                "select _timestamp, value from \"compute_kv_filtered_noncanonical\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "compute_kv_filtered_noncanonical",
            ),
            (
                "Dedup duplicates (suppressed rows)",
                "promql",
                "max(flink_taskmanager_job_task_operator_compute_dedup_duplicates)",
                "flink_taskmanager_job_task_operator_compute_dedup_duplicates",
            ),
            (
                "Late records dropped",
                "promql",
                "max(flink_taskmanager_job_task_operator_numlaterecordsdropped)",
                "flink_taskmanager_job_task_operator_numlaterecordsdropped",
            ),
            (
                "Source idle time (ms)",
                "promql",
                "max(flink_taskmanager_job_task_operator_sourceidletime)",
                "flink_taskmanager_job_task_operator_sourceidletime",
            ),
            (
                "Collector send failed (0 = healthy)",
                "timeseries",
                "select _timestamp, value from \"otelcol_exporter_send_failed_metric_points\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "otelcol_exporter_send_failed_metric_points",
            ),
            (
                "Collector uptime (s)",
                "promql",
                "max(otelcol_process_uptime)",
                "otelcol_process_uptime",
            ),
            (
                "Collector heap alloc (bytes)",
                "timeseries",
                "select _timestamp, value from \"otelcol_process_runtime_heap_alloc_bytes\" where _timestamp >= '{start_time}' and _timestamp <= '{end_time}' order by _timestamp",
                "otelcol_process_runtime_heap_alloc_bytes",
            ),
        ],
    },
]

# Alert rules. Each entry is a dict:
#   name, stream, desc, conditions=[(column, operator, value), ...] (ANDed),
#   period (minutes), frequency (minutes; 0 = real-time continuous), threshold,
#   destinations.
# Severity rides the name prefix + description (O2 v0.91.5 v2 alerts have NO
# first-class severity field — verified on the live API 2026-08-11).
ALERTS = [
    # --- Ingestion phase (ING- prefix) ---
    dict(
        name="ING-warn-capacity-80",
        stream="bridge_slot_capacity_used_percent",
        conditions=[("value", ">=", 80)],
        desc="Slot capacity >= 80%: rotation/eviction pressure building",
    ),
    dict(
        name="ING-warn-fd-80",
        stream="process_fd_usage_percent",
        conditions=[("value", ">=", 80)],
        desc="FD usage >= 80%: approaching the halting threshold",
    ),
    dict(
        name="ING-crit-reconnect-consecutive",
        stream="bridge_reconnect_consecutive",
        conditions=[("value", ">=", 5)],
        desc="[Critical/ingestion][contract #4] Consecutive reconnects >= 5: broker link unstable",
    ),
    dict(
        name="ING-crit-fd-90",
        stream="process_fd_usage_percent",
        conditions=[("value", ">=", 90)],
        desc="FD usage >= 90%: programmed safety halt fires at this level",
    ),
    dict(
        name="ING-crit-orphan-process",
        stream="bridge_child_process_alive",
        conditions=[("value", "=", 0)],
        desc="Child process dead but bridge alive: orphaned supervision",
    ),
    dict(
        name="ING-crit-unsafe-duration-30s",
        stream="bridge_slot_unsafe_duration_ms",
        conditions=[("value", ">=", 30000)],
        desc="Slot unsafe for >= 30s: sustained unsafe window",
    ),
    dict(
        name="ING-crit-capacity-over-100",
        stream="bridge_slot_capacity_used_percent",
        conditions=[("value", ">", 100)],
        desc="Capacity > 100%: mathematically unreachable (assigned <= 1024) — kept for parity with the documented rule",
    ),
    dict(
        name="ING-crit-telemetry-delivery-failed",
        stream="otelcol_exporter_send_failed_metric_points",
        conditions=[("value", ">", 0)],
        desc="Collector failed to deliver metrics to OpenObserve: data gap started",
    ),
    # --- Contract reconciliation (docs/06_operations/02-ingestion-alerting.md
    # rule table; wired live 2026-08-24). The 11 pinned ingestion-plane alerts,
    # exact thresholds from the doc. Slot metrics are per-slot gauges; counters
    # (bridge.reconnects, decode.errors, heartbeat.failures) are cumulative
    # (AGGREGATION_TEMPORALITY_CUMULATIVE in OtlpMetricsEmitter), so rate
    # windows use increase().
    dict(
        name="ING-warn-capacity-90",
        stream="bridge_slot_capacity_used_percent",
        conditions=[("value", ">=", 90)],
        period=5,
        desc="[Warning/ingestion][contract #1] Slot capacity >= 90% for 5 min: subscription headroom low",
    ),
    dict(
        name="ING-crit-capacity-98",
        stream="bridge_slot_capacity_used_percent",
        conditions=[("value", ">=", 98)],
        period=2,
        desc="[Critical/ingestion][contract #2] Slot capacity >= 98% for 2 min: subscription headroom critical",
    ),
    dict(
        name="ING-crit-reconnect-storm",
        stream="bridge_reconnects",
        promql="sum(increase(bridge_reconnects[600s]))",
        promql_condition=(">=", 5),
        period=1,
        desc="[Critical/ingestion][contract #3] >= 5 reconnects / 10 min: reconnect storm",
    ),
    dict(
        name="ING-crit-stale-feed",
        stream="bridge_slot_last_frame_age_ms",
        conditions=[("value", ">=", 5000)],
        period=1,
        desc="[Critical/ingestion][contract #5] Last-frame age >= 5000 ms (freshness limit) for 1 min: stale feed",
    ),
    dict(
        name="ING-crit-decode-error-burst",
        stream="decode_errors",
        promql="sum(increase(decode_errors[70s]))",
        promql_condition=(">=", 100),
        period=1,
        desc="[Critical/ingestion][contract #6] Decode errors >= 100 within ~1 min (doc pins 100/10 s; 70 s range guarantees counter samples across the 10 s flush): decode error burst",
    ),
    dict(
        name="ING-warn-partial-subscription",
        stream="bridge_slot_rejected",
        conditions=[("value", ">", 0)],
        period=5,
        desc="[Warning/ingestion][contract #7] bridge.slot.rejected > 0 for 5 min: partial subscription",
    ),
    dict(
        name="ING-crit-not-ready",
        stream="ingestion_ready",
        conditions=[("value", "=", 0)],
        period=2,
        desc="[Critical/ingestion][contract #8] ingestion.ready == 0 for 2 min: pipeline not ready",
    ),
    dict(
        name="ING-crit-bridge-disconnected",
        stream="bridge_connected",
        conditions=[("value", "=", 0)],
        period=1,
        desc="[Critical/ingestion][contract #9] bridge.connected == 0 for 1 min: bridge disconnected",
    ),
    dict(
        name="ING-warn-heartbeat-failures",
        stream="heartbeat_failures",
        promql="sum(increase(heartbeat_failures[300s]))",
        promql_condition=(">=", 3),
        period=1,
        desc="[Warning/ingestion][contract #10] Heartbeat failures >= 3 / 5 min",
    ),
    dict(
        name="ING-warn-otlp-collector-unhealthy",
        stream="otel_collector_healthy",
        conditions=[("value", "=", 0)],
        period=10,
        desc="[Warning/ingestion][contract #11] otel.collector.healthy == 0 for 10 min: OTLP delivery degraded",
    ),
    # Compute phase (SIGNAL- prefix): schema-version drift between the raw-tick
    # producer (ingestion) and the compute gate. Emitter sends DELTA per 10 s
    # flush (non-monotonic), so value > 0 = NEW rejections in the window —
    # a historical replay of legacy rows never re-fires this alert.
    dict(
        name="SIGNAL-crit-schema-version-rejected",
        stream="flink_taskmanager_job_task_operator_compute_invalid_byreason_schema_version",
        conditions=[("value", ">", 0)],
        desc="Compute gate rejected raw rows on schema_version: producer/consumer version drift",
    ),
    # --- P8.3 SignalJob/Flink/collector rules (tracker 14, approved 2026-08-11) ---
    dict(
        name="SIGNAL-crit-checkpoint-failed",
        stream="flink_jobmanager_job_numberoffailedcheckpoints",
        conditions=[("value", ">", 0)],
        desc="[Critical/compute] A Flink checkpoint FAILED: recovery may be required; inspect flink_logs + job restart metrics; recovery = next successful checkpoint",
    ),
    dict(
        name="SIGNAL-error-checkpoint-slow",
        stream="flink_jobmanager_job_lastcheckpointduration",
        conditions=[("value", ">=", 240000)],
        desc="[Error/compute] Checkpoint duration >= 240000 ms = 80% of pinned CHECKPOINT_TIMEOUT_MS=300000: timeout risk; recovery = fast checkpoint",
    ),
    dict(
        name="SIGNAL-error-job-restarting",
        stream="flink_jobmanager_job_numrestarts",
        conditions=[("value", ">", 0)],
        desc="[Error/compute] Job has restarted (numRestarts > 0): investigate trigger in flink_logs; recovery = job re-enters RUNNING",
    ),
    dict(
        name="SIGNAL-error-source-stalled",
        stream="flink_taskmanager_job_task_numrecordsinpersecond",
        conditions=[
            ("task_name", "=", "Source:_raw_table_1____raw_validation"),
            ("value", "=", 0),
        ],
        period=2,
        desc="[Error/compute] Source task consuming 0 records/s for 2 min while the job runs: ingestion/feed stall; recovery = next records > 0 (quiesced dev feeds false-fire by design)",
    ),
    dict(
        name="SIGNAL-warn-candle-sink-zero",
        stream="flink_taskmanager_job_task_numrecordsinpersecond",
        conditions=[
            ("task_name", "=", "feature_candles_15s_sink:_Writer"),
            ("value", "=", 0),
        ],
        period=2,
        desc="[Warning/compute] Candle KV sink writing 0 records/s for 2 min while running: window-close stall / watermark freeze; recovery = sink resumes (quiesced dev feeds false-fire by design)",
    ),
    dict(
        name="SIGNAL-warn-dedup-state",
        stream="flink_jobmanager_job_lastcheckpointsize",
        # 2026-08-28 (gauge remediation): repointed from the broken
        # compute_dedup_state_count gauge (cumulative firsts, ~39x over-report;
        # MapState.entries() does NOT filter TTL-expired in RocksDB — see
        # logs/tracker-14/dedup-ttl-diagnosis-20260828.md) to the TRUE live
        # state: the job's last checkpoint size. Bounded at ~rate x TTL
        # (observed ~16-18 MB total). Threshold 1.5 GB = the checkpoint size
        # when live state approaches the 6 GB TM budget minus other operators,
        # with margin. Job-level gauge — no subtask aggregation needed.
        conditions=[("value", ">", 1500000000)],
        period=1,
        desc="[Warning/compute] TOTAL dedup+compute state (last checkpoint size) > 1.5 GB: state approaching the 6 GB TM budget; checkpoint timeout risk; recovery = check accepted-rate/TTL math vs envelope, consider restore/balance",
    ),
    dict(
        name="SIGNAL-warn-dedup-firsts-rate",
        stream="flink_taskmanager_job_task_operator_compute_dedup_first",
        # 2026-08-28 (gauge remediation): the cumulative firsts counter is a
        # valid INPUT-rate signal (dedup throughput), not state size. A
        # sustained firsts rate above the real-broker max (48.7k/s) means the
        # feed is beyond the envelope — state grows faster than the 60s TTL
        # can drain it. 300s range averages out per-flush spikes.
        promql="sum(increase(flink_taskmanager_job_task_operator_compute_dedup_first[300s]))/300",
        promql_condition=(">", 50000),
        period=1,
        desc="[Warning/compute] Dedup firsts (unique fingerprints) sustained > 50k/s over 5 min: input beyond the real-broker envelope (48.7k/s); state grows faster than TTL drains; recovery = check feed rate vs envelope",
    ),
    # RETIRED 2026-08-17 (CHG-023 item 2): SIGNAL-warn-dedup-expiry watched
    # compute_dedup_expiry_index_count — the expiry-index gauge is DELETED with
    # the index (expiry is native StateTtlConfig on the MapState now; no
    # event-time timers to stall). SIGNAL-warn-dedup-state above covers the
    # same Design-B envelope on the live-set count (compute_dedup_state_count).
    dict(
        name="SIGNAL-warn-schema-rejected-rate",
        stream="flink_taskmanager_job_task_operator_compute_invalid_byreason_schema_version",
        conditions=[("value", ">", 10)],
        desc="[Warning/compute] >10 schema_version rejections in one 10s flush: sustained producer/consumer drift; recovery = producer version aligned",
    ),
    dict(
        name="SIGNAL-crit-full-replay-started",
        stream="flink_taskmanager_job_task_operator_compute_startup_mode",
        conditions=[("value", "=", 1)],
        desc="[Critical/compute] Startup mode = FULL_REPLAY (offset-0): operator must acknowledge; recovery = RESTORE-mode restart (A3.3/A3.4). Series = SignalJob RawValidationFunction Flink gauge (P8.1 box 850 — distributed reporter path; the client-side emitter gauge dies with the submitting JVM)",
    ),
    dict(
        name="SIGNAL-error-flink-jm-scrape-down",
        stream="up",
        conditions=[("instance", "=", "flink-jobmanager:9249"), ("value", "=", 0)],
        period=2,
        desc="[Error/ops] JobManager Prometheus scrape down for 2 min: JM metrics absent; recovery = reporter endpoint returns",
    ),
    dict(
        name="SIGNAL-error-flink-tm-scrape-down",
        stream="up",
        conditions=[("instance", "=", "flink-taskmanager:9249"), ("value", "=", 0)],
        period=2,
        desc="[Error/ops] TaskManager Prometheus scrape down for 2 min: TM metrics absent; recovery = reporter endpoint returns",
    ),
    dict(
        name="SIGNAL-warn-jvm-heap-high",
        stream="flink_jobmanager_status_jvm_memory_heap_used",
        conditions=[("value", ">=", 900000000)],
        desc="[Warning/compute] JM heap >= 900 MB (~0.85 x 1 GiB container max, verified 2026-08-11): GC pressure; recovery = GC/restart relieves",
    ),
    dict(
        name="SIGNAL-crit-taskmanager-down",
        stream="flink_jobmanager_numregisteredtaskmanagers",
        conditions=[("value", "<", 1)],
        desc="[Critical/ops] No TaskManagers registered: job cannot run; recovery = TM restarts, job rescales",
    ),
    dict(
        name="SIGNAL-warn-scrape-slow",
        stream="scrape_duration_seconds",
        conditions=[("value", ">=", 1.0)],
        desc="[Warning/ops] Prometheus scrape takes >= 1 s (of 15 s interval): reporter load; recovery = load drops",
    ),
    # Rule 15 (approved set): source lag. P8.3 originally documented it as NOT
    # expressible — the Fluss-connector/operator metrics are now live on the
    # cluster (2026-08-11 discovery: flink_taskmanager_job_task_operator_*),
    # so it is provisioned. Event-time lag = source staleness; live dev feed
    # baseline ~244 s (faketool replays historical timestamps), post-storm
    # quiesced feed 3.5M ms. Threshold 600 s sits above both the dev baseline
    # and the production live-market baseline (~0 s); fires on genuine long
    # stalls. Caveat: fires continuously while the dev feed is stopped
    # (feed-state-driven, same class as SIGNAL-error-source-stalled).
    dict(
        name="SIGNAL-warn-source-lag",
        stream="flink_taskmanager_job_task_operator_currentfetcheventtimelag",
        conditions=[("value", ">=", 600000)],
        period=2,
        desc="[Warning/ops] Event-time lag >= 600 s (10 min): source data stalled or replay far behind; recovery = live feed resumes / replay drains",
    ),
    # G6/G1 (levers map 2026-08-30): volume-drop monitor — the source-stalled
    # rule only fires at exactly 0 records/s; a partial feed degradation
    # (broker throttling, partial subscription) kept that rule silent.
    # Absolute floor at 50% of the design rate (10,240/s): sustained 5 min
    # below ~5k/s while the job runs. Same quiesced-feed caveat as
    # SIGNAL-error-source-stalled (fires by design when the feed is stopped).
    # The lever-map ideal (relative >50% drop vs rolling median) needs a
    # recording rule O2 v0.91.5 lacks; revisit on upgrade.
    dict(
        name="SIGNAL-warn-source-volume-drop",
        stream="flink_taskmanager_job_task_numrecordsinpersecond",
        conditions=[
            ("task_name", "=", "Source:_raw_table_1____raw_validation"),
            ("value", "<", 5000),
        ],
        period=5,
        desc="[Warning/compute] Source consuming < 5,000 rec/s (50% of design rate) for 5 min while the job runs: partial feed degradation (broker throttle / partial subscription); recovery = rate recovers (quiesced dev feeds false-fire by design)",
    ),
    # --- 2026-09-05 Phase 5 multi-TF side-by-side (design doc
    # docs/plans/2026-09-05-multitimeframe-candle-aggregator-design.md).
    # All three series are registered by the new operators
    # (MultiTimeframeAggregateFunction / MultiTimeframeSignalProducer) and
    # flow to O2 on the existing Flink->Prometheus->remote-write path with
    # no new plumbing. Verified live 2026-09-04 (soak-e2e-20260904-182216):
    # post_close rose 3.3M -> 11.4M in O2 while every tick was
    # session-filtered — the query path below is proven end to end. The
    # stall/suppression rules share the quiesced-feed caveat of the other
    # SIGNAL- throughput rules (they false-fire by design when the feed is
    # stopped or outside 09:15-15:30 IST).
    dict(
        name="SIGNAL-warn-multitf-session-drop",
        stream="flink_taskmanager_job_task_operator_compute_session_filtered_post_close",
        promql="sum(increase(flink_taskmanager_job_task_operator_compute_session_filtered_post_close[300s]))",
        promql_condition=(">", 10000),
        period=2,
        desc="[Warning/compute] Multi-TF aggregator session-dropping >10k ticks / 5 min over 2 evals: feed outside 09:15-15:30 IST (post-close soak) or clock skew while ticks flow; the new candle/signal legs are starved; recovery = in-session feed resumes",
    ),
    dict(
        name="SIGNAL-warn-multitf-branch-stall",
        stream="flink_taskmanager_job_task_numrecordsinpersecond",
        conditions=[
            ("task_name", "=", "multi_tf_aggregator"),
            ("value", "=", 0),
        ],
        period=5,
        desc="[Warning/compute] multi-tf-aggregator consuming 0 records/s for 5 min while the job runs: branch starved (session filter, upstream stall, or flag miswired); old chain unaffected; recovery = branch rate resumes (quiesced/off-session feeds false-fire by design)",
    ),
    dict(
        name="SIGNAL-warn-multitf-signal-suppressed",
        stream="flink_taskmanager_job_task_operator_compute_multitf_signal_suppressed",
        promql="sum(increase(flink_taskmanager_job_task_operator_compute_multitf_signal_suppressed[600s]))",
        promql_condition=(">", 1000),
        period=1,
        desc="[Warning/compute] Multi-TF fire-once latch suppressing >1000 candidate signals / 10 min: rule stuck true on forming candles or latch not clearing at boundaries; recovery = suppression rate falls",
    ),
    # --- 2026-08-22 single-pane: infra/JVM/host infra alerts (10-observability.md scale-up thresholds) ---
    dict(
        name="INFRA-warn-host-cpu-80",
        stream="node_cpu_seconds_total",
        promql="100 - (avg by (instance) (rate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)",
        promql_condition=(">=", 80),
        period=1,
        frequency=1,
        desc="[Warning/infra] Host CPU >80% for 60s per vm_id (node_exporter). Recovery = load drops. Scope=vm_id",
    ),
    dict(
        name="INFRA-crit-host-cpu-90",
        stream="node_cpu_seconds_total",
        promql="100 - (avg by (instance) (rate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)",
        promql_condition=(">=", 90),
        period=1,
        frequency=1,
        desc="[Critical/infra] Host CPU >90% for 60s — safe-halt candidate. Scope=vm_id",
    ),
    dict(
        name="INFRA-crit-jvm-heap-85",
        stream="flink_jobmanager_status_jvm_memory_heap_used",
        promql="max (flink_jobmanager_status_jvm_memory_heap_used / flink_jobmanager_status_jvm_memory_heap_max * 100)",
        promql_condition=(">=", 85),
        period=1,
        frequency=1,
        desc="[Critical/infra] JVM heap >85% per service (native Flink reporter flink_jobmanager_status_jvm_memory_heap_*; OTel jvm.* via javaagent pending ingestion rebuild). Recovery = GC/restart. Scope=jobmanager",
    ),
    dict(
        name="INFRA-warn-jvm-gc-500",
        stream="flink_jobmanager_status_jvm_garbagecollector_g1_young_generation_time",
        promql="max (rate(flink_jobmanager_status_jvm_garbagecollector_g1_young_generation_time[5m]) * 1000)",
        promql_condition=(">=", 500),
        period=1,
        frequency=1,
        desc="[Warning/infra] JVM GC young-gen time >500ms/s for 60s (native Flink GC metric; OTel jvm_gc_duration pending). Recovery = GC tuning. Scope=jobmanager",
    ),
    dict(
        name="INFRA-crit-disk-20",
        stream="node_filesystem_avail_bytes",
        promql="100 * node_filesystem_avail_bytes / node_filesystem_size_bytes",
        promql_condition=("<", 20),
        period=1,
        frequency=1,
        desc="[Critical/infra] Free SSD <20% per mount. Recovery = free disk. Scope=vm_id/mountpoint",
    ),
    dict(
        name="INFRA-warn-disk-io-20",
        stream="node_disk_io_time_seconds_total",
        promql="rate(node_disk_io_time_seconds_total[5m]) * 1000",
        promql_condition=(">=", 20),
        period=1,
        frequency=1,
        desc="[Warning/infra] Disk IO await >20ms per device 60s. Scope=device",
    ),
    dict(
        name="INFRA-warn-net-80",
        stream="node_network_transmit_bytes_total",
        promql="rate(node_network_transmit_bytes_total[5m])",
        promql_condition=(">", 80),
        period=1,
        frequency=1,
        desc="[Warning/infra] Network TX >80% capacity per host 60s (rate observed). Scope=host/device",
    ),
    dict(
        name="INFRA-crit-o2-mem-14",
        stream="container_memory_usage_bytes",
        conditions=[("container", "=", "openobserve"), ("value", ">=", 14 * 1024 * 1024 * 1024)],
        period=1,
        desc="[Critical/infra] OpenObserve >14GB for 60s (ZO_MEMORY_LIMIT=12g starvation risk). Recovery = restart/limit. Scope=container",
    ),
    dict(
        name="INFRA-crit-collector-export-failed",
        stream="otelcol_exporter_send_failed_metric_points",
        conditions=[("value", ">", 0)],
        desc="[Critical/infra] Collector otelcol_exporter_send_failed >0 for 5m retry window: O2 delivery gap started — single-pane breach. Scope=global",
    ),
]

# Retention policy (docs/04_contracts/openobserve.md): logs 30d, metrics 90d,
# traces 14d. O2 v0.91.5 resolves per-stream retention as
# stream_settings.data_retention (>0) else the global
# ZO_COMPACT_DATA_RETENTION_DAYS default (3650d = 10y) — verified in
# src/service/compact/retention.rs on the v0.91.5 tag. Per-stream settings are
# therefore REQUIRED to honor the per-type contract; new metric streams inherit
# the global until this sync re-runs (idempotent, so just re-run provisioning).
# Alert rules are NOT streams: definitions + trigger history live in the O2
# meta store (metadata.sqlite `alerts` table — verified live 2026-08-11) and
# are not subject to stream retention; the 180d alert contract is satisfied by
# the meta store (no TTL) and is documented separately.
RETENTION_DAYS = {"logs": 30, "metrics": 90, "traces": 14}


def provision_retention():
    """Idempotently set per-stream data_retention for every stream of each
    type. Partial PUT (stream settings merge — verified live: other settings
    fields survive). Skips streams already at the target value."""
    changed = 0
    for stype, days in RETENTION_DAYS.items():
        status, resp = api("GET", f"/streams?type={stype}")
        if status != 200:
            print(f"{status} list streams type={stype}: {str(resp)[:200]}")
            continue
        streams = resp.get("list", []) if isinstance(resp, dict) else []
        for s in streams:
            name = s["name"]
            cur = s.get("settings", {}).get("data_retention", 0)
            if cur == days:
                continue
            st, body = api(
                "PUT",
                f"/streams/{name}/settings?type={stype}",
                {"data_retention": days},
            )
            if st == 200:
                changed += 1
                print(f"200 set retention {stype}/{name}: {cur} -> {days}")
            else:
                print(
                    f"{st} set retention {stype}/{name} (cur={cur}): {str(body)[:200]}"
                )
    print(
        f"retention sync: {changed} stream(s) updated "
        f"(policy {json.dumps(RETENTION_DAYS)}; alert rules live in the O2 "
        f"meta store, not stream retention)"
    )


def api(method, path, body=None):
    req = urllib.request.Request(
        f"{BASE}/api/{ORG}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers=HEADERS,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:2000]


def api_raw(method, url, body=None):
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode() if body is not None else None,
        headers=HEADERS,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:2000]


def make_panel(pid, title, ptype, query, stream):
    q = {
        "query": query,
        "query_type": "sql" if ptype != "promql" else "promql",
        "format": "table" if ptype == "table" else "time_series",
        "group_by": [] if ptype == "table" else [],
        "stream": {"name": stream, "type": "metrics"},
    }
    if ptype in ("gauge", "value"):
        q["fields"] = {"value": "value"}
    return {
        "id": pid,
        "title": title,
        "type": "panel",
        "data": {"type": ptype, "queries": [q], "base": {"show": True}},
    }


# ---- v0.91.5 v8 dashboard schema (empirically verified 2026-08-11 against
# the running commit fed9187 + per-panel CRUD routes):
#   POST /dashboards  body: {version: 8, title, description, tabs: [{tabId,
#     name, panels: [...]}]}   (folder = query param)
#   PUT  /dashboards/{id}?hash=<hash>  body: same version-tagged body (the
#     update handler REQUIRES the current hash as a query param)
#   POST /dashboards/{id}/panels?hash=<hash>  body: {tabId, panel} (add one
#     panel; response carries the NEW hash to chain)
# Panel/Query fields are camelCase; PanelConfig/PanelFields/QueryConfig are
# snake_case (no rename_all on those structs — show_legends, stream_type,
# promql_legend). This is why the v3-shaped bodies the old provisioner POSTed
# silently stored empty shells (tabs never populated).

EMPTY_FILTER = {
    "type": "condition",
    "values": [],
    "logicalOperator": "AND",
    "filterType": "condition",
}


def make_panel_v8(pid, index, title, ptype, query, stream):
    """v8 Panel JSON: 192-col grid (x=column 0-191, w=half=96), auto-visible
    on all metrics streams; query_type promql vs sql."""
    if ptype == "promql":
        query_type, chart_type = "promql", "line"
        fields = {
            "stream": stream,
            "stream_type": "metrics",
            "x": [],
            "y": [],
            "z": [],
            "filter": dict(EMPTY_FILTER),
        }
        qconfig = {"promql_legend": "{{task_name}}"}
    else:
        query_type = "sql"
        chart_type = {"table": "table", "gauge": "gauge", "value": "value"}.get(
            ptype, "line"
        )
        fields = {
            "stream": stream,
            "stream_type": "metrics",
            "x": [
                {"label": "_timestamp", "alias": "_timestamp", "column": "_timestamp"}
            ],
            "y": [{"label": "value", "alias": "value", "column": "value"}],
            "z": [],
            "filter": dict(EMPTY_FILTER),
        }
        # QueryConfig.promql_legend is REQUIRED even for SQL queries (no
        # serde default — verified against the running commit + live API).
        qconfig = {"promql_legend": ""}
    return {
        "id": pid,
        "type": chart_type,
        "title": title,
        "description": "",
        "config": {"show_legends": True},
        "queryType": query_type,
        "queries": [
            {"query": query, "customQuery": True, "fields": fields, "config": qconfig}
        ],
        "layout": {
            "x": (index % 2) * 96,
            "y": (index // 2) * 4,
            "w": 96,
            "h": 4,
            "i": index,
        },
    }


def make_dashboard_v8(spec):
    return {
        "version": 8,
        "title": spec["title"],
        "description": spec["description"],
        "tabs": [
            {
                "tabId": "t0",
                "name": "Overview",
                "panels": [
                    make_panel_v8(f"p{i}", i, *p) for i, p in enumerate(spec["panels"])
                ],
            }
        ],
    }


def provision_dashboards():
    _, existing = api("GET", "/dashboards")
    by_title = (
        {d.get("title"): d for d in existing.get("dashboards", [])}
        if existing and isinstance(existing, dict)
        else {}
    )
    for spec in DASHBOARDS:
        title = spec["title"]
        db = by_title.get(title)
        if db is None:
            status, resp = api("POST", "/dashboards", make_dashboard_v8(spec))
            print(f"{status} create dashboard {title}: {json.dumps(resp)[:160]}")
            continue
        # Converge: the v0.91.5 v8 API stores panels per-panel; add any spec
        # panel whose title is missing from the current body (idempotent —
        # existing panels are left untouched).
        did = db["dashboard_id"]
        status, full = api("GET", f"/dashboards/{did}")
        hashv = full.get("hash", "")
        v8 = full.get("v8") or {}
        tabs = v8.get("tabs") or []
        existing_panels = {p.get("title") for t in tabs for p in t.get("panels", [])}
        missing = [
            (i, p) for i, p in enumerate(spec["panels"]) if p[0] not in existing_panels
        ]
        if not missing:
            print(f"dashboard converged: {title} ({len(existing_panels)} panels)")
            continue
        if tabs:
            tid = tabs[0]["tabId"]  # v8 GET serializes camelCase (tabId)
        else:
            # Empty shell (old provisioner's v3 body was silently dropped):
            # seed the first tab via the update route, then add panels.
            tid = "t0"
            seed = {
                "version": 8,
                "title": title,
                "description": v8.get("description") or spec["description"],
                "tabs": [{"tabId": tid, "name": "Overview", "panels": []}],
            }
            status, resp = api("PUT", f"/dashboards/{did}?hash={hashv}", seed)
            hashv = resp.get("hash", hashv) if isinstance(resp, dict) else hashv
            print(f"{status} seed tab for {title}")
        for i, (ptitle, ptype, query, stream) in missing:
            panel = make_panel_v8(f"p{i}", i, ptitle, ptype, query, stream)
            status, resp = api(
                "POST",
                f"/dashboards/{did}/panels?hash={hashv}",
                {"tabId": tid, "panel": panel},
            )
            hashv = resp.get("hash", hashv) if isinstance(resp, dict) else hashv
            print(f"{status} add panel {ptitle!r} -> {title}")


def provision_destination():
    # v0.91.5 route: /api/{org}/alerts/destinations (not /destinations); list
    # response is a raw JSON array.
    _, existing = api("GET", "/alerts/destinations")
    names = {d.get("name") for d in existing} if isinstance(existing, list) else set()
    if "dev-webhook" in names:
        print("destination exists: dev-webhook")
        return
    body = {
        "name": "dev-webhook",
        # localhost = O2's own netns: the compose alert-consumer service
        # (network_mode: service:openobserve) listens on 127.0.0.1:9999 and
        # ZO_SSRF_ALLOW_LOOPBACK=true lets the guard pass it (dev only).
        # O2 v0.91.5's SSRF DNS resolver blocks ALL private-ranged targets
        # (10/8, 172.16/12, 192.168/16) unconditionally, so a bridge-network
        # receiver can never be reached — production needs a public endpoint
        # or a netns-shared consumer.
        "url": "http://localhost:9999/noop",
        "method": "post",
        "type": "http",
        "template": "prebuilt_webhook",
        "headers": {"Content-Type": "application/json"},
        "output_format": "json",
        "metadata": {},
    }
    status, resp = api("POST", "/alerts/destinations", body)
    print(f"{status} create destination dev-webhook: {json.dumps(resp)[:200]}")


# O2 refuses alerts on streams that don't exist yet ("Stream X not found").
# Seed each planned stream with one zero-valued point (child_alive=1 so the
# orphan rule never false-fires) via the otel-collector OTLP/JSON endpoint,
# using the same payload shape the Java emitter produces.
SEED_GAUGES = [
    # (otlp name, value, extra attributes)
    ("bridge.slot.capacity.remaining", 1024.0, [("slot_id", "1000")]),
    ("bridge.slot.safety.state", 1.0, [("slot_id", "1000")]),
    # 2026-08-24: seed the FLINK-REPORTED series name so SIGNAL-crit-schema-
    # version-rejected / SIGNAL-warn-schema-rejected-rate can be created
    # (O2 v2 alerts validate stream existence at create; the hand-emitted
    # unprefixed name is dead in this stack — the SignalJob counter arrives
    # via the Prometheus reporter as flink_taskmanager_job_task_operator_*).
    ("flink.taskmanager.job.task.operator.compute.invalid.byreason.schema_version", 0.0, []),
    ("bridge.slot.unsafe.duration.ms", 0.0, [("slot_id", "1000")]),
    ("bridge.reconnect.consecutive", 0.0, [("slot_id", "1000")]),
    ("bridge.active.sockets", 0.0, [("slot_id", "1000")]),
    ("bridge.child.process.alive", 1.0, [("slot_id", "1000")]),
    ("process.open.fds", 0.0, []),
    ("process.fd.limit", 65535.0, []),
    ("process.fd.usage.percent", 0.0, []),
    ("process.rss.bytes", 0.0, []),
    ("go.goroutines", 0.0, []),
    ("jvm.threads.live", 0.0, []),
]
SEED_SUMS = [
    ("otelcol.exporter.send.failed.metric.points", 0, []),
]
# DELTA sums (aggregationTemporality=1, non-monotonic) — match the compute
# emitter's per-flush delta, so the stream carries ONE temporality and the
# SIGNAL-crit-schema-version-rejected alert (value > 0) fires only on NEW
# rejections, never on historical replay.
SEED_DELTA_SUMS = [
    ("compute.invalid.byReason.schema-version", 0, []),
]
SEED_HISTOGRAM = (
    "append.latency.ms",
    [5, 10, 25, 50, 100, 250, 500],
    ["0", "0", "1", "0", "0", "0", "0"],
    12.5,
    1,
)


def seed_streams():
    now_ns = str(int(__import__("time").time() * 1_000_000_000))
    metrics = []
    for name, value, attrs in SEED_GAUGES:
        dp = {
            "timeUnixNano": now_ns,
            "asDouble": value,
            "attributes": [{"key": k, "value": {"stringValue": v}} for k, v in attrs],
        }
        metrics.append({"name": name, "gauge": {"dataPoints": [dp]}})
    for name, value, attrs in SEED_SUMS:
        dp = {
            "timeUnixNano": now_ns,
            "asInt": str(value),
            "attributes": [{"key": k, "value": {"stringValue": v}} for k, v in attrs],
        }
        metrics.append(
            {
                "name": name,
                "sum": {
                    "dataPoints": [dp],
                    "aggregationTemporality": 2,
                    "isMonotonic": True,
                },
            }
        )
    for name, value, attrs in SEED_DELTA_SUMS:
        dp = {
            "timeUnixNano": now_ns,
            "asInt": str(value),
            "attributes": [{"key": k, "value": {"stringValue": v}} for k, v in attrs],
        }
        metrics.append(
            {
                "name": name,
                "sum": {
                    "dataPoints": [dp],
                    "aggregationTemporality": 1,
                    "isMonotonic": False,
                },
            }
        )
    name, bounds, counts, total, count = SEED_HISTOGRAM
    metrics.append(
        {
            "name": name,
            "histogram": {
                "dataPoints": [
                    {
                        "timeUnixNano": now_ns,
                        "count": str(count),
                        "sum": total,
                        "explicitBounds": bounds,
                        "bucketCounts": counts,
                    }
                ]
            },
        }
    )
    payload = {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "ingestion"}}
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "ingestion.metrics", "version": "1.0.0"},
                        "metrics": metrics,
                    }
                ],
            }
        ]
    }
    req = urllib.request.Request(
        "http://localhost:4318/v1/metrics",
        data=json.dumps(payload).encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(f"seed streams: collector {resp.status}")
    except urllib.error.HTTPError as e:
        print(f"seed streams: collector error {e.code}: {e.read().decode()[:300]}")
        sys.exit(1)


def provision_alerts():
    # v0.91.5: v2 alerts API lives at /api/v2/{org}/alerts (v2 BEFORE org id;
    # /api/{org}/v2/alerts 404s). Body is the flat v2 Alert JSON (Alert fields
    # serde-flattened into CreateAlertRequestBody). Realtime alerts require
    # query_type=custom; O2 builds `SELECT * FROM stream WHERE <condition>`
    # from the conditions list (column value on metrics streams). Multiple
    # conditions are ANDed (stored shape: condition.conditions.and[]); label
    # columns (e.g. task_name, instance) are first-class condition targets.
    v2api = lambda method, body=None: api_raw(
        method, f"{BASE}/api/v2/{ORG}/alerts", body
    )
    _, existing = v2api("GET")
    names = (
        {a.get("name") for a in existing.get("list", [])}
        if existing and isinstance(existing, dict)
        else set()
    )
    for spec in ALERTS:
        name = spec["name"]
        if name in names:
            print(f"alert exists: {name}")
            continue
        promql = spec.get("promql")
        body = {
            "name": name,
            "stream_name": spec["stream"],
            "stream_type": "metrics",
            "is_real_time": promql is None,
            "query_condition": (
                {
                    "type": "promql",
                    "promql": promql,
                    "promql_condition": {
                        "column": "value",
                        "operator": spec["promql_condition"][0],
                        "value": spec["promql_condition"][1],
                        "ignore_case": False,
                    },
                }
                if promql
                else {
                    "type": "custom",
                    "conditions": [
                        {"column": col, "operator": op, "value": val}
                        for col, op, val in spec["conditions"]
                    ],
                }
            ),
            "trigger_condition": {
                "period": spec.get("period", 1),  # minutes
                "frequency": spec.get(
                    "frequency", 0
                ),  # real-time: 0 = evaluate continuously
                "frequency_type": "minutes",
                "operator": ">=",
                "threshold": spec.get(
                    "threshold", 1
                ),  # fire after N matching evaluations
            },
            "destinations": spec.get("destinations", ["dev-webhook"]),
            "description": spec["desc"],
            "enabled": True,
            "tz_offset": 0,
            "row_template": f"Alert {name}: {spec['desc']}",
            "row_template_type": "String",
        }
        status, resp = v2api("POST", body)
        print(f"{status} create alert {name}: {json.dumps(resp)[:200]}")


if __name__ == "__main__":
    # Health gate: O2 must be reachable and credentials valid.
    # Route is GET /config (v0.91.5 nests config under /config, not /api/{org}).
    req = urllib.request.Request(f"{BASE}/config", headers=HEADERS, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = resp.status
            payload = resp.read().decode()
    except urllib.error.HTTPError as e:
        status = e.code
        payload = e.read().decode()[:2000]
    if status != 200:
        print(f"ERROR: OpenObserve not ready (GET /config -> {status}): {payload}")
        sys.exit(1)
    print(f"OpenObserve reachable: {json.dumps(json.loads(payload))[:160]}")
    provision_destination()
    provision_dashboards()
    seed_streams()
    provision_alerts()
    provision_retention()
    print("done")
