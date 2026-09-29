# Changelog filesystem-storage plugin (`flink-dstl-dfs`)

`flink-dstl-dfs-2.2.1.jar` is the Flink 2.2.1 **DSTL DFS** plugin: it provides
`FsStateChangelogStorageFactory`, the `filesystem` alternative to the
TaskManager-memory changelog storage. It is **not** part of the stock
`flink:2.2.1` image (`/opt/flink/lib` and `/opt/flink/plugins` do not contain
it), so a changelog-enabled job with `state.changelog.storage: filesystem`
fails at storage creation without this jar on the TaskManager `plugins/`
classpath.

Provenance:
- artifact: `org.apache.flink:flink-dstl-dfs:2.2.1` (Apache Flink 2.2.1 release line)
- local source: `~/.m2/repository/org/apache/flink/flink-dstl-dfs/2.2.1/flink-dstl-dfs-2.2.1.jar`
- sha256: `aa467887d059d348d1f52d134d7ba1c0664e072054dae3748c5d498d63f997ad`
  (recompute after any refresh: `sha256sum flink-dstl-dfs-2.2.1.jar`)

Mounted read-only into the `flink-taskmanager` service at
`/opt/flink/plugins/dstl-dfs/flink-dstl-dfs-2.2.1.jar`. The changelog storage is
a TaskManager-level service
(`TaskExecutorStateChangelogStoragesManager.stateChangelogStorageForJob` reads
the TaskManager configuration), so the JobManager does not need the plugin.

The two matching cluster settings live in the shared `FLINK_PROPERTIES` block
of `docker-compose.yml`:
`state.changelog.storage: filesystem` and
`state.changelog.dstl.dfs.base-path: file:///checkpoints/changelog`.

Why filesystem matters (measured 2026-09-29, CT-4A trial): with the `memory`
storage every checkpoint re-serializes all changes accumulated since the last
materialization, so `state_size` grew 1.5 MB → 38 MB in ~3 minutes and the
checkpoint metadata reached ~49 MB; those payload transfers broke the
TaskManager/JobManager Pekko RPC (transient association errors) and checkpoints
started expiring. The filesystem storage uploads changelog segments to the base
path as files and the checkpoint metadata only references them.

`*.jar` is gitignored (host binary, like `fluss-plugins/iceberg/`); this README
is the tracked provenance record.
