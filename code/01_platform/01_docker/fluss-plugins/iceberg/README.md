# Iceberg tiering plugin jars

All jars here are untracked host binaries (`*.jar` is gitignored), mounted into
the flink-jobmanager / flink-taskmanager `/opt/flink/lib/` by
`docker-compose.yml`.

Downloaded (Fluss 0.9.1 / Flink 2.2 distribution artifacts):
- `fluss-flink-2.2-0.9.1-incubating.jar`
- `fluss-flink-tiering-0.9.1-incubating.jar`
- `fluss-lake-iceberg-0.9.1-incubating.jar`
- `fluss-fs-s3-0.9.1-incubating.jar`
- `fluss-fs-hdfs-0.9.1-incubating.jar`
- `fluss-fs-hadoop-shaded-0.9-SNAPSHOT.jar`
- `flink-shaded-hadoop-2-uber-2.8.3-10.0.jar` — NOT mounted; its hadoop 2.8.3
  `Configuration` breaks fluss-fs-s3's S3A (NoSuchMethodError getTimeDuration,
  M-16 in docs/06_operations/07-lake-archive-ops.md (CHG-117))

## hadoop-mapreduce-compat-2.8.5.jar — CUSTOM BUILD (M-15)

Iceberg's shaded parquet write path (`ParquetInputFormat` inside
`fluss-lake-iceberg`) lazily references the UN-shaded
`org.apache.hadoop.mapreduce.lib.input.FileInputFormat`, which exists only in
`hadoop-mapreduce-client-core` — absent from every other jar here. Without it
the tiering job dies with `NoClassDefFoundError` under sustained write volume.

This minimal jar contains ONLY `org/apache/hadoop/mapreduce/**` (494 classes)
so it cannot clash with the hadoop 3.3.x `Configuration` bundled in the
fluss-fs jars. Rebuild if lost:

```bash
cd /tmp && rm -rf mr-compat && mkdir mr-compat && cd mr-compat
unzip -q ~/.m2/repository/org/apache/hadoop/hadoop-mapreduce-client-core/2.8.5/hadoop-mapreduce-client-core-2.8.5.jar 'org/apache/hadoop/mapreduce/*'
jar cf <repo>/code/01_platform/01_docker/fluss-plugins/iceberg/hadoop-mapreduce-compat-2.8.5.jar org/
```

`tiering-start.sh` has a fail-fast guard that refuses to submit the tiering
job unless this jar is present in both flink containers (and refuses if the
bad hadoop-uber jar is mounted instead).
