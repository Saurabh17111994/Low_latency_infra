# Flink runtime image

The Flink image the **production** Swarm stack runs. Built on a build host,
pushed to a registry, and referenced by manifest digest through `FLINK_IMAGE`
in `docker-stack.yml` — a Swarm stack cannot `build:`.

| File | Role |
|---|---|
| `Dockerfile` | The image. Digest-pinned base + Fluss jars + R2 config + secret bridge. |
| `fetch-jars.sh` | Downloads and SHA256-verifies every jar; derives the compat jar. |
| `core-site.xml` | Hadoop client config, baked to `/etc/hadoop/conf/`. Placeholders only. |
| `20-r2-secrets-from-file.sh` | Entrypoint wrapper bridging Swarm `*_FILE` secrets to env. |

## Why the stock image is not enough

Each of these was verified inside `flink:2.2.1-scala_2.12-java17` (2026-09-16):

| Gap | Consequence |
|---|---|
| No Fluss jars in `/opt/flink/lib` | The job cannot load the connector — `compute.jar` ships no Fluss classes (`provided` scope). |
| `flink-s3-fs-hadoop` ships in `/opt/flink/opt` but is **not** linked into `plugins/` | `state.checkpoints.dir: s3://…` has no filesystem. |
| No `/etc/hadoop/conf/core-site.xml` | Both Flink's state backend and the Fluss client lose the R2 **endpoint**. Credentials are still found by the default chain, so `s3://` resolves against **Amazon's** S3 and fails `403 InvalidAccessKeyId` — a confusing failure, not an obvious one. |
| No `AWS_REGION` in the stack | S3A requests fail `400 Bad Request`. |

`/etc/hadoop/conf` is auto-detected by Flink's own `bin/config.sh`, which appends
it to the JVM classpath, so baking the config needs **no** environment variable
and no per-node bind mount (which a Swarm stack cannot express portably anyway).

## Build

```bash
make flink-image                       # fetch + verify + build + re-verify
make flink-image FLINK_RUNTIME_TAG=myrepo/flink-runtime:1.0.0
```

Or manually:

```bash
cd code/01_platform/01_docker/flink-runtime
bash fetch-jars.sh --dest /tmp/fr/jars
cp Dockerfile core-site.xml 20-r2-secrets-from-file.sh /tmp/fr/
docker build -t myrepo/flink-runtime:1.0.0 /tmp/fr
bash fetch-jars.sh --verify /tmp/fr/jars     # offline re-check
```

## Publishing and pinning

```bash
docker push myrepo/flink-runtime:1.0.0
bash code/01_platform/04_scripts/digest-pin.sh myrepo/flink-runtime:1.0.0
# then put the printed ref on FLINK_IMAGE in runtime.lock
```

A locally built image that was never pushed has **no** manifest digest, so
`repo@sha256:<image-id>` does not resolve. Push before pinning — that is the
whole point of the digest.

## The pinned artifacts

`fetch-jars.sh` carries the SHA256 of every jar it stages:

| Artifact | Source |
|---|---|
| `fluss-flink-2.2-1.0.0.jar` | Maven Central |
| `fluss-flink-tiering-1.0.0.jar` | Maven Central |
| `fluss-lake-iceberg-1.0.0.jar` | Maven Central |
| `fluss-fs-s3-1.0.0.jar` | Maven Central |
| `fluss-fs-hdfs-1.0.0.jar` | Maven Central |
| `hadoop-mapreduce-client-core-2.8.5.jar` | Maven Central — input to the derived jar, not installed |
| `hadoop-mapreduce-compat-2.8.5.jar` | **derived** in-image from the row above |

`flink-shaded-hadoop-2-uber` is deliberately absent (M-16: its 2.8.3
`Configuration` breaks `fluss-fs-s3`'s S3A with `NoSuchMethodError
getTimeDuration`).

### Why the compat jar is derived rather than committed

Iceberg's shaded parquet write path lazily loads the **un-shaded**
`org.apache.hadoop.mapreduce.lib.input.FileInputFormat`, which exists only in
`hadoop-mapreduce-client-core` (M-15/M-16). The full jar also carries
`org/apache/hadoop/mapred/**`, which is exactly the clash M-16 warns about — so
the image gets only the `org/apache/hadoop/mapreduce/**` package.

Deriving it beats committing a 494-class binary: the source is a checksummed
Maven artifact, the extraction is `unzip` + `zip` with a pinned file mtime **and
a pinned collation** (`LC_ALL=C sort`), and the result is verified against its own
SHA256 — so the build fails loudly if a toolchain change alters the bytes.
Collation matters as much as the mtime: `sort` follows the caller's locale, so
unpinned it ordered the same classes differently and the derived jar hashed to a
different value. Measured 2026-09-21: `en_IN.UTF-8` gave `14c5a8e5…`, `C` gave
`c19c414b…`, and the first CI run — a C-locale runner — failed the pin that the
workstation had satisfied for weeks.

### Why `fluss-fs-hadoop-shaded` is not here

The dev compose mounted `fluss-fs-hadoop-shaded-0.9-SNAPSHOT.jar` until
2026-09-22, when the 1.0.0 mounts replaced it. It is an
**unpublished** build module — `fluss-fs-hadoop-shaded` 404s on Central, and the
similarly named published `fluss-fs-hadoop` is an unrelated 10 KB stub. A
SNAPSHOT cannot be checksum-pinned, so it cannot be part of a reproducible
image. All 12336 of its entries are already present in the published
`fluss-fs-s3` + `fluss-fs-hdfs` pair (both staged above), and it declares no
Fluss `FileSystemPlugin` service of its own, so nothing loads it by name.

> Residual risk: 106 of those shared entries differ in bytes (99 are Hadoop
> internals, 7 are metadata). Nothing loads the jar directly, but the only
> definitive test is a running tiering job — a live drill, not covered by the
> offline suites.

## Secrets

R2 credentials reach the container as **Swarm secrets** (files under
`/run/secrets/`), named by `AWS_ACCESS_KEY_ID_FILE` / `AWS_SECRET_ACCESS_KEY_FILE`.
Hadoop can only expand environment variables, so
`20-r2-secrets-from-file.sh` reads the files and exports
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` before Flink starts, then execs
the base image's entrypoint. A `*_FILE` that is set but unreadable **aborts
startup** rather than booting a cluster that cannot write checkpoints.

No credential value is ever written into this repository: `core-site.xml` holds
`${env.NAME}` placeholders that Hadoop expands at read time.
