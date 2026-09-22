# Fluss runtime image

The Fluss image the **production** Swarm stack runs. Built on a build host,
pushed to a registry, and referenced by manifest digest through `FLUSS_IMAGE`
in `docker-stack.yml` — a Swarm stack cannot `build:`.

| File | Role |
|---|---|
| `Dockerfile` | The image. Digest-pinned stock Fluss base + the two lake plugins. |
| `fetch-jars.sh` | Downloads and SHA256-verifies both jars before the build sees them. |

## Why the stock image is not enough

Lake tiering needs an S3 filesystem **inside the iceberg plugin's classloader**.
Fluss loads `/opt/fluss/plugins/<name>/` as separate classloaders, so a jar
sitting in `plugins/s3/` is invisible to the `iceberg` one.

| Gap | Consequence |
|---|---|
| `plugins/iceberg/` has no hadoop-aws (`S3AFileSystem`) | The tiering job's Iceberg writes have no `s3://` filesystem. |
| `plugins/iceberg/` has no `HdfsConfiguration` | `hdfs://` fails the same way. |

The stock image *does* ship both jars — in `plugins/s3/` and `plugins/hdfs/`.
The dev compose bridges the gap by bind-mounting them into `plugins/iceberg/`;
this image bakes them there instead, because the mounted files are gitignored
host binaries that do not exist on a fresh clone of the production VM.

**Verified 2026-09-16:** `git ls-files code/01_platform/01_docker/fluss-plugins/`
returns one file (the README). The dev volumes source the other eight jars from
an untracked working tree.

## Build

```bash
make fluss-image                       # fetch + verify + build + re-verify
make fluss-image FLUSS_RUNTIME_TAG=myrepo/fluss-runtime:0.9.1
```

Or manually:

```bash
cd code/01_platform/01_docker/fluss-runtime
bash fetch-jars.sh --dest /tmp/flr/jars
cp Dockerfile /tmp/flr/
docker build -t myrepo/fluss-runtime:0.9.1 /tmp/flr
bash fetch-jars.sh --verify /tmp/flr/jars     # offline re-check
```

## Publishing and pinning

```bash
docker push myrepo/fluss-runtime:0.9.1
bash code/01_platform/04_scripts/digest-pin.sh myrepo/fluss-runtime:0.9.1
# then put the printed ref on FLUSS_IMAGE in runtime.lock
```

A locally built image that was never pushed has **no** manifest digest, so
`repo@sha256:<image-id>` does not resolve. Push before pinning — that is the
whole point of the digest, and why `runtime.lock` still holds the stock
`apache/fluss` reference until a registry exists.

## The pinned artifacts

`fetch-jars.sh` carries the SHA256 of every jar it stages:

| Artifact | Source |
|---|---|
| `fluss-fs-s3-1.0.0.jar` | Maven Central |
| `fluss-fs-hdfs-1.0.0.jar` | Maven Central |

These are the same two artifacts, with the same SHA256 pins, that the Flink
image stages — so the two images cannot silently drift apart.

### Why `fluss-fs-hadoop-shaded` is not here

The dev compose mounted `fluss-fs-hadoop-shaded-0.9-SNAPSHOT.jar` until
2026-09-22 (when the 1.0.0 mounts replaced it), an
**unpublished** module: HTTP 404 on Maven Central and on the Apache snapshot
repository. A SNAPSHOT cannot be checksum-pinned, so it cannot be part of a
reproducible image.

All 12898 of its entries are already present in the published `fluss-fs-s3` +
`fluss-fs-hdfs` pair (verified entry by entry on 2026-09-16: 0 missing), and it
declares no Fluss `FileSystemPlugin` service of its own, so nothing loads it by
name. Dropping it is what makes this image reproducible — the same call the
Flink image makes, for the same reason.

### Why no `hadoop-mapreduce-compat` jar here

That jar (M-15/M-16) exists for Iceberg's *parquet write* path, which runs in
the Flink tiering job, not in the Fluss servers. Fluss 0.9 tiers the lake from
an external Flink job (`code/01_platform/04_scripts/tiering-start.sh`); the
servers only coordinate. The dev tablet carries no compat jar and lake tiering
works there — the Flink image already ships it.

## What this image does NOT close

`datalake.*` config reaching the servers is a *separate* matter: the stack
already sets the 36 `datalake.iceberg.*` keys, and table-level
`table.datalake.enabled` is per-table. This image only ensures the classes the
tiering path needs are actually present in the one classloader that needs them.

> Residual risk: the entries shared with the dropped SNAPSHOT jar are
> byte-identical only where the published jars agree with it. Nothing loads the
> dropped jar by name and it declares no plugin service, but the only definitive
> test is a running tiering job against a real bucket — a live drill, not
> covered by the offline suite.
