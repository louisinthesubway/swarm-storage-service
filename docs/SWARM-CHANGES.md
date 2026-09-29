# SWARM changes to Signal's storage-service

This is a fork of [signalapp/storage-service](https://github.com/signalapp/storage-service)
(AGPL-3.0-only, Copyright Signal Messenger, LLC). `upstream-main` tracks upstream `main`
unchanged; `swarm-main` (the default branch) is `upstream-main` plus the deviations on this
page. Base: `upstream-764a105` (tag), signalapp/storage-service `main` @ `764a105`.

No cryptographic primitive, no protocol code (`ServerSecretParams`, `ServerZkAuthOperations`,
`ExternalGroupCredentialGenerator`, `GroupsController`, `StorageController`, or anything under
`org.signal.storageservice.util.zk`) is touched by any change on this page. Group credentials
are still verified by upstream code, with the same zk group server parameters the SWARM
Messenger chat server (`swarm-messenger-server`) uses.

The deployment this fork feeds is documented in `swarm-messenger-server`'s
[`docs/STAGING.md`](https://github.com/louisinthesubway/swarm-messenger-server/blob/swarm-main/docs/STAGING.md),
section 5c "Storage service and groups" (image, configuration, secrets, emulator, routes). This
file records the code changes and the few facts about the deployment that explain them.

---

## 1. Configuration file accepts `${SWARM_...}` environment substitution

**File:** `src/main/java/org/signal/storageservice/StorageService.java`, `initialize()`.

Upstream's `StorageService.initialize()` does exactly one thing: if `STORAGE_SERVICE_CONFIG_URI`
is set, it points Dropwizard's configuration loader at a Google Secret Manager secret containing
a *complete* configuration document. If that variable is unset (the normal case), Dropwizard
reads the plain config file given on the command line with no substitution at all — every value,
including secrets, has to be literal text in that file.

A self-hosted deployment has no Google Secret Manager, and committing secret values into
`storage.yml` is not acceptable. SWARM adds a second, unconditional branch: when
`STORAGE_SERVICE_CONFIG_URI` is **not** set, the default file-based configuration source is
wrapped in Dropwizard's own `io.dropwizard.configuration.SubstitutingSourceProvider` with an
`EnvironmentVariableSubstitutor(false)` — the same stock Dropwizard classes many Dropwizard
apps use for this, not a SWARM-invented mechanism. This lets `storage.yml` reference
`${SWARM_STORAGE_AUTH_KEY}`-style placeholders (with optional `${VAR:-default}` fallbacks),
matching the convention `swarm-messenger-server`'s `staging.yml` already uses. The
`STORAGE_SERVICE_CONFIG_URI`/Secret Manager path is untouched and still works if anyone points
a deployment at real Google Cloud.

## 2. Bigtable client honours `BIGTABLE_EMULATOR_HOST`

**File:** `src/main/java/org/signal/storageservice/StorageService.java`, `run()`.

Upstream always builds its `BigtableDataSettings` with `BigtableDataSettings.newBuilder()`,
which dials real Google Cloud Bigtable and expects Application Default Credentials. Upstream's
*own tests* never use that path — they use `com.google.cloud.bigtable.emulator.v2.Emulator`
(`BigtableEmulatorExtension` in this repo's test sources) via `BigtableDataSettings
.newBuilderForEmulator(host, port)`. `google-cloud-bigtable-emulator` was already a test-scope
Maven dependency before this change; nothing was added to the dependency tree.

SWARM's self-hosted staging deployment has no Google Cloud project and runs a Bigtable
emulator as its own service instead. At startup, `StorageService.run()` now reads the
`BIGTABLE_EMULATOR_HOST` environment variable (the name `gcloud beta emulators bigtable env-init`
exports and Google's client libraries use for the same purpose): if it is set to `host:port`,
`BigtableDataSettings.newBuilderForEmulator(host, port)` is used; otherwise behaviour is
byte-for-byte identical to upstream (`BigtableDataSettings.newBuilder()`).

Any emulator that speaks the Bigtable gRPC API works. Which one, and what that means for the
data (deployment facts, recorded here because they decide what this change is good for):

- Google's own emulator (`gcloud beta emulators bigtable start`, the Go `bttest` package) keeps
  every table **in memory**: every group and synced record would vanish on a restart.
- The SWARM staging stack runs `cbtemulator` from
  [fullstorydev/emulators](https://github.com/fullstorydev/emulators) (MIT), a fork of that same
  `bttest` code with a pluggable storage layer, started with `-dir` so each table is a LevelDB
  database on a Docker volume and survives restarts (verified 2026-09-28: tables still listed
  after an emulator restart). Built from pinned source by `deploy/staging/bigtable/Dockerfile`
  in `swarm-messenger-server`.
- Either way this is a **staging** backend: an emulator is a test tool with no replication and
  no durability promises beyond "the files are on the volume". A durable deployment needs real
  Cloud Bigtable or a storage backend of its own: section 3 adds one on FoundationDB.

## 3. A FoundationDB backend for the four data sets

**Status: PROPOSED (design, 2026-09-29, Opus M6c).** Written before the code; the status line is
updated as parts are implemented and tested.

### 3.1 Why

Section 2 is a test tool: one emulator process, LevelDB files on a volume, no replication, no
promise that an acknowledged write is on disk. Real Cloud Bigtable would make a self-hosted
messenger depend on a Google Cloud account at runtime. The staging stack already runs
FoundationDB 7.3.76 for the chat server (`swarm-messenger-server`, `deploy/staging`), with ACID
transactions, `fsync`ed commits and its own backup tooling. This section adds a second backend
for the storage service on that same FoundationDB cluster, in a directory of its own, selected by
configuration. The Bigtable code path stays as it is and stays the default.

### 3.2 Configuration

A new optional top-level block in the service's YAML, next to upstream's `bigtable:` block:

```yaml
storage:
  backend: foundationdb            # bigtable (the default when the block is absent) | foundationdb
  foundationdb:
    clusterFile: /etc/foundationdb/fdb.cluster
    directory: [swarm-storage-service]   # a Directory-layer path; the service owns everything below it
    transactionTimeout: 10s        # optional; covers every retry of one transaction
    transactionRetryLimit: 100     # optional
```

- `storage.backend: bigtable` (or no `storage` block): byte-for-byte the upstream behaviour
  plus sections 1 and 2. `bigtable:` is required.
- `storage.backend: foundationdb`: the service opens the cluster file, creates or opens the
  directory and serves every `/v1/storage*` and `/v2/groups*` call from FoundationDB. It does not
  create a Bigtable client at all. `bigtable:` becomes optional for the service (upstream marks
  it `@NotNull`; SWARM relaxes that to "required when the backend is bigtable").
- The migration command (3.9) reads both blocks: `bigtable:` is its source,
  `storage.foundationdb` its target.

### 3.3 The shape of the change, and why this one

The controllers (`GroupsController`, `GroupsV1Controller`, `StorageController`) talk only to
`GroupsManager` and `StorageManager`. The managers hold the logic that has to behave the same on
every backend: `updateGroup` returning the stored group when the version check fails,
`getChangeRecords` appending the current group state when the log does not have it yet,
`StorageManager.set` writing items only after the manifest moved, `delete` clearing items and
manifest in parallel. The Bigtable specifics live one level lower, in four table classes whose
public methods are already a clean storage API.

So the seam is at the table level:

- Four small interfaces, new files in `org.signal.storageservice.storage`: `GroupsStore`,
  `GroupLogStore`, `StorageManifestsStore`, `StorageItemsStore`. Each declares exactly the public
  methods of one upstream table class, with the same signatures.
- The four upstream table classes change by one word each (`implements ...`).
- `GroupsManager` and `StorageManager` hold the interfaces and get a second constructor that
  takes them; upstream's constructor (`BigtableDataClient` + table ids) stays and delegates, so
  every upstream caller and test compiles unchanged.
- The FoundationDB implementations of the four interfaces are new files in
  `org.signal.storageservice.storage.foundationdb`.
- `StorageService.run()` picks the backend; the controllers are not touched.

Rejected: interfaces for the managers (each backend would carry its own copy of exactly the logic
that must not drift); FoundationDB subclasses of the managers (a method upstream adds to a manager
later would silently run the Bigtable code against null tables).

### 3.4 Key layout

Directory layer: `DirectoryLayer.getDefault().createOrOpen(<directory>)`, and four
subdirectories under it, one per Bigtable table. Every key is a tuple-layer tuple packed into its
subdirectory. Every value is the same bytes Bigtable keeps in the corresponding cell (protobuf
`toByteString()` bytes, or the version as decimal UTF-8 text), so the migration (3.9) is a byte
copy. FoundationDB caps a value at 100,000 bytes, so each record body is stored as consecutive
chunks of at most 100,000 bytes under a trailing chunk index `0, 1, 2, ...`; a read is one key-range
read that concatenates them in key order.

| Bigtable table, row key, cell | FoundationDB subdirectory: key tuple | value |
|---|---|---|
| groups, `<group id>`, `g:gr` | `groups`: `(group id, "gr", chunk)` | `Group` protobuf bytes |
| groups, `<group id>`, `g:ver` | `groups`: `(group id, "ver")` | group version, decimal text |
| group logs, `<group id>#<version, 4 bytes big-endian>`, `l:c` | `group-logs`: `(group id, version, "c", chunk)` | `GroupChange` protobuf bytes |
| same row, `l:s` | `group-logs`: `(group id, version, "s", chunk)` | `Group` bytes (state after the change) |
| same row, `l:v` | `group-logs`: `(group id, version, "v")` | version, decimal text |
| manifests, `<uuid>#manifest`, `m:ver` | `storage-manifests`: `(uuid, "ver")` | manifest version, decimal text |
| same row, `m:dat` | `storage-manifests`: `(uuid, "dat", chunk)` | manifest bytes |
| contacts, `<uuid>#contact#<hex(key)>`, `c:d` | `storage-items`: `(uuid, key, chunk)` | item value bytes |
| same row, `c:k` | (none: the key is in the tuple) | |

Group ids and item keys are tuple byte strings, versions tuple integers, user ids tuple UUIDs.
Tuple integers sort numerically, so "versions `[from, to)` of one group's log" is exactly the key
range `[pack(group id, from), pack(group id, to))`; tuple byte strings sort by unsigned byte order,
which is the order of Bigtable's hex row keys, so items come back in the order Bigtable returns
them.

The chat server's own keys (`FoundationDbMessageStore`, `VersionstampClock`) sit in the raw
subspaces `("M")` and `("V")` at the top of the same cluster. Directory-layer prefixes are
allocated tuple integers and the directory metadata lives under `\xFE`, so neither can overlap
them. The storage service never reads or writes outside its directory.

### 3.5 Operations and their semantics

Every conditional write is one FoundationDB transaction that reads the condition key and writes
in the same transaction. FoundationDB's optimistic concurrency makes a concurrent writer's commit
fail with a conflict; the retry re-reads the condition, so exactly one of two racing writers wins,
as with Bigtable's `CheckAndMutateRow`.

| Operation | Bigtable (upstream) | FoundationDB |
|---|---|---|
| `createGroup` | write `g:gr`, `g:ver` if `g:gr` is empty | same condition on `"gr"`; true/false as upstream |
| `updateGroup` | write if `g:ver` == version - 1 | same, on `"ver"` |
| `getGroup` | read the row | one range read of `(group id)` |
| log `append` | write `l:c`, `l:v`, `l:s` if `l:c` is empty | same condition on `"c"` |
| log `getRecordsFromVersion` | row-range read `[from, to)` | key-range read `[from, to)`; same filtering of states (`maxSupportedChangeEpoch`, first/last state) and the same "seen the current version" flag |
| manifest `set` | two CheckAndMutates: `m:ver` == version - 1, else `m:ver` empty | one transaction: `"ver"` absent or == version - 1 |
| manifest `get`, `getIfNotVersion` | read the row (with a value filter) | one range read of `(uuid)`; empty if the stored version equals the one asked for |
| manifest `clear` | delete the row | clear the range `(uuid)` |
| items `set` | bulk mutations of at most 100,000 mutations, in order, not atomic across pages | transactions of at most about 1 MB or 10,000 operations, in order, not atomic across transactions; inserts (clear the item's range, write its chunks) before deletes (clear), as upstream |
| items `get` | multi-row read | parallel range reads in one read transaction, deduplicated, sorted by key |
| items `clear` | read up to 100,000 rows, delete them, repeat | one clear-range of `(uuid)` |

Two FoundationDB-specific points:

- **`commit_unknown_result`.** FoundationDB can report that it does not know whether a commit
  landed (error 1021, "maybe committed"). The retry of such a transaction checks whether the
  stored record is already exactly what it meant to write and, if so, reports success; without
  that, a write that did land would come back as a version conflict (409), and for a manifest the
  items of that request would never be written. First attempts never take that branch, so they
  behave exactly like Bigtable's `CheckAndMutate` (an identical re-sent request still gets its
  409). Bigtable itself surfaces such ambiguous failures as errors.
- **Timeouts.** `transactionTimeout` (default 10 s) bounds a transaction including all its
  retries, so a request fails instead of hanging when the cluster is down.

Group-log reads are paged: one read transaction returns at most 256 key-values and the next page
starts at the first version not yet complete. Log entries are write-once (append only, never
changed), so reading them across several transactions returns the same entries one transaction
would.

### 3.6 Limits, and why they hold

FoundationDB's limits: a value at most 100,000 bytes, a key at most 10,000 bytes, a transaction at
most 10,000,000 bytes of writes, and at most 5 s between a transaction's read version and its last
read or its commit.

The largest data the service accepts, from `storage.yml` (`group.maxGroupSize` 1001,
`maxGroupTitleLengthBytes` 1024, `maxGroupDescriptionLengthBytes` 8192) and the validators
(`GroupValidator`, `GroupChangeApplicator`: members + members pending a profile key <= 1001,
members pending admin approval <= 1001, banned members <= 1001, member label ciphertexts <= 64 and
<= 512 bytes, disappearing-messages timer <= 42 bytes, zkgroup user and profile-key ciphertexts
65 bytes each):

- **Group state.** One member is about 145 bytes without labels and at most about 726 bytes with
  both labels at their maximum. A 1001-member group without labels is about 155 KB: more than one
  value, so it is stored as two chunks. The worst case the validators allow for honest clients
  (1001 members all with maximal labels, 1001 join requests, 1001 bans, maximal title and
  description) is about 0.96 MB: ten chunks, one transaction of about 1 MB. A test builds that
  group and stores, updates, logs and reads it back (3.10).
- **Group log entry.** The state after the change plus the signed change. The largest change
  adds or removes every member at once (about 0.15 MB, at most about 0.73 MB with labels), so one
  append writes at most about 1.7 MB.
- **Group log read.** `GroupsController` asks for at most 64 versions per request
  (`LOG_VERSION_LIMIT`). A page reads at most 256 key-values, at most about 25 MB even if every one
  were a full chunk, and in practice one page holds all 64 versions of a normal group (about 10 MB
  for a 1001-member group). Reads do not count against the 10 MB write limit, and this is far
  inside 5 s on the staging disk.
- **Manifest.** Roughly 22 bytes per record identifier; it passes 100,000 bytes at about 4,500
  records and is chunked from then on. One manifest write can hold about 9.9 MB, about 450,000
  records.
- **Storage items.** `StorageController` allows at most 1,000,000 mutations per write request
  and 5,120 keys per read. Writes are cut into transactions of about 1 MB; a single item can be up
  to about 9.9 MB. A read is one transaction.
- **Keys.** The longest key is an item key. Bigtable row keys are at most 4 KiB and upstream
  hex-encodes the item key into them, so every item key Bigtable accepts is at most 2 KiB, far
  under FoundationDB's 10,000-byte key limit.

What behaves differently at the extremes: a single record (group state, log entry, manifest or
item) above about 9.9 MB is refused by FoundationDB (`transaction_too_large`, the request fails
with 500 and nothing is written), where Bigtable would take a cell of up to 100 MB. Nothing a
client does within the limits above comes near that.

### 3.7 Readiness

`/_ready` keeps upstream's contract (`ReadinessController`): the first `warmup.count` calls read
one row from each of the four tables, later calls answer `ready` straight away, and a failing
read makes the call fail. `FoundationDbReadinessController` does the same with one key-value from
each of the four subdirectories. It is registered instead of upstream's controller when the
backend is FoundationDB; upstream's class is unchanged.

### 3.8 Container

The Java binding (`org.foundationdb:fdb-java` 7.3.76, API version 730, the versions
`swarm-messenger-server` pins) needs the native client library `libfdb_c.so` of the same version
at runtime. As in the chat server's build, `./mvnw package` downloads
`libfdb_c.x86_64.so` 7.3.76 from the FoundationDB release, checks its SHA-256
(`af099848721d08904ff9e5d38fade0de24d92e86451ea491d9ab5ce84adcf62a`, the value the chat
server's `pom.xml` pins) and puts it at `target/jib-extra/usr/lib/libfdb_c.so`. The deployment's
image copies it to `/usr/lib/libfdb_c.so` (and jib, if anyone uses it, adds `target/jib-extra`
to the image). The cluster file is mounted read-only from the FoundationDB volume, as for the chat
server.

### 3.9 Migration from the emulator

`migrate-bigtable-to-foundationdb [--apply] <config.yml>`, a Dropwizard command in this
repository. It reads the four Bigtable tables (the emulator, through `BIGTABLE_EMULATOR_HOST`)
and writes the same records into the FoundationDB layout above.

- Dry run unless `--apply` is given: it reads both sides and prints what it would do.
- Prints per table: rows in the source, records in the target before, copied (or "would copy"),
  already identical, conflicts, unreadable rows, and records in the target after.
- Idempotent: a record that already exists in FoundationDB with identical bytes is left alone;
  a record that exists with different bytes is never overwritten (FoundationDB is then the newer
  side) and is reported as a conflict; the command then exits non-zero.
- Never deletes or changes anything in Bigtable; it only reads it.

### 3.10 Tests

`GroupsManagerTest` and `StorageManagerTest` become abstract tests with the same test methods;
each has a Bigtable subclass (upstream's emulator extension) and a FoundationDB subclass. The
places where upstream reads or seeds raw Bigtable rows go through small backend hooks. The
FoundationDB subclasses need a real `fdbserver`: they run when the system property
`swarm.foundationdb.clusterFile` names a cluster file, are skipped otherwise, and fail instead of
skipping when `swarm.foundationdb.required=true` (as in CI). Further FoundationDB tests: the
largest group and a manifest and an item above 100,000 bytes (chunking), racing conditional
writes, shrinking records, directory isolation, the maybe-committed retry, the readiness
controller, configuration parsing, and the migration command against the Bigtable emulator and
FoundationDB together.

## 4. What did *not* change

- `zkConfig.serverSecret` is still read the same way (`ZkConfiguration`, base64 `byte[]` via
  `ByteArrayAdapter`) — the SWARM deployment sets it to the exact same base64 value as
  `swarm-messenger-server`'s `groupsZkConfig.serverSecret`, byte for byte, no re-encoding.
- `authentication.key` is still read the same way (`AuthenticationConfiguration`, hex string).
  The SWARM deployment derives it from `swarm-messenger-server`'s
  `storageService.userAuthenticationTokenSharedSecret` (base64) by decoding and re-hex-encoding
  the *same bytes* — a text-encoding conversion done once by a host script
  (`deploy/staging/storage/make-storage-env.sh` in `swarm-messenger-server`), never a new
  secret. See that script for exactly how.
- `group.externalServiceSecret` (the group-call token secret) is generated fresh for this
  deployment; it is internal to `storage-service` (mint and verify both happen inside
  `GroupsController`) and is not shared with `swarm-messenger-server`.
- No change to how `cdn.*` (group avatar upload S3 POST policy signing) works; the SWARM
  deployment points it at the same MinIO bucket and key `swarm-messenger-server` already uses
  for CDN0 profile avatars (objects `groups/<group id>/<random>`).
- No Dockerfile and no build change here. The deployment builds the service with the unchanged
  `./mvnw -B -DskipTests package` (a thin jar plus `target/lib/`, upstream's own
  `copy-dependencies` execution) and wraps it in an image defined next to the deployment,
  `deploy/staging/storage/Dockerfile` in `swarm-messenger-server`, whose base image (the digest in
  `pom.xml`'s `docker.image`) and JVM flags mirror this repository's jib configuration. Only the
  heap limit differs: jib's `-Xmx8192m` is sized for Signal's production.

## 5. Branches and tags

- `upstream-main` — upstream `main`, unmodified, for diffing.
- `upstream-<short-sha>` tags — the upstream commit each SWARM rebase/diff was taken from.
- `swarm-main` — default branch; upstream plus the changes on this page.
