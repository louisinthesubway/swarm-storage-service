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
[`docs/STAGING.md`](https://github.com/Swarm-Official/swarm-messenger-server/blob/swarm-main/docs/STAGING.md),
section "Storage service and groups". This file records only the code changes.

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

SWARM's self-hosted staging deployment has no Google Cloud project and runs the standard
Bigtable emulator (`gcloud beta emulators bigtable start`, in the `google/cloud-sdk` container
image) as its own service instead. At startup, `StorageService.run()` now reads the
`BIGTABLE_EMULATOR_HOST` environment variable (the same variable name `gcloud`'s own emulator
tooling sets when you `eval $(gcloud beta emulators bigtable env-init)`, and the variable this
repository's own `EmulatorSpanner`/Bigtable emulator wrapper's convention follows): if it is
set to `host:port`, `BigtableDataSettings.newBuilderForEmulator(host, port)` is used; otherwise
behaviour is byte-for-byte identical to upstream (`BigtableDataSettings.newBuilder()`).

**Consequence, staging only:** the Bigtable emulator keeps every table in memory. Every group
and every synced settings/contacts record is lost when the `bigtable` container restarts. This
is called out in `swarm-messenger-server`'s `docs/STAGING.md` and is not a change anyone should
carry into a durable deployment — see `docs/STAGING.md` section 5c there for the fork-the-storage
option that would fix it, out of scope for this milestone.

## 3. What did *not* change

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
  deployment points it at the same MinIO bucket `swarm-messenger-server` already uses for CDN0
  avatars.
- No Dockerfile is added here. The image is built directly with the `jib-maven-plugin`
  configuration already in `pom.xml` (`./mvnw -DskipTests compile jib:dockerBuild
  -Dimage=swarm-messenger/storage-service:staging`), which upstream itself uses to publish
  images (`.github/workflows/gcr-push.yml`) — this fork only changes the target (a local Docker
  daemon on the staging host instead of a GCR registry), not the mechanism.

## 4. Branches and tags

- `upstream-main` — upstream `main`, unmodified, for diffing.
- `upstream-<short-sha>` tags — the upstream commit each SWARM rebase/diff was taken from.
- `swarm-main` — default branch; upstream plus the changes on this page.
