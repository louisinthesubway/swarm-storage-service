/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import javax.annotation.Nullable;

/// SWARM: the `storage:` block of the configuration, which selects where groups, group logs, storage manifests and
/// storage items live. Without the block the service uses Bigtable, exactly as upstream does. See
/// docs/SWARM-CHANGES.md, section 3.
///
/// ```yaml
/// storage:
///   backend: foundationdb        # or bigtable (the default)
///   foundationdb:
///     clusterFile: /etc/foundationdb/fdb.cluster
///     directory: [swarm-storage-service]
/// ```
public class StorageBackendConfiguration {

  public enum Backend {
    BIGTABLE,
    FOUNDATIONDB
  }

  @JsonProperty
  @NotNull
  private Backend backend = Backend.BIGTABLE;

  @JsonProperty
  @Valid
  @Nullable
  private FoundationDbConfiguration foundationdb;

  public StorageBackendConfiguration() {
  }

  public StorageBackendConfiguration(final Backend backend, @Nullable final FoundationDbConfiguration foundationdb) {
    this.backend = backend;
    this.foundationdb = foundationdb;
  }

  public Backend getBackend() {
    return backend;
  }

  @Nullable
  public FoundationDbConfiguration getFoundationDbConfiguration() {
    return foundationdb;
  }
}
