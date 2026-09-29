/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vdurmont.semver4j.Semver;
import io.dropwizard.core.Configuration;
import org.signal.storageservice.configuration.AuthenticationConfiguration;
import org.signal.storageservice.configuration.BigTableConfiguration;
import org.signal.storageservice.configuration.CdnConfiguration;
import org.signal.storageservice.configuration.OpenTelemetryConfiguration;
import org.signal.storageservice.configuration.GroupConfiguration;
import org.signal.storageservice.configuration.StorageBackendConfiguration;
import org.signal.storageservice.configuration.WarmupConfiguration;
import org.signal.storageservice.configuration.ZkConfiguration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.signal.storageservice.util.ua.ClientPlatform;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class StorageServiceConfiguration extends Configuration {

  // SWARM: required only when storage.backend is bigtable (the default); see isStorageBackendConfigured() below and
  // docs/SWARM-CHANGES.md, section 3.
  @JsonProperty
  @Valid
  private BigTableConfiguration bigtable;

  // SWARM: selects the backend that keeps groups, group logs and storage records; Bigtable when absent.
  @JsonProperty
  @Valid
  @NotNull
  private StorageBackendConfiguration storage = new StorageBackendConfiguration();

  @JsonProperty
  @Valid
  @NotNull
  private AuthenticationConfiguration authentication;

  @JsonProperty
  @Valid
  @NotNull
  private ZkConfiguration zkConfig;

  @JsonProperty
  @Valid
  @NotNull
  private CdnConfiguration cdn;

  @JsonProperty
  @Valid
  @NotNull
  private GroupConfiguration group;

  @JsonProperty
  @Valid
  @NotNull
  private OpenTelemetryConfiguration openTelemetry;

  @JsonProperty
  @Valid
  @NotNull
  private WarmupConfiguration warmup = new WarmupConfiguration(5);

  @JsonProperty
  @NotNull
  private Map<ClientPlatform, Set<Semver>> recognizedClientVersions = Collections.emptyMap();

  public BigTableConfiguration getBigTableConfiguration() {
    return bigtable;
  }

  public StorageBackendConfiguration getStorageBackendConfiguration() {
    return storage;
  }

  // SWARM: the selected backend must be configured.
  @AssertTrue(message = "storage.backend bigtable needs the bigtable block; storage.backend foundationdb needs storage.foundationdb")
  public boolean isStorageBackendConfigured() {
    if (storage == null || storage.getBackend() == null) {
      return true; // reported by @NotNull
    }

    return switch (storage.getBackend()) {
      case BIGTABLE -> bigtable != null;
      case FOUNDATIONDB -> storage.getFoundationDbConfiguration() != null;
    };
  }

  public AuthenticationConfiguration getAuthenticationConfiguration() {
    return authentication;
  }

  public ZkConfiguration getZkConfiguration() {
    return zkConfig;
  }

  public CdnConfiguration getCdnConfiguration() {
    return cdn;
  }

  public GroupConfiguration getGroupConfiguration() {
    return group;
  }

  public OpenTelemetryConfiguration getOpenTelemetryConfiguration() {
    return openTelemetry;
  }

  public WarmupConfiguration getWarmUpConfiguration() {
    return warmup;
  }

  public Map<ClientPlatform, Set<Semver>> getRecognizedClientVersions() {
    return recognizedClientVersions;
  }
}
