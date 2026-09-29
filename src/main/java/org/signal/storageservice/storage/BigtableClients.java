/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.signal.storageservice.configuration.BigTableConfiguration;

/// SWARM: builds the Bigtable data client for the service and for the migration command
/// (`migrate-bigtable-to-foundationdb`).
///
/// A self-hosted deployment has no Google Cloud project, so when `BIGTABLE_EMULATOR_HOST` is set (`host:port`) the
/// client talks to that emulator (docs/STAGING.md in swarm-messenger-server, section 5c) instead of the real Bigtable
/// Data API that upstream's `BigtableDataSettings.newBuilder()` always dials. `newBuilderForEmulator()` is upstream's
/// own supported entry point for this (used by `BigtableEmulatorExtension` in this repository's tests). Without the
/// variable the settings are upstream's. See docs/SWARM-CHANGES.md, section 2.
public final class BigtableClients {

  public static final String EMULATOR_HOST_ENVIRONMENT_VARIABLE = "BIGTABLE_EMULATOR_HOST";

  private BigtableClients() {
  }

  public static BigtableDataClient create(final BigTableConfiguration configuration) throws IOException {
    final String bigtableEmulatorHost = System.getenv(EMULATOR_HOST_ENVIRONMENT_VARIABLE);
    final BigtableDataSettings.Builder bigtableDataSettingsBuilder;
    if (StringUtils.isNotBlank(bigtableEmulatorHost)) {
      final String[] hostAndPort = bigtableEmulatorHost.split(":", 2);
      if (hostAndPort.length != 2) {
        throw new IllegalArgumentException(
            EMULATOR_HOST_ENVIRONMENT_VARIABLE + " must be host:port, was: " + bigtableEmulatorHost);
      }
      bigtableDataSettingsBuilder = BigtableDataSettings.newBuilderForEmulator(hostAndPort[0],
          Integer.parseInt(hostAndPort[1]));
    } else {
      bigtableDataSettingsBuilder = BigtableDataSettings.newBuilder();
    }

    return BigtableDataClient.create(bigtableDataSettingsBuilder
        .setProjectId(configuration.getProjectId())
        .setInstanceId(configuration.getInstanceId())
        .build());
  }
}
