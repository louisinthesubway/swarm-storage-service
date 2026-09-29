/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.dropwizard.configuration.ConfigurationValidationException;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.signal.storageservice.StorageServiceConfiguration;

/// SWARM: the `storage:` block. Without it the service uses Bigtable as upstream does; `backend: foundationdb` needs
/// `storage.foundationdb` and makes `bigtable:` optional. docs/SWARM-CHANGES.md, section 3.2.
class StorageBackendConfigurationTest {

  // Every block upstream requires, with placeholder values (no real secret anywhere).
  private static final String COMMON = """
      authentication:
        key: "0000000000000000000000000000000000000000000000000000000000000000"
      zkConfig:
        serverSecret: "AAAA"
      cdn:
        accessKey: test
        accessSecret: test
        bucket: test
        region: us-east-1
      group:
        maxGroupSize: 1001
        maxGroupTitleLengthBytes: 1024
        maxGroupDescriptionLengthBytes: 8192
        externalServiceSecret: "0000000000000000000000000000000000000000000000000000000000000000"
      openTelemetry:
        enabled: false
        environment: test
        maxBucketCount: 100
        logUrl: ""
        maxBucketsPerMeter: {}
      """;

  private static final String BIGTABLE = """
      bigtable:
        projectId: p
        instanceId: i
        contactManifestsTableId: m
        contactsTableId: c
        groupsTableId: g
        groupLogsTableId: l
      """;

  private static StorageServiceConfiguration parse(final String yaml) throws Exception {
    return new YamlConfigurationFactory<>(StorageServiceConfiguration.class, Validators.newValidator(),
        Jackson.newObjectMapper(), "dw")
        .build(path -> new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "storage.yml");
  }

  @Test
  void withoutAStorageBlockTheBackendIsBigtable() throws Exception {
    final StorageServiceConfiguration configuration = parse(COMMON + BIGTABLE);

    assertThat(configuration.getStorageBackendConfiguration().getBackend())
        .isEqualTo(StorageBackendConfiguration.Backend.BIGTABLE);
    assertThat(configuration.getBigTableConfiguration().getGroupsTableId()).isEqualTo("g");
  }

  @Test
  void bigtableNeedsTheBigtableBlock() {
    assertThatThrownBy(() -> parse(COMMON)).isInstanceOf(ConfigurationValidationException.class)
        .hasMessageContaining("storageBackendConfigured");
    assertThatThrownBy(() -> parse(COMMON + "storage:\n  backend: bigtable\n"))
        .isInstanceOf(ConfigurationValidationException.class);
  }

  @Test
  void foundationDbWithDefaults() throws Exception {
    final StorageServiceConfiguration configuration = parse(COMMON + """
        storage:
          backend: foundationdb
          foundationdb:
            clusterFile: /etc/foundationdb/fdb.cluster
            directory: [swarm-storage-service]
        """);

    assertThat(configuration.getStorageBackendConfiguration().getBackend())
        .isEqualTo(StorageBackendConfiguration.Backend.FOUNDATIONDB);
    assertThat(configuration.getBigTableConfiguration()).isNull();

    final FoundationDbConfiguration foundationDb = configuration.getStorageBackendConfiguration().getFoundationDbConfiguration();
    assertThat(foundationDb.clusterFile()).isEqualTo("/etc/foundationdb/fdb.cluster");
    assertThat(foundationDb.directory()).isEqualTo(List.of("swarm-storage-service"));
    assertThat(foundationDb.transactionTimeout()).isEqualTo(FoundationDbConfiguration.DEFAULT_TRANSACTION_TIMEOUT);
    assertThat(foundationDb.transactionRetryLimit()).isEqualTo(FoundationDbConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT);
  }

  @Test
  void foundationDbWithBigtableKeptForTheMigration() throws Exception {
    final StorageServiceConfiguration configuration = parse(COMMON + BIGTABLE + """
        storage:
          backend: FoundationDB
          foundationdb:
            clusterFile: /etc/foundationdb/fdb.cluster
            directory: [swarm, storage-service]
            transactionTimeout: PT3S
            transactionRetryLimit: 7
        """);

    assertThat(configuration.getStorageBackendConfiguration().getBackend())
        .isEqualTo(StorageBackendConfiguration.Backend.FOUNDATIONDB);
    assertThat(configuration.getBigTableConfiguration().getContactsTableId()).isEqualTo("c");

    final FoundationDbConfiguration foundationDb = configuration.getStorageBackendConfiguration().getFoundationDbConfiguration();
    assertThat(foundationDb.directory()).isEqualTo(List.of("swarm", "storage-service"));
    assertThat(foundationDb.transactionTimeout()).isEqualTo(Duration.ofSeconds(3));
    assertThat(foundationDb.transactionRetryLimit()).isEqualTo(7);
  }

  @Test
  void foundationDbNeedsItsBlockAndAClusterFileAndADirectory() {
    assertThatThrownBy(() -> parse(COMMON + BIGTABLE + "storage:\n  backend: foundationdb\n"))
        .isInstanceOf(ConfigurationValidationException.class)
        .hasMessageContaining("storageBackendConfigured");

    assertThatThrownBy(() -> parse(COMMON + """
        storage:
          backend: foundationdb
          foundationdb:
            clusterFile: ""
            directory: []
        """))
        .isInstanceOf(ConfigurationValidationException.class)
        .hasMessageContaining("clusterFile")
        .hasMessageContaining("directory");
  }
}
