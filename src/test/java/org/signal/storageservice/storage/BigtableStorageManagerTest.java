/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

/// SWARM: [StorageManagerTest] on Bigtable, with upstream's emulator setup and upstream's raw rows.
class BigtableStorageManagerTest extends StorageManagerTest {

  private static final String CONTACTS_TABLE_NAME = "test-table";
  private static final TableId CONTACTS_TABLE_ID = TableId.of(CONTACTS_TABLE_NAME);

  private static final String MANIFESTS_TABLE_NAME = "manifest-table";
  private static final TableId MANIFESTS_TABLE_ID = TableId.of(MANIFESTS_TABLE_NAME);

  @RegisterExtension
  public final BigtableEmulatorExtension bigtableEmulator = BigtableEmulatorExtension.create();

  private BigtableDataClient client;

  @BeforeEach
  void setup() throws IOException {
    BigtableTableAdminSettings.Builder tableAdminSettings =
        BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort())
            .setProjectId("foo")
            .setInstanceId("bar");

    try (BigtableTableAdminClient tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings.build())) {

      tableAdminClient.createTable(CreateTableRequest.of(CONTACTS_TABLE_NAME).addFamily(StorageItemsTable.FAMILY));
      tableAdminClient.createTable(CreateTableRequest.of(MANIFESTS_TABLE_NAME).addFamily(StorageManifestsTable.FAMILY));

      BigtableDataSettings.Builder dataSettings = BigtableDataSettings.newBuilderForEmulator(bigtableEmulator.getPort())
          .setProjectId("foo")
          .setInstanceId("bar");

      client = BigtableDataClient.create(dataSettings.build());
    }
  }

  @AfterEach
  void tearDown() {
    client.close();
  }

  @Override
  protected StorageManager storageManager() {
    return new StorageManager(client, MANIFESTS_TABLE_NAME, CONTACTS_TABLE_NAME);
  }

  @Override
  protected StorageManager storageManagerWithFailingReads(final RuntimeException failure) {
    BigtableDataClient client = mock(BigtableDataClient.class);
    when(client.readRowAsync(any(TableId.class), any(ByteString.class))).thenReturn(
        ApiFutures.immediateFailedFuture(failure));

    return new StorageManager(client, MANIFESTS_TABLE_NAME, CONTACTS_TABLE_NAME);
  }

  @Override
  protected void writeRawManifest(final UUID userId, final String version, final String data) {
    client.mutateRow(RowMutation.create(MANIFESTS_TABLE_ID, userId + "#manifest",
        Mutation.create()
            .setCell(StorageManifestsTable.FAMILY, StorageManifestsTable.COLUMN_VERSION, version)
            .setCell(StorageManifestsTable.FAMILY, StorageManifestsTable.COLUMN_DATA, data)));
  }

  @Override
  protected void writeRawItems(final UUID userId, final List<Map.Entry<String, String>> keysAndData) {
    BulkMutation bulkMutation = BulkMutation.create(CONTACTS_TABLE_ID);

    for (final Map.Entry<String, String> item : keysAndData) {
      // Each setCell() is a mutation
      if (bulkMutation.getEntryCount() * StorageItemsTable.MUTATIONS_PER_INSERT >= StorageItemsTable.MAX_MUTATIONS) {
        client.bulkMutateRows(bulkMutation);
        bulkMutation = BulkMutation.create(CONTACTS_TABLE_ID);
      }

      bulkMutation.add(userId + "#contact#" + item.getKey(),
          Mutation.create()
              .setCell(StorageItemsTable.FAMILY, StorageItemsTable.COLUMN_DATA, item.getValue())
              .setCell(StorageItemsTable.FAMILY, StorageItemsTable.COLUMN_KEY, item.getKey()));
    }

    if (bulkMutation.getEntryCount() > 0) {
      client.bulkMutateRows(bulkMutation);
    }
  }

  @Override
  protected List<String> rawItemData(final UUID userId) {
    final List<String> data = new ArrayList<>();

    for (Row row : client.readRows(Query.create(CONTACTS_TABLE_ID).prefix(userId + "#contact#"))) {
      List<RowCell> cells = row.getCells(StorageItemsTable.FAMILY, StorageItemsTable.COLUMN_DATA);
      assertThat(cells.size()).isEqualTo(1);
      data.add(cells.getFirst().getValue().toStringUtf8());
    }

    return data;
  }

  @Override
  protected long countRawItems(final UUID userId) {
    return client.readRows(Query.create(CONTACTS_TABLE_ID).prefix(userId + "#contact#")).stream().count();
  }
}
