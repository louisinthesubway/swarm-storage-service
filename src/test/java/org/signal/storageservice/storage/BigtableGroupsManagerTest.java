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
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.storageservice.util.Conversions;

/// SWARM: [GroupsManagerTest] on Bigtable, with upstream's emulator setup and upstream's raw-row checks (one cell per
/// column).
class BigtableGroupsManagerTest extends GroupsManagerTest {

  private static final String GROUPS_TABLE_NAME = "groups-table";
  private static final TableId GROUPS_TABLE_ID = TableId.of(GROUPS_TABLE_NAME);

  private static final String GROUP_LOGS_TABLE_NAME = "group-logs-table";
  private static final TableId GROUP_LOGS_TABLE_ID = TableId.of(GROUP_LOGS_TABLE_NAME);

  @RegisterExtension
  private final BigtableEmulatorExtension bigtableEmulator = BigtableEmulatorExtension.create();

  private BigtableDataClient client;

  @BeforeEach
  void setup() throws IOException {
    BigtableTableAdminSettings.Builder tableAdminSettings = BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort()).setProjectId("foo").setInstanceId("bar");
    try (BigtableTableAdminClient tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings.build())) {

      BigtableDataSettings.Builder dataSettings = BigtableDataSettings.newBuilderForEmulator(bigtableEmulator.getPort())
          .setProjectId("foo").setInstanceId("bar");
      client = BigtableDataClient.create(dataSettings.build());

      tableAdminClient.createTable(CreateTableRequest.of(GROUPS_TABLE_NAME).addFamily(GroupsTable.FAMILY));
      tableAdminClient.createTable(CreateTableRequest.of(GROUP_LOGS_TABLE_NAME).addFamily(GroupLogTable.FAMILY));
    }
  }

  @AfterEach
  void teardown() {
    client.close();
  }

  @Override
  protected GroupsManager groupsManager() {
    return new GroupsManager(client, GROUPS_TABLE_NAME, GROUP_LOGS_TABLE_NAME);
  }

  @Override
  protected GroupsManager groupsManagerWithFailingReads(final RuntimeException failure) {
    BigtableDataClient client = mock(BigtableDataClient.class);
    when(client.readRowAsync(any(TableId.class), any(ByteString.class)))
        .thenReturn(ApiFutures.immediateFailedFuture(failure));

    return new GroupsManager(client, GROUPS_TABLE_NAME, GROUP_LOGS_TABLE_NAME);
  }

  @Override
  protected Optional<StoredGroup> storedGroup(final ByteString groupId) {
    Row row = client.readRow(GROUPS_TABLE_ID, groupId);
    if (row == null) {
      return Optional.empty();
    }

    List<RowCell> versionCells = row.getCells(GroupsTable.FAMILY, GroupsTable.COLUMN_VERSION);
    assertThat(versionCells.size()).isEqualTo(1);

    List<RowCell> dataCells = row.getCells(GroupsTable.FAMILY, GroupsTable.COLUMN_GROUP_DATA);
    assertThat(dataCells.size()).isEqualTo(1);

    return Optional.of(new StoredGroup(versionCells.getFirst().getValue().toStringUtf8(), dataCells.getFirst().getValue()));
  }

  @Override
  protected Optional<StoredLogEntry> storedLogEntry(final ByteString groupId, final int version) {
    Row row = client.readRow(GROUP_LOGS_TABLE_ID, groupId.concat(ByteString.copyFromUtf8("#")).concat(ByteString.copyFrom(Conversions.intToByteArray(version))));
    if (row == null) {
      return Optional.empty();
    }

    List<RowCell> versionCells = row.getCells(GroupLogTable.FAMILY, GroupLogTable.COLUMN_VERSION);
    assertThat(versionCells.size()).isEqualTo(1);

    List<RowCell> dataCells = row.getCells(GroupLogTable.FAMILY, GroupLogTable.COLUMN_CHANGE);
    assertThat(dataCells.size()).isEqualTo(1);

    List<RowCell> groupStateCells = row.getCells(GroupLogTable.FAMILY, GroupLogTable.COLUMN_STATE);
    assertThat(groupStateCells.size()).isEqualTo(1);

    return Optional.of(new StoredLogEntry(versionCells.getFirst().getValue().toStringUtf8(),
        dataCells.getFirst().getValue(), groupStateCells.getFirst().getValue()));
  }
}
