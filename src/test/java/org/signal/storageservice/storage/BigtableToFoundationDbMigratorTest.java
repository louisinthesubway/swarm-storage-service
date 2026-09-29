/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.storageservice.auth.User;
import org.signal.storageservice.configuration.BigTableConfiguration;
import org.signal.storageservice.storage.foundationdb.FoundationDbExtension;
import org.signal.storageservice.storage.foundationdb.FoundationDbGroupLogTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbGroupsTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorage;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorageItemsTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorageManifestsTable;
import org.signal.storageservice.storage.protos.contacts.StorageItem;
import org.signal.storageservice.storage.protos.contacts.StorageManifest;
import org.signal.storageservice.storage.protos.groups.Group;
import org.signal.storageservice.storage.protos.groups.GroupChange;
import org.signal.storageservice.storage.protos.groups.Member;
import org.signal.storageservice.workers.BigtableToFoundationDbMigrator;
import org.signal.storageservice.workers.BigtableToFoundationDbMigrator.TableReport;

/// SWARM: `migrate-bigtable-to-foundationdb` against the Bigtable emulator and a real FoundationDB cluster: rows
/// written by upstream's own Bigtable code arrive byte for byte, a dry run writes nothing, a second run changes
/// nothing, newer FoundationDB data is never overwritten, unreadable rows are reported, and Bigtable is left exactly
/// as it was. Needs a FoundationDB cluster (see [FoundationDbExtension]). docs/SWARM-CHANGES.md, section 3.9.
class BigtableToFoundationDbMigratorTest {

  private static final String GROUPS = "swarm_storage_groups";
  private static final String GROUP_LOGS = "swarm_storage_group_logs";
  private static final String MANIFESTS = "swarm_storage_manifests";
  private static final String CONTACTS = "swarm_storage_contacts";

  private static final SecureRandom RANDOM = new SecureRandom();

  @RegisterExtension
  static final FoundationDbExtension FOUNDATION_DB = new FoundationDbExtension();

  @RegisterExtension
  private final BigtableEmulatorExtension bigtableEmulator = BigtableEmulatorExtension.create();

  private BigtableDataClient client;
  private BigTableConfiguration tables;

  @BeforeEach
  void setUp() throws Exception {
    BigtableTableAdminSettings.Builder tableAdminSettings = BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort())
        .setProjectId("foo").setInstanceId("bar");
    try (BigtableTableAdminClient tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings.build())) {
      tableAdminClient.createTable(CreateTableRequest.of(GROUPS).addFamily(GroupsTable.FAMILY));
      tableAdminClient.createTable(CreateTableRequest.of(GROUP_LOGS).addFamily(GroupLogTable.FAMILY));
      tableAdminClient.createTable(CreateTableRequest.of(MANIFESTS).addFamily(StorageManifestsTable.FAMILY));
      tableAdminClient.createTable(CreateTableRequest.of(CONTACTS).addFamily(StorageItemsTable.FAMILY));
    }

    client = BigtableDataClient.create(BigtableDataSettings.newBuilderForEmulator(bigtableEmulator.getPort())
        .setProjectId("foo").setInstanceId("bar").build());

    // the bigtable: block of storage.yml
    tables = new ObjectMapper().convertValue(Map.of(
        "projectId", "foo", "instanceId", "bar",
        "contactManifestsTableId", MANIFESTS, "contactsTableId", CONTACTS,
        "groupsTableId", GROUPS, "groupLogsTableId", GROUP_LOGS), BigTableConfiguration.class);
  }

  @AfterEach
  void tearDown() {
    client.close();
  }

  private static ByteString randomBytes(final int length) {
    final byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return ByteString.copyFrom(bytes);
  }

  private record Seeded(List<ByteString> groupIds, List<User> users, List<List<ByteString>> itemKeys) {
  }

  /// Fills Bigtable through upstream's own table code: groups (one above 100,000 bytes), their logs, manifests and
  /// items (one above 100,000 bytes), plus deletes and updates.
  private Seeded seed() {
    final GroupsManager bigtableGroups = new GroupsManager(client, GROUPS, GROUP_LOGS);
    final List<ByteString> groupIds = new ArrayList<>();

    for (int g = 0; g < 3; g++) {
      final ByteString groupId = randomBytes(32);
      groupIds.add(groupId);

      final Group.Builder group = Group.newBuilder().setPublicKey(randomBytes(97)).setTitle(randomBytes(40)).setVersion(0);
      final int members = g == 0 ? 1001 : 3;
      for (int m = 0; m < members; m++) {
        group.addMembers(Member.newBuilder().setUserId(randomBytes(65)).setProfileKey(randomBytes(65))
            .setRole(Member.Role.DEFAULT));
      }

      Group state = group.build();
      assertThat(bigtableGroups.createGroup(groupId, state).join()).isTrue();
      assertThat(bigtableGroups.appendChangeRecord(groupId, 0,
          GroupChange.newBuilder().setActions(randomBytes(20)).build(), state).join()).isTrue();

      for (int version = 1; version <= 2 + g; version++) {
        state = state.toBuilder().setVersion(version).setTitle(randomBytes(40)).build();
        assertThat(bigtableGroups.updateGroup(groupId, state).join()).isEmpty();
        assertThat(bigtableGroups.appendChangeRecord(groupId, version, GroupChange.newBuilder()
            .setActions(randomBytes(20)).setServerSignature(randomBytes(64)).setChangeEpoch(version % 2).build(), state)
            .join()).isTrue();
      }
    }

    final StorageManager bigtableStorage = new StorageManager(client, MANIFESTS, CONTACTS);
    final List<User> users = new ArrayList<>();
    final List<List<ByteString>> itemKeys = new ArrayList<>();

    for (int u = 0; u < 2; u++) {
      final User user = new User(UUID.randomUUID());
      users.add(user);

      final boolean firstUser = u == 0;
      final List<StorageItem> inserts = new ArrayList<>();
      IntStream.range(0, 20).forEach(i -> inserts.add(StorageItem.newBuilder()
          .setKey(randomBytes(16)).setValue(randomBytes(i == 0 && firstUser ? 250_000 : 150)).build()));
      assertThat(bigtableStorage.set(user, StorageManifest.newBuilder().setVersion(1).setValue(randomBytes(200)).build(),
          inserts, List.of()).join()).isEmpty();

      // version 2 deletes one item and adds one
      final StorageItem added = StorageItem.newBuilder().setKey(randomBytes(16)).setValue(randomBytes(150)).build();
      assertThat(bigtableStorage.set(user, StorageManifest.newBuilder().setVersion(2).setValue(randomBytes(200)).build(),
          List.of(added), List.of(inserts.get(5).getKey())).join()).isEmpty();

      final List<ByteString> keys = new ArrayList<>(inserts.stream().map(StorageItem::getKey).toList());
      keys.add(added.getKey());
      itemKeys.add(keys);
    }

    return new Seeded(groupIds, users, itemKeys);
  }

  private BigtableToFoundationDbMigrator migrator(final ByteArrayOutputStream output) {
    return new BigtableToFoundationDbMigrator(client, tables, FOUNDATION_DB.getStorage(),
        new PrintStream(output, true, StandardCharsets.UTF_8));
  }

  private static String print(final ByteArrayOutputStream output) {
    final String text = output.toString(StandardCharsets.UTF_8);
    System.out.print(text);
    return text;
  }

  /// Every row of every table, with every cell, as text: to prove the source is untouched.
  private String bigtableSnapshot() {
    final StringBuilder snapshot = new StringBuilder();
    for (final String table : List.of(GROUPS, GROUP_LOGS, MANIFESTS, CONTACTS)) {
      for (final Row row : client.readRows(Query.create(TableId.of(table)))) {
        snapshot.append(table).append(' ').append(HexFormat.of().formatHex(row.getKey().toByteArray()));
        for (final RowCell cell : row.getCells()) {
          snapshot.append(' ').append(cell.getFamily()).append(':').append(cell.getQualifier().toStringUtf8())
              .append('@').append(cell.getTimestamp()).append('=').append(cell.getValue().hashCode());
        }
        snapshot.append('\n');
      }
    }
    return snapshot.toString();
  }

  private static TableReport report(final List<TableReport> reports, final String dataSet) {
    return reports.stream().filter(report -> report.dataSet().equals(dataSet)).findFirst().orElseThrow();
  }

  @Test
  void migratesByteForByteIdempotentlyAndLeavesBigtableAlone() {
    final Seeded seeded = seed();
    final String sourceBefore = bigtableSnapshot();

    final FoundationDbStorage storage = FOUNDATION_DB.getStorage();
    final GroupsManager bigtableGroups = new GroupsManager(client, GROUPS, GROUP_LOGS);
    final StorageManager bigtableStorage = new StorageManager(client, MANIFESTS, CONTACTS);
    final GroupsManager foundationDbGroups =
        new GroupsManager(new FoundationDbGroupsTable(storage), new FoundationDbGroupLogTable(storage));
    final StorageManager foundationDbStorage =
        new StorageManager(new FoundationDbStorageManifestsTable(storage), new FoundationDbStorageItemsTable(storage));

    // rows: 3 groups; 3 + 4 + 5 = 12 log entries; 2 manifests; 2 x (20 - 1 + 1) = 40 items
    final Map<String, Long> rows = Map.of(FoundationDbStorage.GROUPS, 3L, FoundationDbStorage.GROUP_LOGS, 12L,
        FoundationDbStorage.STORAGE_MANIFESTS, 2L, FoundationDbStorage.STORAGE_ITEMS, 40L);

    // 1. dry run: reports what it would copy, writes nothing
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    List<TableReport> reports = migrator(output).run(false);
    assertThat(print(output)).contains("DRY RUN").contains("OK:");
    rows.forEach((dataSet, count) -> {
      final TableReport report = report(reports, dataSet);
      assertThat(report.sourceRows()).as(dataSet).isEqualTo(count);
      assertThat(report.targetBefore()).as(dataSet).isZero();
      assertThat(report.copied()).as(dataSet).isEqualTo(count);
      assertThat(report.identical() + report.conflicts() + report.unreadable()).as(dataSet).isZero();
      assertThat(report.targetAfter()).as(dataSet).isEqualTo(-1);
    });
    assertThat(foundationDbGroups.getGroup(seeded.groupIds().getFirst()).join()).isEmpty();
    assertThat(foundationDbStorage.getManifest(seeded.users().getFirst()).join()).isEmpty();

    // 2. apply: everything arrives
    output = new ByteArrayOutputStream();
    final List<TableReport> applied = migrator(output).run(true);
    assertThat(print(output)).contains("APPLY").contains("OK:");
    rows.forEach((dataSet, count) -> {
      final TableReport report = report(applied, dataSet);
      assertThat(report.copied()).as(dataSet).isEqualTo(count);
      assertThat(report.targetAfter()).as(dataSet).isEqualTo(count);
    });

    // ... and reads back through the FoundationDB backend exactly as through Bigtable
    for (final ByteString groupId : seeded.groupIds()) {
      final Group group = bigtableGroups.getGroup(groupId).join().orElseThrow();
      assertThat(foundationDbGroups.getGroup(groupId).join()).contains(group);
      assertThat(foundationDbGroups.getChangeRecords(groupId, group, null, false, false, 0, group.getVersion() + 1).join())
          .isEqualTo(bigtableGroups.getChangeRecords(groupId, group, null, false, false, 0, group.getVersion() + 1).join());
      assertThat(foundationDbGroups.getChangeRecords(groupId, group, 0, true, true, 0, group.getVersion() + 1).join())
          .isEqualTo(bigtableGroups.getChangeRecords(groupId, group, 0, true, true, 0, group.getVersion() + 1).join());
    }
    for (int u = 0; u < seeded.users().size(); u++) {
      final User user = seeded.users().get(u);
      assertThat(foundationDbStorage.getManifest(user).join()).isEqualTo(bigtableStorage.getManifest(user).join());
      assertThat(foundationDbStorage.getItems(user, seeded.itemKeys().get(u)).join())
          .isEqualTo(bigtableStorage.getItems(user, seeded.itemKeys().get(u)).join())
          .hasSize(20);
    }

    // 3. a second run changes nothing
    output = new ByteArrayOutputStream();
    final List<TableReport> again = migrator(output).run(true);
    print(output);
    rows.forEach((dataSet, count) -> {
      final TableReport report = report(again, dataSet);
      assertThat(report.copied()).as(dataSet).isZero();
      assertThat(report.identical()).as(dataSet).isEqualTo(count);
      assertThat(report.targetAfter()).as(dataSet).isEqualTo(count);
    });

    // 4. newer data in FoundationDB is never overwritten; an unreadable row is reported, not copied
    final ByteString changedGroupId = seeded.groupIds().get(1);
    final Group current = foundationDbGroups.getGroup(changedGroupId).join().orElseThrow();
    final Group newer = current.toBuilder().setVersion(current.getVersion() + 1).setTitle(randomBytes(40)).build();
    assertThat(foundationDbGroups.updateGroup(changedGroupId, newer).join()).isEmpty();
    client.mutateRow(RowMutation.create(TableId.of(MANIFESTS), "not-a-uuid#manifest",
        Mutation.create().setCell(StorageManifestsTable.FAMILY, StorageManifestsTable.COLUMN_VERSION, "1")
            .setCell(StorageManifestsTable.FAMILY, StorageManifestsTable.COLUMN_DATA, "x")));
    final String sourceWithExtraRow = bigtableSnapshot();

    output = new ByteArrayOutputStream();
    final List<TableReport> withConflict = migrator(output).run(true);
    assertThat(print(output)).contains("ATTENTION");
    assertThat(report(withConflict, FoundationDbStorage.GROUPS).conflicts()).isEqualTo(1);
    assertThat(report(withConflict, FoundationDbStorage.GROUPS).identical()).isEqualTo(2);
    assertThat(report(withConflict, FoundationDbStorage.STORAGE_MANIFESTS).unreadable()).isEqualTo(1);
    assertThat(foundationDbGroups.getGroup(changedGroupId).join()).contains(newer);

    // Bigtable was only ever read: after four runs it holds exactly the seeded rows plus the row added above
    assertThat(bigtableSnapshot()).isEqualTo(sourceWithExtraRow);
    final String extraRow = MANIFESTS + " " + HexFormat.of().formatHex("not-a-uuid#manifest".getBytes(StandardCharsets.UTF_8)) + " ";
    assertThat(sourceWithExtraRow.lines().filter(line -> !line.startsWith(extraRow)).map(line -> line + "\n")
        .reduce("", String::concat)).isEqualTo(sourceBefore);
  }

  /// The command's dry run opens the FoundationDB directory read-only ([FoundationDbStorage#openIfExists]) and, when
  /// it does not exist yet, compares with nothing: it must report the same counts as against an empty directory and
  /// leave the cluster without the directory.
  @Test
  void dryRunAgainstAMissingDirectoryWritesNothing() {
    seed();

    final Database database = FOUNDATION_DB.getDatabase();
    final List<String> missing = List.of(FoundationDbExtension.TEST_ROOT, UUID.randomUUID().toString());
    assertThat(FoundationDbStorage.openIfExists(database, missing, false).join()).isEmpty();

    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final List<TableReport> reports = new BigtableToFoundationDbMigrator(client, tables, null, missing,
        new PrintStream(output, true, StandardCharsets.UTF_8)).run(false);
    assertThat(print(output)).contains("DRY RUN").contains("does not exist yet").contains("OK:");

    Map.of(FoundationDbStorage.GROUPS, 3L, FoundationDbStorage.GROUP_LOGS, 12L,
        FoundationDbStorage.STORAGE_MANIFESTS, 2L, FoundationDbStorage.STORAGE_ITEMS, 40L).forEach((dataSet, count) -> {
      final TableReport report = report(reports, dataSet);
      assertThat(report.sourceRows()).as(dataSet).isEqualTo(count);
      assertThat(report.targetBefore()).as(dataSet).isZero();
      assertThat(report.copied()).as(dataSet).isEqualTo(count);
      assertThat(report.identical() + report.conflicts() + report.unreadable()).as(dataSet).isZero();
      assertThat(report.targetAfter()).as(dataSet).isEqualTo(-1);
    });

    // nothing was created
    assertThat(DirectoryLayer.getDefault().exists(database, missing).join()).isFalse();

    // a directory with one of its four subdirectories missing counts as missing, and is left as it is
    final List<String> partial = List.of(FoundationDbExtension.TEST_ROOT, UUID.randomUUID().toString());
    DirectoryLayer.getDefault().createOrOpen(database, Stream.concat(partial.stream(), Stream.of(FoundationDbStorage.GROUPS)).toList()).join();
    try {
      assertThat(FoundationDbStorage.openIfExists(database, partial, false).join()).isEmpty();
      assertThat(DirectoryLayer.getDefault().exists(database,
          Stream.concat(partial.stream(), Stream.of(FoundationDbStorage.GROUP_LOGS)).toList()).join()).isFalse();
    } finally {
      DirectoryLayer.getDefault().removeIfExists(database, partial).join();
    }

    // an existing directory opens read-only with the same four subdirectories
    final FoundationDbStorage existing = FoundationDbStorage.openIfExists(database, FOUNDATION_DB.getDirectory(), false)
        .join().orElseThrow();
    final FoundationDbStorage created = FOUNDATION_DB.getStorage();
    assertThat(existing.getGroups().getKey()).isEqualTo(created.getGroups().getKey());
    assertThat(existing.getGroupLogs().getKey()).isEqualTo(created.getGroupLogs().getKey());
    assertThat(existing.getStorageManifests().getKey()).isEqualTo(created.getStorageManifests().getKey());
    assertThat(existing.getStorageItems().getKey()).isEqualTo(created.getStorageItems().getKey());

    // copying needs the directory
    assertThatThrownBy(() -> new BigtableToFoundationDbMigrator(client, tables, null, missing,
        new PrintStream(OutputStream.nullOutputStream())).run(true))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
