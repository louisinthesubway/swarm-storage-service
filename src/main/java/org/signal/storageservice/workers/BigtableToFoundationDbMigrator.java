/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.workers;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.signal.storageservice.configuration.BigTableConfiguration;
import org.signal.storageservice.storage.GroupLogTable;
import org.signal.storageservice.storage.GroupsTable;
import org.signal.storageservice.storage.StorageItemsTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbGroupLogTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbGroupsTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbRecords;
import org.signal.storageservice.storage.foundationdb.FoundationDbRecords.Columns;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorage;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorageItemsTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorageManifestsTable;
import org.signal.storageservice.util.Conversions;

/// SWARM: copies the storage service's four Bigtable tables into the FoundationDB layout of docs/SWARM-CHANGES.md,
/// section 3.4. Section 3.9 there describes the command around it.
///
/// - Reads Bigtable only; never writes or deletes anything there.
/// - Dry run unless `apply`: the target is read, never written. A dry run gets no target at all when the
///   FoundationDB directory does not exist yet (the command opens it read-only, [FoundationDbStorage#openIfExists]);
///   then every readable row counts as "would copy".
/// - A record missing in FoundationDB is copied. A record that exists with exactly the same bytes is counted as
///   identical and left alone. A record that exists with different bytes is a conflict and is never overwritten
///   (FoundationDB is then the newer side). So running it again changes nothing.
/// - Reports counts only, never keys or contents (they are user and group identifiers).
public class BigtableToFoundationDbMigrator {

  /// Records per FoundationDB transaction, at most...
  private static final int BATCH_RECORDS = 200;
  /// ... and about this many bytes of record data (a single larger record goes alone).
  private static final int BATCH_BYTES = 1_000_000;
  /// Key-values per read transaction while counting records.
  private static final int COUNT_PAGE_KEY_VALUES = 10_000;

  private static final String MANIFEST_ROW_SUFFIX = "#manifest";
  private static final String ITEM_ROW_INFIX = "#" + StorageItemsTable.ROW_KEY + "#";

  // Column families and names of upstream's table classes (the manifest ones are package-private there).
  private static final String GROUPS_FAMILY = GroupsTable.FAMILY;
  private static final String GROUP_LOGS_FAMILY = GroupLogTable.FAMILY;
  private static final String MANIFESTS_FAMILY = "m";
  private static final String ITEMS_FAMILY = StorageItemsTable.FAMILY;

  public enum Outcome {
    COPIED,
    IDENTICAL,
    CONFLICT
  }

  /// Counts for one data set.
  public static final class TableReport {

    private final String dataSet;
    private final String bigtableTable;
    long sourceRows;
    long targetBefore;
    long copied;
    long identical;
    long conflicts;
    long unreadable;
    long targetAfter = -1;

    TableReport(final String dataSet, final String bigtableTable) {
      this.dataSet = dataSet;
      this.bigtableTable = bigtableTable;
    }

    public String dataSet() { return dataSet; }
    public String bigtableTable() { return bigtableTable; }
    public long sourceRows() { return sourceRows; }
    public long targetBefore() { return targetBefore; }
    public long copied() { return copied; }
    public long identical() { return identical; }
    public long conflicts() { return conflicts; }
    public long unreadable() { return unreadable; }
    /// @return the record count after copying, or -1 for a dry run
    public long targetAfter() { return targetAfter; }
  }

  private record TargetRecord(Tuple prefix, Columns columns, long bytes) {
  }

  /// How one Bigtable table maps onto one FoundationDB subdirectory.
  /// `subspace` is null for a dry run against a directory that does not exist yet.
  private record DataSet(String name, String bigtableTable, @Nullable Subspace subspace,
                         RowConverter converter, Predicate<Tuple> recordMarker) {
  }

  @FunctionalInterface
  private interface RowConverter {

    /// @return the record for the row, or empty if the row cannot be read
    Optional<TargetRecord> convert(Row row);
  }

  private final BigtableDataClient source;
  private final BigTableConfiguration sourceTables;
  @Nullable
  private final FoundationDbStorage target;
  private final List<String> targetDirectory;
  private final PrintStream out;

  public BigtableToFoundationDbMigrator(final BigtableDataClient source, final BigTableConfiguration sourceTables,
      final FoundationDbStorage target, final PrintStream out) {

    this(source, sourceTables, target, target.getDirectory(), out);
  }

  /// @param target          the FoundationDB storage, or `null` for a dry run against a directory that does not
  ///                        exist yet
  /// @param targetDirectory the configured FoundationDB directory, for the report
  public BigtableToFoundationDbMigrator(final BigtableDataClient source, final BigTableConfiguration sourceTables,
      @Nullable final FoundationDbStorage target, final List<String> targetDirectory, final PrintStream out) {

    this.source = source;
    this.sourceTables = sourceTables;
    this.target = target;
    this.targetDirectory = List.copyOf(targetDirectory);
    this.out = out;
  }

  /// Runs the migration (or the dry run) over all four data sets and prints a report.
  ///
  /// @return one report per data set, in the order groups, group logs, storage manifests, storage items
  public List<TableReport> run(final boolean apply) {
    if (apply && target == null) {
      throw new IllegalArgumentException("copying needs the FoundationDB directory; only a dry run may go without it");
    }

    final List<DataSet> dataSets = List.of(
        new DataSet(FoundationDbStorage.GROUPS, sourceTables.getGroupsTableId(), subspace(FoundationDbStorage::getGroups),
            BigtableToFoundationDbMigrator::convertGroup,
            // (group id, "ver")
            key -> key.size() == 2 && FoundationDbGroupsTable.COLUMN_VERSION.equals(key.get(1))),
        new DataSet(FoundationDbStorage.GROUP_LOGS, sourceTables.getGroupLogsTableId(), subspace(FoundationDbStorage::getGroupLogs),
            BigtableToFoundationDbMigrator::convertGroupLogEntry,
            // (group id, version, "v")
            key -> key.size() == 3 && FoundationDbGroupLogTable.COLUMN_VERSION.equals(key.get(2))),
        new DataSet(FoundationDbStorage.STORAGE_MANIFESTS, sourceTables.getContactManifestsTableId(),
            subspace(FoundationDbStorage::getStorageManifests), BigtableToFoundationDbMigrator::convertManifest,
            // (uuid, "ver")
            key -> key.size() == 2 && FoundationDbStorageManifestsTable.COLUMN_VERSION.equals(key.get(1))),
        new DataSet(FoundationDbStorage.STORAGE_ITEMS, sourceTables.getContactsTableId(), subspace(FoundationDbStorage::getStorageItems),
            BigtableToFoundationDbMigrator::convertItem,
            // (uuid, item key, 0): the first chunk of an item
            key -> key.size() == 3 && Long.valueOf(0).equals(key.get(2))));

    out.printf("migrate-bigtable-to-foundationdb: %s%n", apply
        ? "APPLY: records missing in FoundationDB are copied; nothing is overwritten or deleted"
        : "DRY RUN: nothing is written (add --apply to copy)");
    out.printf("source: Bigtable project %s, instance %s; target: FoundationDB directory %s%s%n",
        sourceTables.getProjectId(), sourceTables.getInstanceId(), targetDirectory,
        target == null ? " (does not exist yet: nothing to compare, every readable row would be copied)" : "");

    final List<TableReport> reports = new ArrayList<>();
    for (final DataSet dataSet : dataSets) {
      reports.add(migrate(dataSet, apply));
    }

    printReport(reports, apply);
    return reports;
  }

  @Nullable
  private Subspace subspace(final Function<FoundationDbStorage, Subspace> subdirectory) {
    return target == null ? null : subdirectory.apply(target);
  }

  private TableReport migrate(final DataSet dataSet, final boolean apply) {
    final TableReport report = new TableReport(dataSet.name(), dataSet.bigtableTable());
    report.targetBefore = countRecords(dataSet.subspace(), dataSet.recordMarker());

    final List<TargetRecord> batch = new ArrayList<>();
    long batchBytes = 0;

    for (final Row row : source.readRows(Query.create(TableId.of(dataSet.bigtableTable())))) {
      report.sourceRows++;

      final Optional<TargetRecord> maybeRecord = dataSet.converter().convert(row);
      if (maybeRecord.isEmpty()) {
        report.unreadable++;
        continue;
      }

      final TargetRecord record = maybeRecord.get();
      if (!batch.isEmpty() && (batch.size() >= BATCH_RECORDS || batchBytes + record.bytes() > BATCH_BYTES)) {
        count(report, flush(dataSet.subspace(), batch, apply));
        batch.clear();
        batchBytes = 0;
      }

      batch.add(record);
      batchBytes += record.bytes();
    }

    if (!batch.isEmpty()) {
      count(report, flush(dataSet.subspace(), batch, apply));
    }

    if (apply) {
      report.targetAfter = countRecords(dataSet.subspace(), dataSet.recordMarker());
    }

    return report;
  }

  private static void count(final TableReport report, final List<Outcome> outcomes) {
    for (final Outcome outcome : outcomes) {
      switch (outcome) {
        case COPIED -> report.copied++;
        case IDENTICAL -> report.identical++;
        case CONFLICT -> report.conflicts++;
      }
    }
  }

  /// Compares a batch of records with FoundationDB in one transaction and, if `apply`, writes the missing ones in
  /// that same transaction.
  private List<Outcome> flush(@Nullable final Subspace subspace, final List<TargetRecord> records, final boolean apply) {
    final List<TargetRecord> snapshot = List.copyOf(records);

    if (subspace == null) {
      // a dry run against a directory that does not exist yet
      return snapshot.stream().map(ignored -> Outcome.COPIED).toList();
    }

    if (apply) {
      return target.write((transaction, mayHaveCommitted) -> readAll(transaction, subspace, snapshot)
          .thenApply(existing -> {
            final List<Outcome> outcomes = classify(snapshot, existing);
            for (int i = 0; i < snapshot.size(); i++) {
              if (outcomes.get(i) == Outcome.COPIED) {
                FoundationDbRecords.write(transaction, subspace, snapshot.get(i).prefix(), snapshot.get(i).columns());
              }
            }
            return outcomes;
          })).join();
    }

    return target.read(transaction -> readAll(transaction, subspace, snapshot).thenApply(existing ->
        classify(snapshot, existing))).join();
  }

  private static CompletableFuture<List<Columns>> readAll(final ReadTransaction transaction,
      final Subspace subspace, final List<TargetRecord> records) {

    final List<CompletableFuture<Columns>> reads = records.stream()
        .map(record -> FoundationDbRecords.read(transaction, subspace, record.prefix()))
        .toList();

    return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new))
        .thenApply(ignored -> reads.stream().map(CompletableFuture::join).toList());
  }

  private static List<Outcome> classify(final List<TargetRecord> records, final List<Columns> existing) {
    final List<Outcome> outcomes = new ArrayList<>(records.size());
    for (int i = 0; i < records.size(); i++) {
      final Columns stored = existing.get(i);
      if (stored.isEmpty()) {
        outcomes.add(Outcome.COPIED);
      } else if (stored.sameContentAs(records.get(i).columns())) {
        outcomes.add(Outcome.IDENTICAL);
      } else {
        outcomes.add(Outcome.CONFLICT);
      }
    }
    return outcomes;
  }

  /// Counts the records of a subdirectory: the keys that `recordMarker` accepts, one per record. Reads page by page,
  /// each page in its own read transaction.
  long countRecords(@Nullable final Subspace subspace, final Predicate<Tuple> recordMarker) {
    if (subspace == null) {
      return 0;
    }

    long count = 0;
    byte[] begin = subspace.range().begin;
    final byte[] end = subspace.range().end;

    while (true) {
      final byte[] pageBegin = begin;
      final List<KeyValue> page = target.read(transaction -> transaction.getRange(
              KeySelector.firstGreaterOrEqual(pageBegin), KeySelector.firstGreaterOrEqual(end),
              COUNT_PAGE_KEY_VALUES, false, StreamingMode.WANT_ALL).asList())
          .join();

      for (final KeyValue keyValue : page) {
        if (recordMarker.test(subspace.unpack(keyValue.getKey()))) {
          count++;
        }
      }

      if (page.size() < COUNT_PAGE_KEY_VALUES) {
        return count;
      }

      // continue just after the last key read
      final byte[] lastKey = page.getLast().getKey();
      begin = Arrays.copyOf(lastKey, lastKey.length + 1);
    }
  }

  private void printReport(final List<TableReport> reports, final boolean apply) {
    out.printf("%-18s %-28s %12s %14s %11s %10s %10s %11s %13s%n", "data set", "bigtable table", "source rows",
        "target before", apply ? "copied" : "would copy", "identical", "conflicts", "unreadable", "target after");

    for (final TableReport report : reports) {
      out.printf("%-18s %-28s %12d %14d %11d %10d %10d %11d %13s%n", report.dataSet(), report.bigtableTable(),
          report.sourceRows(), report.targetBefore(), report.copied(), report.identical(), report.conflicts(),
          report.unreadable(), report.targetAfter() < 0 ? "-" : String.valueOf(report.targetAfter()));
    }

    final long conflicts = reports.stream().mapToLong(TableReport::conflicts).sum();
    final long unreadable = reports.stream().mapToLong(TableReport::unreadable).sum();
    if (conflicts == 0 && unreadable == 0) {
      out.printf("OK: every Bigtable row is %s FoundationDB.%n",
          apply ? "now in" : "either already identical in, or would be copied to,");
    } else {
      out.printf("ATTENTION: %d conflict(s) (FoundationDB holds different bytes; left alone) and %d unreadable "
          + "row(s) (not copied). Nothing was overwritten and nothing was deleted.%n", conflicts, unreadable);
    }
  }

  // ------------------------------------------------------------------ row conversion

  /// groups: row key = group id; `g:gr` (the cell with timestamp 0, as upstream's GroupsTable reads it), `g:ver`.
  static Optional<TargetRecord> convertGroup(final Row row) {
    final Optional<ByteString> groupData = row.getCells(GROUPS_FAMILY, GroupsTable.COLUMN_GROUP_DATA).stream()
        .filter(cell -> cell.getTimestamp() == 0)
        .findFirst()
        .map(RowCell::getValue);
    final Optional<ByteString> version = firstCell(row, GROUPS_FAMILY, GroupsTable.COLUMN_VERSION);

    if (groupData.isEmpty() || version.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(record(FoundationDbGroupsTable.recordPrefix(row.getKey()),
        FoundationDbGroupsTable.columns(groupData.get().toByteArray(), version.get().toByteArray())));
  }

  /// group logs: row key = group id + `#` + version (4 bytes, big-endian); `l:c`, `l:s`, `l:v`.
  static Optional<TargetRecord> convertGroupLogEntry(final Row row) {
    final byte[] key = row.getKey().toByteArray();
    if (key.length < 5 || key[key.length - 5] != '#') {
      return Optional.empty();
    }

    final ByteString groupId = ByteString.copyFrom(key, 0, key.length - 5);
    final int version = Conversions.byteArrayToInt(key, key.length - 4);

    final Optional<ByteString> change = firstCell(row, GROUP_LOGS_FAMILY, GroupLogTable.COLUMN_CHANGE);
    final Optional<ByteString> state = firstCell(row, GROUP_LOGS_FAMILY, GroupLogTable.COLUMN_STATE);
    final Optional<ByteString> versionText = firstCell(row, GROUP_LOGS_FAMILY, GroupLogTable.COLUMN_VERSION);

    if (change.isEmpty() || state.isEmpty() || versionText.isEmpty()
        || !versionText.get().toStringUtf8().equals(String.valueOf(version))) {
      return Optional.empty();
    }

    return Optional.of(record(FoundationDbGroupLogTable.recordPrefix(groupId, version),
        FoundationDbGroupLogTable.columns(change.get().toByteArray(), state.get().toByteArray(),
            versionText.get().toByteArray())));
  }

  /// storage manifests: row key = `<uuid>#manifest`; `m:ver`, `m:dat`.
  static Optional<TargetRecord> convertManifest(final Row row) {
    final String key = row.getKey().toStringUtf8();
    if (!key.endsWith(MANIFEST_ROW_SUFFIX)) {
      return Optional.empty();
    }

    final Optional<UUID> uuid = parseUuid(key.substring(0, key.length() - MANIFEST_ROW_SUFFIX.length()));
    final Optional<ByteString> version = firstCell(row, MANIFESTS_FAMILY, FoundationDbStorageManifestsTable.COLUMN_VERSION);
    final Optional<ByteString> data = firstCell(row, MANIFESTS_FAMILY, FoundationDbStorageManifestsTable.COLUMN_DATA);

    if (uuid.isEmpty() || version.isEmpty() || data.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(record(FoundationDbStorageManifestsTable.recordPrefix(uuid.get()),
        FoundationDbStorageManifestsTable.columns(version.get().toByteArray(), data.get().toByteArray())));
  }

  /// storage items: row key = `<uuid>#contact#<hex of the item key>`; `c:d` (value), `c:k` (the item key again).
  static Optional<TargetRecord> convertItem(final Row row) {
    final String key = row.getKey().toStringUtf8();
    final int infix = key.indexOf(ITEM_ROW_INFIX);
    if (infix < 0) {
      return Optional.empty();
    }

    final Optional<UUID> uuid = parseUuid(key.substring(0, infix));
    final Optional<ByteString> value = firstCellByQualifier(row, StorageItemsTable.COLUMN_DATA);
    final Optional<ByteString> itemKey = firstCellByQualifier(row, StorageItemsTable.COLUMN_KEY);

    if (uuid.isEmpty() || value.isEmpty() || itemKey.isEmpty()) {
      return Optional.empty();
    }

    // The hex in the row key and the key cell must agree.
    final String hex = key.substring(infix + ITEM_ROW_INFIX.length());
    if (!hex.equals(HexFormat.of().formatHex(itemKey.get().toByteArray()))) {
      return Optional.empty();
    }

    return Optional.of(record(FoundationDbStorageItemsTable.recordPrefix(uuid.get(), itemKey.get()),
        FoundationDbStorageItemsTable.columns(value.get().toByteArray())));
  }

  private static TargetRecord record(final Tuple prefix, final Columns columns) {
    long bytes = 0;
    for (final byte[] value : columns.plain().values()) {
      bytes += value.length;
    }
    for (final byte[] value : columns.chunked().values()) {
      bytes += value.length;
    }
    return new TargetRecord(prefix, columns, bytes + 64);
  }

  /// The first cell of a column, as upstream's GroupLogTable and StorageManifestsTable read it (Bigtable returns a
  /// column's cells newest first).
  private static Optional<ByteString> firstCell(final Row row, final String family, final String qualifier) {
    return row.getCells(family, qualifier).stream().findFirst().map(RowCell::getValue);
  }

  /// As upstream's StorageItemsTable reads a cell: the first cell of the row with that qualifier.
  private static Optional<ByteString> firstCellByQualifier(final Row row, final String qualifier) {
    return row.getCells().stream()
        .filter(cell -> cell.getFamily().equals(ITEMS_FAMILY) && qualifier.equals(cell.getQualifier().toStringUtf8()))
        .findFirst()
        .map(RowCell::getValue);
  }

  private static Optional<UUID> parseUuid(final String text) {
    try {
      final UUID uuid = UUID.fromString(text);
      // UUID.fromString accepts some non-canonical forms; the row key was written with UUID.toString().
      return uuid.toString().equals(text) ? Optional.of(uuid) : Optional.empty();
    } catch (final IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
