/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.signal.storageservice.storage.GroupLogStore;
import org.signal.storageservice.storage.GroupLogTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbRecords.Columns;
import org.signal.storageservice.storage.protos.groups.Group;
import org.signal.storageservice.storage.protos.groups.GroupChange;
import org.signal.storageservice.storage.protos.groups.GroupChanges.GroupChangeState;
import org.signal.storageservice.util.Pair;

/// SWARM: group change logs in FoundationDB, the counterpart of upstream's [GroupLogTable]. See
/// docs/SWARM-CHANGES.md, section 3.
///
/// Subdirectory [FoundationDbStorage#GROUP_LOGS]; one record per group version:
///
/// - `(group id, version, "c", chunk)`: the `GroupChange` protobuf bytes (Bigtable cell `l:c`)
/// - `(group id, version, "s", chunk)`: the `Group` bytes after the change (Bigtable cell `l:s`)
/// - `(group id, version, "v")`: the version as decimal text (Bigtable cell `l:v`)
///
/// Tuple integers sort numerically, so the versions `[from, to)` of one group are the key range
/// `[pack(group id, from), pack(group id, to))`.
public class FoundationDbGroupLogTable implements GroupLogStore {

  public static final String COLUMN_VERSION = GroupLogTable.COLUMN_VERSION;
  public static final String COLUMN_CHANGE = GroupLogTable.COLUMN_CHANGE;
  public static final String COLUMN_STATE = GroupLogTable.COLUMN_STATE;

  /// The most key-values one read transaction of [#getRecordsFromVersion] returns. More than any single log entry
  /// can have (one entry is written in one transaction of at most 10,000,000 bytes, so at most about 101 chunks
  /// per column), so every page completes at least one entry.
  @VisibleForTesting
  static final int PAGE_KEY_VALUES = 256;

  private final FoundationDbStorage storage;
  private final Subspace subspace;

  public FoundationDbGroupLogTable(final FoundationDbStorage storage) {
    this.storage = storage;
    this.subspace = storage.getGroupLogs();
  }

  /// The key prefix of one log entry.
  public static Tuple recordPrefix(final ByteString groupId, final long version) {
    return Tuple.from(groupId.toByteArray(), version);
  }

  /// The columns of one log entry, holding exactly the bytes Bigtable keeps in `l:c`, `l:s` and `l:v`.
  public static Columns columns(final byte[] groupChange, final byte[] groupState, final byte[] version) {
    return FoundationDbRecords.columns()
        .chunked(COLUMN_CHANGE, groupChange)
        .chunked(COLUMN_STATE, groupState)
        .plain(COLUMN_VERSION, version)
        .build();
  }

  /// Stores the entry if no non-empty change is stored for this version yet (upstream: `setIfEmpty` on `l:c`).
  @Override
  public CompletableFuture<Boolean> append(final ByteString groupId, final int version, final GroupChange groupChange,
      final Group group) {

    final Tuple prefix = recordPrefix(groupId, version);
    final Columns columns = columns(groupChange.toByteArray(), group.toByteArray(),
        FoundationDbGroupsTable.versionBytes(version));

    return storage.write((transaction, mayHaveCommitted) ->
        FoundationDbRecords.hasNonEmptyChunkedColumn(transaction, subspace, prefix, COLUMN_CHANGE)
            .thenCompose(exists -> {
              if (!exists) {
                FoundationDbRecords.write(transaction, subspace, prefix, columns);
                return CompletableFuture.completedFuture(true);
              }

              return mayHaveCommitted
                  ? FoundationDbRecords.read(transaction, subspace, prefix).thenApply(columns::sameContentAs)
                  : CompletableFuture.completedFuture(false);
            }));
  }

  @Override
  public CompletableFuture<Pair<List<GroupChangeState>, Boolean>> getRecordsFromVersion(final ByteString groupId,
      @Nullable final Integer maxSupportedChangeEpoch, final boolean includeFirstState, final boolean includeLastState,
      final int fromVersionInclusive, final int toVersionExclusive, final int currentVersion) {

    final List<GroupChangeState> results = new LinkedList<>();
    final boolean[] seenCurrentVersion = {false};

    // The same per-entry logic as upstream's GroupLogTable.getRecordsFromVersion. Entries arrive one page after the
    // other, never concurrently.
    final Consumer<Entry> consumer = entry -> {
      try {
        final GroupChange groupChange = GroupChange.parseFrom(entry.require(COLUMN_CHANGE));
        final Group groupState = Group.parseFrom(entry.require(COLUMN_STATE));

        if (groupState.getVersion() == currentVersion) {
          seenCurrentVersion[0] = true;
        }

        final GroupChangeState.Builder groupChangeStateBuilder = GroupChangeState.newBuilder().setGroupChange(groupChange);
        if (maxSupportedChangeEpoch == null || maxSupportedChangeEpoch < groupChange.getChangeEpoch()
            || (includeFirstState && groupState.getVersion() == fromVersionInclusive)
            || (includeLastState && groupState.getVersion() == toVersionExclusive - 1)) {
          groupChangeStateBuilder.setGroupState(groupState);
        }

        results.add(groupChangeStateBuilder.build());
      } catch (final InvalidProtocolBufferException e) {
        throw new CompletionException(e);
      }
    };

    final byte[] groupIdBytes = groupId.toByteArray();
    final byte[] end = subspace.pack(Tuple.from(groupIdBytes, (long) toVersionExclusive));

    return readEntries(groupIdBytes, fromVersionInclusive, end, consumer)
        .thenApply(ignored -> new Pair<>(results, seenCurrentVersion[0]));
  }

  /// Hands every complete entry with a version of at least `startVersion` and a key below `end` to `consumer`, in
  /// version order, reading at most [#PAGE_KEY_VALUES] key-values per read transaction. Log entries are written once
  /// and never changed, so reading them page by page returns what one transaction would.
  private CompletableFuture<Void> readEntries(final byte[] groupId, final long startVersion, final byte[] end,
      final Consumer<Entry> consumer) {

    final byte[] begin = subspace.pack(Tuple.from(groupId, startVersion));
    if (Arrays.compareUnsigned(begin, end) >= 0) {
      return CompletableFuture.completedFuture(null);
    }

    return storage.read(transaction ->
            transaction.getRange(begin, end, PAGE_KEY_VALUES, false, StreamingMode.WANT_ALL).asList())
        .thenCompose(keyValues -> {
          final List<Entry> entries = entries(keyValues);

          if (keyValues.size() < PAGE_KEY_VALUES) {
            entries.forEach(consumer);
            return CompletableFuture.completedFuture(null);
          }

          // A full page: its last entry may continue on the next page.
          final long lastVersion = entries.getLast().version();

          if (entries.size() > 1) {
            entries.subList(0, entries.size() - 1).forEach(consumer);
            return readEntries(groupId, lastVersion, end, consumer);
          }

          // One entry filled the whole page (cannot happen within FoundationDB's transaction size limit, but never
          // loop): read that entry on its own, without a limit.
          return storage.read(transaction -> readEntry(transaction, groupId, lastVersion))
              .thenCompose(entry -> {
                consumer.accept(entry);
                return readEntries(groupId, lastVersion + 1, end, consumer);
              });
        });
  }

  private CompletableFuture<Entry> readEntry(final ReadTransaction transaction, final byte[] groupId,
      final long version) {

    return transaction.getRange(subspace.range(Tuple.from(groupId, version)), ReadTransaction.ROW_LIMIT_UNLIMITED,
            false, StreamingMode.WANT_ALL)
        .asList()
        .thenApply(keyValues -> entries(keyValues).getFirst());
  }

  /// Splits key-values of one group, in key order, into entries by version.
  private List<Entry> entries(final List<KeyValue> keyValues) {
    final List<Entry> entries = new ArrayList<>();
    int first = 0;

    while (first < keyValues.size()) {
      final long version = subspace.unpack(keyValues.get(first).getKey()).getLong(1);
      int last = first + 1;
      while (last < keyValues.size() && subspace.unpack(keyValues.get(last).getKey()).getLong(1) == version) {
        last++;
      }

      entries.add(new Entry(version, FoundationDbRecords.parse(subspace, 2, keyValues.subList(first, last))));
      first = last;
    }

    return entries;
  }

  private record Entry(long version, Columns columns) {

    /// Upstream reads each cell with `findFirst().orElseThrow()`; so does this.
    byte[] require(final String column) {
      final byte[] value = columns.chunked(column);
      if (value == null) {
        throw new NoSuchElementException("log entry " + version + " has no column " + column);
      }
      return value;
    }
  }
}
