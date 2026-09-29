/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage.foundationdb;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.signal.storageservice.storage.GroupsStore;
import org.signal.storageservice.storage.GroupsTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbRecords.Columns;
import org.signal.storageservice.storage.protos.groups.Group;

/// SWARM: group state in FoundationDB, the counterpart of upstream's [GroupsTable]. See docs/SWARM-CHANGES.md,
/// section 3.
///
/// Subdirectory [FoundationDbStorage#GROUPS]; one record per group:
///
/// - `(group id, "gr", chunk)`: the `Group` protobuf bytes (Bigtable cell `g:gr`)
/// - `(group id, "ver")`: the group's version as decimal text (Bigtable cell `g:ver`)
public class FoundationDbGroupsTable implements GroupsStore {

  public static final String COLUMN_GROUP_DATA = GroupsTable.COLUMN_GROUP_DATA;
  public static final String COLUMN_VERSION = GroupsTable.COLUMN_VERSION;

  private final FoundationDbStorage storage;
  private final Subspace subspace;

  public FoundationDbGroupsTable(final FoundationDbStorage storage) {
    this.storage = storage;
    this.subspace = storage.getGroups();
  }

  /// The key prefix of a group's record.
  public static Tuple recordPrefix(final ByteString groupId) {
    return Tuple.from((Object) groupId.toByteArray());
  }

  /// The columns of a group's record, holding exactly the bytes Bigtable keeps in `g:gr` and `g:ver`.
  public static Columns columns(final byte[] groupData, final byte[] version) {
    return FoundationDbRecords.columns()
        .chunked(COLUMN_GROUP_DATA, groupData)
        .plain(COLUMN_VERSION, version)
        .build();
  }

  static byte[] versionBytes(final long version) {
    return String.valueOf(version).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public CompletableFuture<Optional<Group>> getGroup(final ByteString groupId) {
    return storage.read(transaction -> FoundationDbRecords.read(transaction, subspace, recordPrefix(groupId)))
        .thenApply(columns -> {
          final byte[] groupData = columns.chunked(COLUMN_GROUP_DATA);

          if (groupData == null) {
            return Optional.empty();
          }

          try {
            return Optional.of(Group.parseFrom(groupData));
          } catch (final InvalidProtocolBufferException e) {
            // as upstream's GroupsTable
            throw new AssertionError(e);
          }
        });
  }

  /// Stores the group if no non-empty group data is stored under the id yet (upstream: `setIfEmpty` on `g:gr`).
  @Override
  public CompletableFuture<Boolean> createGroup(final ByteString groupId, final Group group) {
    final Tuple prefix = recordPrefix(groupId);
    final Columns columns = columns(group.toByteArray(), versionBytes(group.getVersion()));

    return storage.write((transaction, mayHaveCommitted) ->
        FoundationDbRecords.hasNonEmptyChunkedColumn(transaction, subspace, prefix, COLUMN_GROUP_DATA)
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

  /// Replaces the group if the stored version is exactly `group.getVersion() - 1` (upstream: `setIfValue` on
  /// `g:ver`).
  @Override
  public CompletableFuture<Boolean> updateGroup(final ByteString groupId, final Group group) {
    final Tuple prefix = recordPrefix(groupId);
    final byte[] expectedVersion = versionBytes(group.getVersion() - 1);
    final Columns columns = columns(group.toByteArray(), versionBytes(group.getVersion()));

    return storage.write((transaction, mayHaveCommitted) ->
        transaction.get(subspace.pack(prefix.add(COLUMN_VERSION))).thenCompose(storedVersion -> {
          if (Arrays.equals(storedVersion, expectedVersion)) {
            FoundationDbRecords.write(transaction, subspace, prefix, columns);
            return CompletableFuture.completedFuture(true);
          }

          return mayHaveCommitted
              ? FoundationDbRecords.read(transaction, subspace, prefix).thenApply(columns::sameContentAs)
              : CompletableFuture.completedFuture(false);
        }));
  }
}
