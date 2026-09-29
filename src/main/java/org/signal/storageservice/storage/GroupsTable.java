/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.signal.storageservice.storage.protos.groups.Group;

public class GroupsTable extends Table implements GroupsStore {

  public static final String FAMILY = "g";

  public static final String COLUMN_GROUP_DATA = "gr";
  public static final String COLUMN_VERSION    = "ver";

  public GroupsTable(BigtableDataClient client, String tableId) {
    super(client, tableId);
  }

  public CompletableFuture<Optional<Group>> getGroup(ByteString groupId) {
    return toFuture(client.readRowAsync(tableId, groupId)).thenApply(row -> {
      if (row == null) return Optional.empty();

      try {
        ByteString groupData = row.getCells(FAMILY, COLUMN_GROUP_DATA)
                                  .stream()
                                  .filter(cell -> cell.getTimestamp() == 0)
                                  .findFirst()
                                  .orElseThrow()
                                  .getValue();

        return Optional.of(Group.parseFrom(groupData));
      } catch (InvalidProtocolBufferException e) {
        throw new AssertionError(e);
      }
    });
  }

  public CompletableFuture<Boolean> createGroup(ByteString groupId, Group group) {
    Mutation mutation = Mutation.create()
                                .setCell(FAMILY, ByteString.copyFromUtf8(COLUMN_GROUP_DATA), 0, group.toByteString())
                                .setCell(FAMILY, COLUMN_VERSION, 0, String.valueOf(group.getVersion()));

    return setIfEmpty(groupId, FAMILY, COLUMN_GROUP_DATA, mutation);
  }

  public CompletableFuture<Boolean> updateGroup(ByteString groupId, Group group) {
    Mutation mutation = Mutation.create()
                                .setCell(FAMILY, ByteString.copyFromUtf8(COLUMN_GROUP_DATA), 0, group.toByteString())
                                .setCell(FAMILY, COLUMN_VERSION, 0, String.valueOf(group.getVersion()));

    return setIfValue(groupId, FAMILY, COLUMN_VERSION, String.valueOf(group.getVersion() - 1), mutation);
  }

}
