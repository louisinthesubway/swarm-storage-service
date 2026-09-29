/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.signal.storageservice.storage.protos.groups.Group;

/// SWARM: the storage operations [GroupsManager] needs for group state, with exactly the signatures of upstream's
/// [GroupsTable] (Bigtable), so that a second backend can serve them. See docs/SWARM-CHANGES.md, section 3.
public interface GroupsStore {

  /// @return the stored group, or empty if there is none
  CompletableFuture<Optional<Group>> getGroup(ByteString groupId);

  /// Stores `group` if and only if no group is stored under `groupId` yet.
  ///
  /// @return `true` if the group was stored, `false` if a group already existed
  CompletableFuture<Boolean> createGroup(ByteString groupId, Group group);

  /// Replaces the stored group if and only if the stored version is exactly `group.getVersion() - 1`.
  ///
  /// @return `true` if the group was replaced, `false` otherwise
  CompletableFuture<Boolean> updateGroup(ByteString groupId, Group group);
}
