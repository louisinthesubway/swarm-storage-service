/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.signal.storageservice.storage.protos.groups.Group;
import org.signal.storageservice.storage.protos.groups.GroupChange;
import org.signal.storageservice.storage.protos.groups.GroupChanges.GroupChangeState;
import org.signal.storageservice.util.Pair;

/// SWARM: the storage operations [GroupsManager] needs for group change logs, with exactly the signatures of
/// upstream's [GroupLogTable] (Bigtable), so that a second backend can serve them. See docs/SWARM-CHANGES.md,
/// section 3.
public interface GroupLogStore {

  /// Stores the change that produced `version` and the group state after it, if and only if no change is stored
  /// for that version yet.
  ///
  /// @return `true` if the entry was stored, `false` if one already existed
  CompletableFuture<Boolean> append(ByteString groupId, int version, GroupChange groupChange, Group group);

  /// Reads the log entries with versions in `[fromVersionInclusive, toVersionExclusive)`, in version order. A
  /// returned entry carries its group state if `maxSupportedChangeEpoch` is `null` or lower than the change's epoch,
  /// or if it is the first (`includeFirstState`) or last (`includeLastState`) version of the range.
  ///
  /// @return the entries, and whether any entry's group state has the version `currentVersion`
  CompletableFuture<Pair<List<GroupChangeState>, Boolean>> getRecordsFromVersion(ByteString groupId,
      @Nullable Integer maxSupportedChangeEpoch, boolean includeFirstState, boolean includeLastState,
      int fromVersionInclusive, int toVersionExclusive, int currentVersion);
}
