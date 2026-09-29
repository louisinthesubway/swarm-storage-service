/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.signal.storageservice.auth.User;
import org.signal.storageservice.storage.protos.contacts.StorageItem;

/// SWARM: the storage operations [StorageManager] needs for storage items, with exactly the signatures of upstream's
/// [StorageItemsTable] (Bigtable), so that a second backend can serve them. See docs/SWARM-CHANGES.md, section 3.
public interface StorageItemsStore {

  /// Writes `inserts` (replacing items with the same key), then deletes the items with the keys in `deletes`. Not
  /// atomic as a whole.
  CompletableFuture<Void> set(User user, List<StorageItem> inserts, List<ByteString> deletes);

  /// Deletes every item stored for `user`.
  CompletableFuture<Void> clear(User user);

  /// @return the stored items among `keys`, in key order; absent keys are left out
  /// @throws IllegalArgumentException if `keys` is empty
  CompletableFuture<List<StorageItem>> get(User user, List<ByteString> keys);
}
