/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.signal.storageservice.auth.User;
import org.signal.storageservice.storage.protos.contacts.StorageManifest;

/// SWARM: the storage operations [StorageManager] needs for storage manifests, with exactly the signatures of
/// upstream's [StorageManifestsTable] (Bigtable), so that a second backend can serve them. See
/// docs/SWARM-CHANGES.md, section 3.
public interface StorageManifestsStore {

  /// Stores `manifest` if and only if no manifest is stored for `user` or the stored version is exactly
  /// `manifest.getVersion() - 1`.
  ///
  /// @return `true` if the manifest was stored, `false` otherwise
  CompletableFuture<Boolean> set(User user, StorageManifest manifest);

  /// @return the stored manifest, or empty if there is none
  CompletableFuture<Optional<StorageManifest>> get(User user);

  /// @return the stored manifest, or empty if there is none or if its version is `version`
  CompletableFuture<Optional<StorageManifest>> getIfNotVersion(User user, long version);

  /// Deletes the stored manifest, if any.
  CompletableFuture<Void> clear(User user);
}
