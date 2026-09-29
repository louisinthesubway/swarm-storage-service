/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage.foundationdb;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.signal.storageservice.auth.User;
import org.signal.storageservice.storage.StorageManifestsStore;
import org.signal.storageservice.storage.foundationdb.FoundationDbRecords.Columns;
import org.signal.storageservice.storage.protos.contacts.StorageManifest;

/// SWARM: storage manifests in FoundationDB, the counterpart of upstream's `StorageManifestsTable`. See
/// docs/SWARM-CHANGES.md, section 3.
///
/// Subdirectory [FoundationDbStorage#STORAGE_MANIFESTS]; one record per user:
///
/// - `(uuid, "ver")`: the manifest version as decimal text (Bigtable cell `m:ver`)
/// - `(uuid, "dat", chunk)`: the manifest bytes (Bigtable cell `m:dat`)
public class FoundationDbStorageManifestsTable implements StorageManifestsStore {

  // The same names as upstream's StorageManifestsTable (package-private there).
  public static final String COLUMN_VERSION = "ver";
  public static final String COLUMN_DATA = "dat";

  private final FoundationDbStorage storage;
  private final Subspace subspace;

  public FoundationDbStorageManifestsTable(final FoundationDbStorage storage) {
    this.storage = storage;
    this.subspace = storage.getStorageManifests();
  }

  /// The key prefix of a user's manifest.
  public static Tuple recordPrefix(final UUID uuid) {
    return Tuple.from(uuid);
  }

  /// The columns of a manifest record, holding exactly the bytes Bigtable keeps in `m:ver` and `m:dat`.
  public static Columns columns(final byte[] version, final byte[] data) {
    return FoundationDbRecords.columns()
        .plain(COLUMN_VERSION, version)
        .chunked(COLUMN_DATA, data)
        .build();
  }

  private static byte[] versionBytes(final long version) {
    return String.valueOf(version).getBytes(StandardCharsets.UTF_8);
  }

  /// Stores the manifest if no version is stored for the user or the stored version is exactly
  /// `manifest.getVersion() - 1` (upstream: `setIfValueOrEmpty` on `m:ver`).
  @Override
  public CompletableFuture<Boolean> set(final User user, final StorageManifest manifest) {
    final Tuple prefix = recordPrefix(user.getUuid());
    final byte[] expectedVersion = versionBytes(manifest.getVersion() - 1);
    final Columns columns = columns(versionBytes(manifest.getVersion()), manifest.getValue().toByteArray());

    return storage.write((transaction, mayHaveCommitted) ->
        transaction.get(subspace.pack(prefix.add(COLUMN_VERSION))).thenCompose(storedVersion -> {
          if (storedVersion == null || storedVersion.length == 0 || Arrays.equals(storedVersion, expectedVersion)) {
            FoundationDbRecords.write(transaction, subspace, prefix, columns);
            return CompletableFuture.completedFuture(true);
          }

          return mayHaveCommitted
              ? FoundationDbRecords.read(transaction, subspace, prefix).thenApply(columns::sameContentAs)
              : CompletableFuture.completedFuture(false);
        }));
  }

  @Override
  public CompletableFuture<Optional<StorageManifest>> get(final User user) {
    return storage.read(transaction -> FoundationDbRecords.read(transaction, subspace, recordPrefix(user.getUuid())))
        .thenApply(FoundationDbStorageManifestsTable::toManifest);
  }

  @Override
  public CompletableFuture<Optional<StorageManifest>> getIfNotVersion(final User user, final long version) {
    final byte[] unwantedVersion = versionBytes(version);

    return storage.read(transaction -> FoundationDbRecords.read(transaction, subspace, recordPrefix(user.getUuid())))
        .thenApply(columns -> Arrays.equals(columns.plain(COLUMN_VERSION), unwantedVersion)
            ? Optional.empty()
            : toManifest(columns));
  }

  @Override
  public CompletableFuture<Void> clear(final User user) {
    return storage.write((transaction, mayHaveCommitted) -> {
      FoundationDbRecords.clear(transaction, subspace, recordPrefix(user.getUuid()));
      return CompletableFuture.completedFuture(null);
    });
  }

  private static Optional<StorageManifest> toManifest(final Columns columns) {
    if (columns.isEmpty()) {
      return Optional.empty();
    }

    // Upstream reads both cells with findFirst().orElseThrow().
    final byte[] version = columns.plain(COLUMN_VERSION);
    final byte[] data = columns.chunked(COLUMN_DATA);
    if (version == null || data == null) {
      throw new NoSuchElementException("incomplete manifest record");
    }

    return Optional.of(StorageManifest.newBuilder()
        .setVersion(Long.parseLong(new String(version, StandardCharsets.UTF_8)))
        .setValue(ByteString.copyFrom(data))
        .build());
  }
}
