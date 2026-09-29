/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage.foundationdb;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.signal.storageservice.auth.User;
import org.signal.storageservice.storage.StorageItemsStore;
import org.signal.storageservice.storage.foundationdb.FoundationDbRecords.Columns;
import org.signal.storageservice.storage.protos.contacts.StorageItem;

/// SWARM: storage items (settings/contacts sync records) in FoundationDB, the counterpart of upstream's
/// `StorageItemsTable`. See docs/SWARM-CHANGES.md, section 3.
///
/// Subdirectory [FoundationDbStorage#STORAGE_ITEMS]; one record per item: `(uuid, item key, chunk)` holds the item's
/// value (Bigtable cell `c:d`). Bigtable also keeps the key in a cell (`c:k`); here it is part of the tuple.
public class FoundationDbStorageItemsTable implements StorageItemsStore {

  /// Writes are cut into transactions of about this many bytes (FoundationDB allows 10,000,000 per transaction and
  /// recommends staying under 1,000,000)...
  @VisibleForTesting
  static final int MAX_BATCH_BYTES = 1_000_000;

  /// ... and at most this many inserts and deletes.
  @VisibleForTesting
  static final int MAX_BATCH_OPERATIONS = 10_000;

  // A generous allowance for the directory prefix, the tuple encoding of the uuid, the chunk index and the
  // bookkeeping FoundationDB counts per key (conflict ranges): an estimate, not an exact size.
  private static final int KEY_OVERHEAD_BYTES = 64;

  private final FoundationDbStorage storage;
  private final Subspace subspace;

  public FoundationDbStorageItemsTable(final FoundationDbStorage storage) {
    this.storage = storage;
    this.subspace = storage.getStorageItems();
  }

  /// The key prefix of one item.
  public static Tuple recordPrefix(final UUID uuid, final ByteString key) {
    return Tuple.from(uuid, key.toByteArray());
  }

  /// The columns of an item record: its value, chunked, under an empty column name.
  public static Columns columns(final byte[] value) {
    return FoundationDbRecords.columns().chunked(Tuple.from(), value).build();
  }

  /// Writes the inserts, then the deletes, in transactions of at most [#MAX_BATCH_BYTES] (estimated) and
  /// [#MAX_BATCH_OPERATIONS], one after the other. Like upstream's bulk mutations this is not atomic as a whole; every
  /// operation is idempotent, so a retried transaction does no harm.
  @Override
  public CompletableFuture<Void> set(final User user, final List<StorageItem> inserts, final List<ByteString> deletes) {
    final UUID uuid = user.getUuid();
    final List<List<Consumer<Transaction>>> batches = new ArrayList<>();
    final BatchBuilder batch = new BatchBuilder(batches);

    for (final StorageItem insert : inserts) {
      final int chunks = Math.max(1, (insert.getValue().size() + FoundationDbRecords.MAX_VALUE_BYTES - 1)
          / FoundationDbRecords.MAX_VALUE_BYTES);

      // The key and value bytes are copied only when the transaction runs.
      batch.add(3 * (insert.getKey().size() + KEY_OVERHEAD_BYTES) + insert.getValue().size() + chunks * KEY_OVERHEAD_BYTES,
          transaction -> FoundationDbRecords.write(transaction, subspace, recordPrefix(uuid, insert.getKey()),
              columns(insert.getValue().toByteArray())));
    }

    for (final ByteString delete : deletes) {
      batch.add(2 * (delete.size() + KEY_OVERHEAD_BYTES),
          transaction -> FoundationDbRecords.clear(transaction, subspace, recordPrefix(uuid, delete)));
    }

    batch.finish();

    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
    for (final List<Consumer<Transaction>> operations : batches) {
      future = future.thenCompose(ignored -> storage.write((transaction, mayHaveCommitted) -> {
        operations.forEach(operation -> operation.accept(transaction));
        return CompletableFuture.completedFuture(null);
      }));
    }

    return future;
  }

  /// Deletes every item of the user in one clear-range (upstream reads and deletes up to 100,000 rows at a time).
  @Override
  public CompletableFuture<Void> clear(final User user) {
    return storage.write((transaction, mayHaveCommitted) -> {
      FoundationDbRecords.clear(transaction, subspace, Tuple.from(user.getUuid()));
      return CompletableFuture.completedFuture(null);
    });
  }

  /// Reads the items in one read transaction. Like Bigtable's multi-row read, each key is returned at most once and
  /// the items come back in key order (unsigned byte order, the order of upstream's hex row keys).
  @Override
  public CompletableFuture<List<StorageItem>> get(final User user, final List<ByteString> keys) {
    if (keys.isEmpty()) {
      throw new IllegalArgumentException("No keys");
    }

    final UUID uuid = user.getUuid();
    final List<ByteString> sortedKeys = keys.stream()
        .distinct()
        .sorted(ByteString.unsignedLexicographicalComparator())
        .toList();

    return storage.read(transaction -> {
      final List<CompletableFuture<Optional<StorageItem>>> reads = sortedKeys.stream()
          .map(key -> FoundationDbRecords.read(transaction, subspace, recordPrefix(uuid, key))
              .thenApply(columns -> Optional.ofNullable(columns.chunked().get(Tuple.from()))
                  .map(value -> StorageItem.newBuilder()
                      .setKey(key)
                      .setValue(ByteString.copyFrom(value))
                      .build())))
          .toList();

      return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new))
          .thenApply(ignored -> {
            final List<StorageItem> items = new LinkedList<>();
            reads.forEach(read -> read.join().ifPresent(items::add));
            return items;
          });
    });
  }

  /// Groups operations into batches by estimated size and count.
  private static final class BatchBuilder {

    private final List<List<Consumer<Transaction>>> batches;
    private List<Consumer<Transaction>> current = new ArrayList<>();
    private long currentBytes = 0;

    BatchBuilder(final List<List<Consumer<Transaction>>> batches) {
      this.batches = batches;
    }

    void add(final long estimatedBytes, final Consumer<Transaction> operation) {
      if (!current.isEmpty()
          && (currentBytes + estimatedBytes > MAX_BATCH_BYTES || current.size() >= MAX_BATCH_OPERATIONS)) {
        finish();
      }

      current.add(operation);
      currentBytes += estimatedBytes;
    }

    void finish() {
      if (!current.isEmpty()) {
        batches.add(current);
        current = new ArrayList<>();
        currentBytes = 0;
      }
    }
  }
}
