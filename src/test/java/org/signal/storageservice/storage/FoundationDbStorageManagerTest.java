/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.storageservice.storage.foundationdb.FoundationDbExtension;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorage;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorageItemsTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorageManifestsTable;

/// SWARM: upstream's StorageManagerTest on the FoundationDB backend. Raw writes and reads use the documented key
/// layout (docs/SWARM-CHANGES.md, section 3.4) with explicit keys. Skipped without a FoundationDB cluster (see
/// [FoundationDbExtension]).
class FoundationDbStorageManagerTest extends StorageManagerTest {

  @RegisterExtension
  static final FoundationDbExtension FOUNDATION_DB = new FoundationDbExtension();

  /// Items per transaction when seeding: far below FoundationDB's transaction size limit.
  private static final int SEED_BATCH = 5_000;

  @Override
  protected StorageManager storageManager() {
    final FoundationDbStorage storage = FOUNDATION_DB.getStorage();
    return new StorageManager(new FoundationDbStorageManifestsTable(storage), new FoundationDbStorageItemsTable(storage));
  }

  @Override
  protected StorageManager storageManagerWithFailingReads(final RuntimeException failure) {
    final Database database = mock(Database.class);
    doReturn(CompletableFuture.failedFuture(failure)).when(database).readAsync(any());

    final FoundationDbStorage storage = new FoundationDbStorage(database, List.of("unused"),
        new Subspace(Tuple.from("g")), new Subspace(Tuple.from("l")), new Subspace(Tuple.from("m")),
        new Subspace(Tuple.from("i")), false);

    return new StorageManager(new FoundationDbStorageManifestsTable(storage), new FoundationDbStorageItemsTable(storage));
  }

  @Override
  protected void writeRawManifest(final UUID userId, final String version, final String data) {
    final Subspace manifests = FOUNDATION_DB.getStorage().getStorageManifests();

    FOUNDATION_DB.getDatabase().run(transaction -> {
      transaction.set(manifests.pack(Tuple.from(userId, "ver")), version.getBytes(StandardCharsets.UTF_8));
      transaction.set(manifests.pack(Tuple.from(userId, "dat", 0)), data.getBytes(StandardCharsets.UTF_8));
      return null;
    });
  }

  @Override
  protected void writeRawItems(final UUID userId, final List<Map.Entry<String, String>> keysAndData) {
    final Subspace items = FOUNDATION_DB.getStorage().getStorageItems();

    for (int first = 0; first < keysAndData.size(); first += SEED_BATCH) {
      final List<Map.Entry<String, String>> batch =
          keysAndData.subList(first, Math.min(keysAndData.size(), first + SEED_BATCH));

      FOUNDATION_DB.getDatabase().run(transaction -> {
        for (final Map.Entry<String, String> item : batch) {
          transaction.set(items.pack(Tuple.from(userId, item.getKey().getBytes(StandardCharsets.UTF_8), 0)),
              item.getValue().getBytes(StandardCharsets.UTF_8));
        }
        return null;
      });
    }
  }

  @Override
  protected List<String> rawItemData(final UUID userId) {
    final Subspace items = FOUNDATION_DB.getStorage().getStorageItems();

    final List<KeyValue> keyValues = FOUNDATION_DB.getDatabase().read(transaction ->
        transaction.getRange(items.range(Tuple.from(userId))).asList().join());

    final List<String> data = new ArrayList<>();
    for (final KeyValue keyValue : keyValues) {
      // every seeded and every test item fits one chunk
      if (items.unpack(keyValue.getKey()).getLong(2) != 0) {
        throw new AssertionError("unexpected second chunk");
      }
      data.add(new String(keyValue.getValue(), StandardCharsets.UTF_8));
    }
    return data;
  }

  @Override
  protected long countRawItems(final UUID userId) {
    final Subspace items = FOUNDATION_DB.getStorage().getStorageItems();
    long count = 0;
    byte[] begin = items.range(Tuple.from(userId)).begin;
    final byte[] end = items.range(Tuple.from(userId)).end;

    // page through: one transaction must stay under FoundationDB's 5-second limit
    while (true) {
      final byte[] pageBegin = begin;
      final List<KeyValue> page = FOUNDATION_DB.getDatabase().read(transaction ->
          transaction.getRange(pageBegin, end, 10_000).asList().join());

      for (final KeyValue keyValue : page) {
        if (items.unpack(keyValue.getKey()).getLong(2) == 0) {
          count++;
        }
      }

      if (page.size() < 10_000) {
        return count;
      }

      final byte[] last = page.getLast().getKey();
      begin = java.util.Arrays.copyOf(last, last.length + 1);
    }
  }
}
