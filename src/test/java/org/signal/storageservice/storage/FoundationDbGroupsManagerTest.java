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
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.storageservice.storage.foundationdb.FoundationDbExtension;
import org.signal.storageservice.storage.foundationdb.FoundationDbGroupLogTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbGroupsTable;
import org.signal.storageservice.storage.foundationdb.FoundationDbRecords;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorage;

/// SWARM: upstream's GroupsManagerTest on the FoundationDB backend. The raw reads check the documented key layout
/// (docs/SWARM-CHANGES.md, section 3.4) with explicit keys, independently of [FoundationDbRecords]. Skipped without a
/// FoundationDB cluster (see [FoundationDbExtension]).
class FoundationDbGroupsManagerTest extends GroupsManagerTest {

  @RegisterExtension
  static final FoundationDbExtension FOUNDATION_DB = new FoundationDbExtension();

  @Override
  protected GroupsManager groupsManager() {
    final FoundationDbStorage storage = FOUNDATION_DB.getStorage();
    return new GroupsManager(new FoundationDbGroupsTable(storage), new FoundationDbGroupLogTable(storage));
  }

  @Override
  protected GroupsManager groupsManagerWithFailingReads(final RuntimeException failure) {
    final Database database = mock(Database.class);
    doReturn(CompletableFuture.failedFuture(failure)).when(database).readAsync(any());

    final FoundationDbStorage storage = new FoundationDbStorage(database, List.of("unused"),
        new Subspace(Tuple.from("g")), new Subspace(Tuple.from("l")), new Subspace(Tuple.from("m")),
        new Subspace(Tuple.from("i")), false);

    return new GroupsManager(new FoundationDbGroupsTable(storage), new FoundationDbGroupLogTable(storage));
  }

  @Override
  protected Optional<StoredGroup> storedGroup(final ByteString groupId) {
    final Subspace groups = FOUNDATION_DB.getStorage().getGroups();
    final byte[] id = groupId.toByteArray();

    return FOUNDATION_DB.getDatabase().read(transaction -> {
      final byte[] version = transaction.get(groups.pack(Tuple.from(id, "ver"))).join();
      final List<KeyValue> chunks = transaction.getRange(groups.range(Tuple.from(id, "gr"))).asList().join();

      if (version == null && chunks.isEmpty()) {
        return Optional.empty();
      }

      return Optional.of(new StoredGroup(new String(version, StandardCharsets.UTF_8), concatenate(groups, chunks)));
    });
  }

  @Override
  protected Optional<StoredLogEntry> storedLogEntry(final ByteString groupId, final int version) {
    final Subspace logs = FOUNDATION_DB.getStorage().getGroupLogs();
    final byte[] id = groupId.toByteArray();

    return FOUNDATION_DB.getDatabase().read(transaction -> {
      final byte[] versionText = transaction.get(logs.pack(Tuple.from(id, version, "v"))).join();
      final List<KeyValue> change = transaction.getRange(logs.range(Tuple.from(id, version, "c"))).asList().join();
      final List<KeyValue> state = transaction.getRange(logs.range(Tuple.from(id, version, "s"))).asList().join();

      if (versionText == null && change.isEmpty() && state.isEmpty()) {
        return Optional.empty();
      }

      return Optional.of(new StoredLogEntry(new String(versionText, StandardCharsets.UTF_8),
          concatenate(logs, change), concatenate(logs, state)));
    });
  }

  /// Joins chunks 0, 1, 2, ... (the last tuple element of each key), checking that they are consecutive.
  static ByteString concatenate(final Subspace subspace, final List<KeyValue> chunks) {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    long expected = 0;
    for (final KeyValue chunk : chunks) {
      final Tuple key = subspace.unpack(chunk.getKey());
      if (key.getLong(key.size() - 1) != expected++) {
        throw new AssertionError("chunks are not consecutive");
      }
      if (chunk.getValue().length > FoundationDbRecords.MAX_VALUE_BYTES) {
        throw new AssertionError("chunk larger than FoundationDB allows");
      }
      bytes.writeBytes(chunk.getValue());
    }
    return ByteString.copyFrom(bytes.toByteArray());
  }
}
