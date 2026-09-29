/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.signal.storageservice.auth.User;
import org.signal.storageservice.storage.protos.contacts.StorageItem;
import org.signal.storageservice.storage.protos.contacts.StorageManifest;

/// SWARM: upstream's StorageManagerTest, run against every storage backend. The test methods are upstream's; where
/// upstream wrote or read raw Bigtable rows, the tests go through the backend hooks below. Subclasses:
/// [BigtableStorageManagerTest] (upstream's emulator setup) and [FoundationDbStorageManagerTest]. See
/// docs/SWARM-CHANGES.md, section 3.10.
abstract class StorageManagerTest {

  /// @return a manager on the backend under test
  protected abstract StorageManager storageManager();

  /// @return a manager whose manifest reads fail with `failure`
  protected abstract StorageManager storageManagerWithFailingReads(RuntimeException failure);

  /// Writes a manifest directly into the backend (upstream: a raw `m:ver` + `m:dat` row).
  protected abstract void writeRawManifest(UUID userId, String version, String data) throws Exception;

  /// Writes items directly into the backend (upstream: raw `c:d` + `c:k` rows under `<uuid>#contact#<key>`).
  protected abstract void writeRawItems(UUID userId, List<Map.Entry<String, String>> keysAndData) throws Exception;

  /// @return the data of every item stored for the user, in key order, read directly from the backend
  protected abstract List<String> rawItemData(UUID userId) throws Exception;

  /// @return the number of items stored for the user, read directly from the backend
  protected abstract long countRawItems(UUID userId) throws Exception;

  @Test
  void testReadManifest() throws Exception {
    UUID userId = UUID.randomUUID();
    User user = new User(userId);
    StorageManager contactsManager = storageManager();

    writeRawManifest(userId, "1", "A manifest");

    Optional<StorageManifest> manifest = contactsManager.getManifest(user).get();
    assertTrue(manifest.isPresent());
    assertThat(manifest.get().getVersion()).isEqualTo(1);
    assertThat(manifest.get().getValue().toStringUtf8()).isEqualTo("A manifest");
  }

  @Test
  void testGetManifestIfNotVersionDifferent() throws Exception {
    UUID userId = UUID.randomUUID();
    User user = new User(userId);
    StorageManager contactsManager = storageManager();

    writeRawManifest(UUID.randomUUID(), "4", "A manifest other");
    writeRawManifest(userId, "3", "A manifest");

    Optional<StorageManifest> manifest = contactsManager.getManifestIfNotVersion(user, 2).get();
    assertTrue(manifest.isPresent());
    assertThat(manifest.get().getVersion()).isEqualTo(3);
    assertThat(manifest.get().getValue().toStringUtf8()).isEqualTo("A manifest");
  }

  @Test
  void testGetManifestIfNotVersionSame() throws Exception {
    UUID userId = UUID.randomUUID();
    User user = new User(userId);
    StorageManager contactsManager = storageManager();

    writeRawManifest(UUID.randomUUID(), "4", "A manifest other");
    writeRawManifest(userId, "3", "A manifest");

    Optional<StorageManifest> manifest = contactsManager.getManifestIfNotVersion(user, 3).get();
    assertTrue(manifest.isEmpty());
  }

  @Test
  void testReadError() {
    UUID userId = UUID.randomUUID();
    User user = new User(userId);
    StorageManager contactsManager = storageManagerWithFailingReads(new RuntimeException("Bad news"));

    assertThatThrownBy(() -> contactsManager.getManifest(user).get())
        .isInstanceOf(ExecutionException.class)
        .hasRootCauseMessage("Bad news");
  }

  @Test
  void testSetEmptyManifest() throws Exception {
    UUID userId = UUID.randomUUID();
    User user = new User(userId);
    StorageManager contactsManager = storageManager();

    StorageManifest manifest = StorageManifest.newBuilder()
        .setVersion(1)
        .setValue(ByteString.copyFromUtf8("A manifest"))
        .build();

    StorageItem contact = StorageItem.newBuilder()
        .setKey(ByteString.copyFromUtf8("mykey"))
        .setValue(ByteString.copyFromUtf8("myvalue"))
        .build();

    Optional<StorageManifest> result = contactsManager.set(user, manifest, List.of(contact), new LinkedList<>()).get();

    assertTrue(result.isEmpty());

    Optional<StorageManifest> retrieved = contactsManager.getManifest(user).get();

    assertTrue(retrieved.isPresent());
    assertThat(retrieved.get().getVersion()).isEqualTo(1);
    assertThat(retrieved.get().getValue().toStringUtf8()).isEqualTo("A manifest");

    List<StorageItem> contacts = contactsManager.getItems(user, List.of(ByteString.copyFromUtf8("mykey"))).get();

    assertThat(contacts.size()).isEqualTo(1);
    assertThat(contacts.getFirst().getKey().toStringUtf8()).isEqualTo("mykey");
    assertThat(contacts.getFirst().getValue().toStringUtf8()).isEqualTo("myvalue");
  }

  @Test
  void testSetStaleManifest() throws Exception {
    UUID userId = UUID.randomUUID();
    User user = new User(userId);
    StorageManager contactsManager = storageManager();

    StorageManifest manifest = StorageManifest.newBuilder()
        .setVersion(1)
        .setValue(ByteString.copyFromUtf8("A manifest"))
        .build();

    StorageManifest staleManifest = StorageManifest.newBuilder()
        .setVersion(1)
        .setValue(ByteString.copyFromUtf8("A stale value"))
        .build();

    StorageItem contact = StorageItem.newBuilder()
        .setKey(ByteString.copyFromUtf8("mykey"))
        .setValue(ByteString.copyFromUtf8("myvalue"))
        .build();

    StorageItem staleContact = StorageItem.newBuilder()
        .setKey(ByteString.copyFromUtf8("stalekey"))
        .setValue(ByteString.copyFromUtf8("stalevalue"))
        .build();

    Optional<StorageManifest> initialInsert = contactsManager.set(user, manifest, List.of(contact), new LinkedList<>())
        .get();
    assertTrue(initialInsert.isEmpty());

    Optional<StorageManifest> staleInsert = contactsManager.set(user, staleManifest, List.of(staleContact),
        List.of(ByteString.copyFromUtf8("mykey"))).get();
    assertTrue(staleInsert.isPresent());
    assertThat(staleInsert.get().getValue().toStringUtf8()).isEqualTo("A manifest");
    assertThat(staleInsert.get().getVersion()).isEqualTo(1);

    Optional<StorageManifest> retrieved = contactsManager.getManifest(user).get();

    assertTrue(retrieved.isPresent());
    assertThat(retrieved.get().getVersion()).isEqualTo(1);
    assertThat(retrieved.get().getValue().toStringUtf8()).isEqualTo("A manifest");

    List<StorageItem> contacts = contactsManager.getItems(user,
        List.of(ByteString.copyFromUtf8("mykey"), ByteString.copyFromUtf8("stalekey"))).get();

    assertThat(contacts.size()).isEqualTo(1);
    assertThat(contacts.getFirst().getKey().toStringUtf8()).isEqualTo("mykey");
    assertThat(contacts.getFirst().getValue().toStringUtf8()).isEqualTo("myvalue");
  }

  @Test
  void testSetUpdatedManifest() throws Exception {
    UUID userId = UUID.randomUUID();
    User user = new User(userId);
    StorageManager contactsManager = storageManager();

    StorageManifest manifest = StorageManifest.newBuilder()
        .setVersion(1)
        .setValue(ByteString.copyFromUtf8("A manifest"))
        .build();

    StorageManifest updatedManifest = StorageManifest.newBuilder()
        .setVersion(2)
        .setValue(ByteString.copyFromUtf8("An updated manifest"))
        .build();

    StorageItem contact = StorageItem.newBuilder()
        .setKey(ByteString.copyFromUtf8("mykey"))
        .setValue(ByteString.copyFromUtf8("myvalue"))
        .build();

    StorageItem updatedContact = StorageItem.newBuilder()
        .setKey(ByteString.copyFromUtf8("updatedkey"))
        .setValue(ByteString.copyFromUtf8("updatedvalue"))
        .build();

    Optional<StorageManifest> initialInsert = contactsManager.set(user, manifest, List.of(contact), new LinkedList<>())
        .get();
    assertTrue(initialInsert.isEmpty());

    Optional<StorageManifest> updatedInsert = contactsManager.set(user, updatedManifest, List.of(updatedContact),
        List.of(ByteString.copyFromUtf8("mykey"))).get();
    assertTrue(updatedInsert.isEmpty());

    Optional<StorageManifest> retrieved = contactsManager.getManifest(user).get();

    assertTrue(retrieved.isPresent());
    assertThat(retrieved.get().getVersion()).isEqualTo(2);
    assertThat(retrieved.get().getValue().toStringUtf8()).isEqualTo("An updated manifest");

    List<StorageItem> contacts = contactsManager.getItems(user,
        List.of(ByteString.copyFromUtf8("mykey"), ByteString.copyFromUtf8("updatedkey"))).get();

    assertThat(contacts.size()).isEqualTo(1);
    assertThat(contacts.getFirst().getKey().toStringUtf8()).isEqualTo("updatedkey");
    assertThat(contacts.getFirst().getValue().toStringUtf8()).isEqualTo("updatedvalue");
  }

  @Test
  void testSetNoMutations() {
    final User user = new User(UUID.randomUUID());
    final StorageManager contactsManager = storageManager();

    final StorageManifest manifest = StorageManifest.newBuilder()
        .setVersion(1)
        .setValue(ByteString.copyFromUtf8("A manifest"))
        .build();

    assertTrue(contactsManager.set(user, manifest, Collections.emptyList(), Collections.emptyList()).join().isEmpty());
    assertEquals(Optional.of(manifest), contactsManager.getManifest(user).join());
  }

  @Test
  void testSetLargeRequest() throws Exception {
    final User user = new User(UUID.randomUUID());
    final StorageManager storageManager = storageManager();

    final StorageManifest manifest = StorageManifest.newBuilder()
        .setVersion(1)
        .setValue(ByteString.copyFromUtf8("A manifest"))
        .build();

    // Make sure we have multiple "pages" of mutations
    final int insertCount = (StorageItemsTable.MAX_MUTATIONS / StorageItemsTable.MUTATIONS_PER_INSERT) + 1;
    final int deleteCount = StorageItemsTable.MAX_MUTATIONS + 1;

    final List<StorageItem> inserts = IntStream.range(0, insertCount)
        .mapToObj(i -> StorageItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("key-" + i))
            .setValue(ByteString.copyFromUtf8("value-" + i))
            .build())
        .toList();

    final List<ByteString> deletes = IntStream.range(0, deleteCount)
        .mapToObj(i -> ByteString.copyFromUtf8("deleted-key-" + i))
        .toList();

    final Optional<StorageManifest> result = storageManager.set(user, manifest, inserts, deletes).get();

    assertTrue(result.isEmpty());
    assertEquals(Optional.of(manifest), storageManager.getManifest(user).join());

    assertEquals(insertCount, countRawItems(user.getUuid()));
  }

  @Test
  void testClearItems() throws Exception {
    UUID userId = UUID.randomUUID();
    User user = new User(userId);

    UUID secondUserId = UUID.randomUUID();

    StorageManager contactsManager = storageManager();

    final List<Map.Entry<String, String>> items = new ArrayList<>();
    final List<Map.Entry<String, String>> secondItems = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      items.add(Map.entry("somekey" + String.format("%03d", i), "data" + String.format("%03d", i)));
      secondItems.add(Map.entry("somekey" + String.format("%03d", i), "seconddata" + String.format("%03d", i)));
    }
    writeRawItems(userId, items);
    writeRawItems(secondUserId, secondItems);

    List<String> data = rawItemData(userId);
    assertThat(data.size()).isEqualTo(100);
    for (int i = 0; i < 100; i++) {
      assertThat(data.get(i)).isEqualTo("data" + String.format("%03d", i));
    }

    data = rawItemData(secondUserId);
    assertThat(data.size()).isEqualTo(100);
    for (int i = 0; i < 100; i++) {
      assertThat(data.get(i)).isEqualTo("seconddata" + String.format("%03d", i));
    }

    contactsManager.clearItems(user).get();

    assertThat(rawItemData(userId).size()).isEqualTo(0);

    data = rawItemData(secondUserId);
    assertThat(data.size()).isEqualTo(100);
    for (int i = 0; i < 100; i++) {
      assertThat(data.get(i)).isEqualTo("seconddata" + String.format("%03d", i));
    }
  }

  @Test
  void testClearItemsLargeBatch() throws Exception {
    UUID userId = UUID.randomUUID();
    User user = new User(userId);

    StorageManager contactsManager = storageManager();

    for (int chunk = 0; chunk < 2; chunk++) {
      final List<Map.Entry<String, String>> items = new ArrayList<>();

      // Each setCell() is a mutation
      for (int i = 0; i < StorageItemsTable.MAX_MUTATIONS; i += StorageItemsTable.MUTATIONS_PER_INSERT) {
        items.add(Map.entry(String.format("somekey%d_%05d", chunk, i), "data" + String.format("%03d", i)));
      }

      writeRawItems(userId, items);
    }

    assertDoesNotThrow(() -> contactsManager.clearItems(user).join());

    assertEquals(0, countRawItems(userId));
  }

  @Test
  void testDelete() throws Exception {
    UUID userId = UUID.randomUUID();
    User user = new User(userId);

    UUID secondUserId = UUID.randomUUID();
    User secondUser = new User(secondUserId);

    StorageManager contactsManager = storageManager();

    writeRawManifest(userId, "1", "A manifest");
    writeRawManifest(secondUserId, "1", "A different manifest");

    final List<Map.Entry<String, String>> items = new ArrayList<>();
    final List<Map.Entry<String, String>> secondItems = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      items.add(Map.entry("somekey" + String.format("%03d", i), "data" + String.format("%03d", i)));
      secondItems.add(Map.entry("somekey" + String.format("%03d", i), "seconddata" + String.format("%03d", i)));
    }
    writeRawItems(userId, items);
    writeRawItems(secondUserId, secondItems);

    List<String> data = rawItemData(userId);
    assertThat(data.size()).isEqualTo(100);
    for (int i = 0; i < 100; i++) {
      assertThat(data.get(i)).isEqualTo("data" + String.format("%03d", i));
    }

    data = rawItemData(secondUserId);
    assertThat(data.size()).isEqualTo(100);
    for (int i = 0; i < 100; i++) {
      assertThat(data.get(i)).isEqualTo("seconddata" + String.format("%03d", i));
    }

    contactsManager.delete(user).join();

    assertThat(rawItemData(userId).size()).isEqualTo(0);

    data = rawItemData(secondUserId);
    assertThat(data.size()).isEqualTo(100);
    for (int i = 0; i < 100; i++) {
      assertThat(data.get(i)).isEqualTo("seconddata" + String.format("%03d", i));
    }

    assertFalse(contactsManager.getManifest(user).join().isPresent());

    Optional<StorageManifest> manifest = contactsManager.getManifest(secondUser).join();
    assertTrue(manifest.isPresent());
    assertThat(manifest.get().getVersion()).isEqualTo(1);
    assertThat(manifest.get().getValue().toStringUtf8()).isEqualTo("A different manifest");
  }

}
