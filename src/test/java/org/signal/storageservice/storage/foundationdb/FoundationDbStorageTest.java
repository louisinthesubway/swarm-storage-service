/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage.foundationdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.storageservice.auth.User;
import org.signal.storageservice.controllers.FoundationDbReadinessController;
import org.signal.storageservice.storage.GroupsManager;
import org.signal.storageservice.storage.StorageManager;
import org.signal.storageservice.storage.protos.contacts.StorageItem;
import org.signal.storageservice.storage.protos.contacts.StorageManifest;
import org.signal.storageservice.storage.protos.groups.AccessControl;
import org.signal.storageservice.storage.protos.groups.Group;
import org.signal.storageservice.storage.protos.groups.GroupChange;
import org.signal.storageservice.storage.protos.groups.GroupChange.Actions;
import org.signal.storageservice.storage.protos.groups.GroupChange.Actions.AddMemberAction;
import org.signal.storageservice.storage.protos.groups.GroupChanges.GroupChangeState;
import org.signal.storageservice.storage.protos.groups.Member;
import org.signal.storageservice.storage.protos.groups.MemberBanned;
import org.signal.storageservice.storage.protos.groups.MemberPendingAdminApproval;

/// SWARM: what only the FoundationDB backend needs proving: records above FoundationDB's 100,000-byte value limit
/// (the largest group the validators allow), records that shrink, racing conditional writes, the maybe-committed
/// branch of the conditional writes, paged log reads, the readiness check, and that nothing is written outside the
/// configured directory. Needs a FoundationDB cluster (see [FoundationDbExtension]). docs/SWARM-CHANGES.md, 3.6 and
/// 3.10.
class FoundationDbStorageTest {

  @RegisterExtension
  static final FoundationDbExtension FOUNDATION_DB = new FoundationDbExtension();

  private static final SecureRandom RANDOM = new SecureRandom();

  // storage.yml and the validators: group.maxGroupSize, title/description limits, label ciphertext limits,
  // the disappearing-messages timer, the size of zkgroup ciphertexts
  private static final int MAX_GROUP_SIZE = 1001;
  private static final int MAX_TITLE_BYTES = 1024;
  private static final int MAX_DESCRIPTION_BYTES = 8192;
  private static final int MAX_LABEL_EMOJI_BYTES = 64;
  private static final int MAX_LABEL_STRING_BYTES = 512;
  private static final int MAX_TIMER_BYTES = 42;
  private static final int CIPHERTEXT_BYTES = 65;

  private static ByteString randomBytes(final int length) {
    final byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return ByteString.copyFrom(bytes);
  }

  private static ByteString randomGroupId() {
    return randomBytes(32);
  }

  private static Member member(final boolean withLabels) {
    final Member.Builder member = Member.newBuilder()
        .setUserId(randomBytes(CIPHERTEXT_BYTES))
        .setProfileKey(randomBytes(CIPHERTEXT_BYTES))
        .setRole(Member.Role.DEFAULT)
        .setJoinedAtVersion(Integer.MAX_VALUE);
    if (withLabels) {
      member.setLabelEmoji(randomBytes(MAX_LABEL_EMOJI_BYTES)).setLabelString(randomBytes(MAX_LABEL_STRING_BYTES));
    }
    return member.build();
  }

  /// The largest group the validators accept from honest clients: 1001 members with both labels at their maximum,
  /// 1001 join requests, 1001 bans, maximal title, description and timer.
  static Group largestGroup(final int version) {
    final Group.Builder group = Group.newBuilder()
        .setPublicKey(randomBytes(97))
        .setTitle(randomBytes(MAX_TITLE_BYTES))
        .setDescription(randomBytes(MAX_DESCRIPTION_BYTES))
        .setAvatarUrl("groups/" + "A".repeat(43) + "/" + "B".repeat(22))
        .setDisappearingMessagesTimer(randomBytes(MAX_TIMER_BYTES))
        .setAccessControl(AccessControl.newBuilder()
            .setMembers(AccessControl.AccessRequired.ADMINISTRATOR)
            .setAttributes(AccessControl.AccessRequired.ADMINISTRATOR)
            .setAddFromInviteLink(AccessControl.AccessRequired.ADMINISTRATOR))
        .setInviteLinkPassword(randomBytes(16))
        .setVersion(version);

    for (int i = 0; i < MAX_GROUP_SIZE; i++) {
      group.addMembers(member(true));
      group.addMembersPendingAdminApproval(MemberPendingAdminApproval.newBuilder()
          .setUserId(randomBytes(CIPHERTEXT_BYTES))
          .setProfileKey(randomBytes(CIPHERTEXT_BYTES))
          .setTimestamp(System.currentTimeMillis()));
      group.addMembersBanned(MemberBanned.newBuilder()
          .setUserId(randomBytes(CIPHERTEXT_BYTES))
          .setTimestamp(System.currentTimeMillis()));
    }

    return group.build();
  }

  /// A full group as it usually looks: 1001 members, no labels, no requests, no bans.
  static Group typicalFullGroup(final int version) {
    final Group.Builder group = Group.newBuilder()
        .setPublicKey(randomBytes(97))
        .setTitle(randomBytes(64))
        .setVersion(version);
    for (int i = 0; i < MAX_GROUP_SIZE; i++) {
      group.addMembers(member(false));
    }
    return group.build();
  }

  private static GroupsManager groupsManager(final FoundationDbStorage storage) {
    return new GroupsManager(new FoundationDbGroupsTable(storage), new FoundationDbGroupLogTable(storage));
  }

  private static StorageManager storageManager(final FoundationDbStorage storage) {
    return new StorageManager(new FoundationDbStorageManifestsTable(storage), new FoundationDbStorageItemsTable(storage));
  }

  private static List<KeyValue> keyValues(final Range range) {
    return FOUNDATION_DB.getDatabase().read(transaction -> transaction.getRange(range).asList().join());
  }

  @Test
  void largestGroupRoundTrips() {
    final GroupsManager groupsManager = groupsManager(FOUNDATION_DB.getStorage());
    final ByteString groupId = randomGroupId();

    final Group typical = typicalFullGroup(0);
    final Group largest = largestGroup(0);
    System.out.printf("[limits] typical 1001-member group: %d bytes; largest group the validators allow: %d bytes%n",
        typical.getSerializedSize(), largest.getSerializedSize());

    assertThat(typical.getSerializedSize()).isGreaterThan(FoundationDbRecords.MAX_VALUE_BYTES);
    assertThat(largest.getSerializedSize()).isBetween(900_000, 1_000_000);

    assertThat(groupsManager.createGroup(groupId, largest).join()).isTrue();
    assertThat(groupsManager.getGroup(groupId).join()).contains(largest);

    // every stored value is within FoundationDB's limit, and the data is split into consecutive chunks
    final Subspace groups = FOUNDATION_DB.getStorage().getGroups();
    final List<KeyValue> stored = keyValues(groups.range(Tuple.from(groupId.toByteArray())));
    assertThat(stored).allSatisfy(keyValue -> assertThat(keyValue.getValue().length)
        .isLessThanOrEqualTo(FoundationDbRecords.MAX_VALUE_BYTES));
    assertThat(stored).hasSize(1 + (largest.getSerializedSize() + FoundationDbRecords.MAX_VALUE_BYTES - 1)
        / FoundationDbRecords.MAX_VALUE_BYTES);

    // the next version, and the log entry of a change that adds a thousand members at once
    final Group next = largestGroup(1);
    assertThat(groupsManager.updateGroup(groupId, next).join()).isEmpty();
    assertThat(groupsManager.getGroup(groupId).join()).contains(next);

    final Actions.Builder actions = Actions.newBuilder().setVersion(1).setSourceUserId(randomBytes(CIPHERTEXT_BYTES));
    for (int i = 0; i < 1000; i++) {
      actions.addAddMembers(AddMemberAction.newBuilder().setAdded(member(true)));
    }
    final GroupChange change = GroupChange.newBuilder()
        .setActions(actions.build().toByteString())
        .setServerSignature(randomBytes(64))
        .setChangeEpoch(5)
        .build();
    System.out.printf("[limits] log entry: change %d bytes + state %d bytes%n", change.getSerializedSize(),
        next.getSerializedSize());

    assertThat(groupsManager.appendChangeRecord(groupId, 1, change, next).join()).isTrue();

    final List<GroupChangeState> records = groupsManager.getChangeRecords(groupId, next, null, false, false, 1, 2).join();
    assertThat(records).hasSize(1);
    assertThat(records.getFirst().getGroupChange()).isEqualTo(change);
    assertThat(records.getFirst().getGroupState()).isEqualTo(next);
  }

  @Test
  void shrinkingRecordsLeaveNoChunksBehind() {
    final FoundationDbStorage storage = FOUNDATION_DB.getStorage();
    final GroupsManager groupsManager = groupsManager(storage);
    final ByteString groupId = randomGroupId();

    final Group large = typicalFullGroup(0);
    final Group small = Group.newBuilder().setPublicKey(randomBytes(97)).setTitle(randomBytes(10)).setVersion(1).build();

    assertThat(groupsManager.createGroup(groupId, large).join()).isTrue();
    assertThat(groupsManager.updateGroup(groupId, small).join()).isEmpty();
    assertThat(groupsManager.getGroup(groupId).join()).contains(small);
    // one data chunk and the version
    assertThat(keyValues(storage.getGroups().range(Tuple.from(groupId.toByteArray())))).hasSize(2);

    // the same for an item and a manifest
    final StorageManager storageManager = storageManager(storage);
    final User user = new User(UUID.randomUUID());
    final ByteString key = randomBytes(16);
    final ByteString bigValue = randomBytes(250_000);
    final ByteString smallValue = randomBytes(10);

    assertThat(storageManager.set(user, manifest(1, 250_000), List.of(item(key, bigValue)), List.of()).join()).isEmpty();
    assertThat(keyValues(storage.getStorageItems().range(Tuple.from(user.getUuid(), key.toByteArray())))).hasSize(3);

    final StorageManifest smallManifest = manifest(2, 10);
    assertThat(storageManager.set(user, smallManifest, List.of(item(key, smallValue)), List.of()).join()).isEmpty();
    assertThat(storageManager.getItems(user, List.of(key)).join()).containsExactly(item(key, smallValue));
    assertThat(storageManager.getManifest(user).join()).contains(smallManifest);
    assertThat(keyValues(storage.getStorageItems().range(Tuple.from(user.getUuid(), key.toByteArray())))).hasSize(1);
    assertThat(keyValues(storage.getStorageManifests().range(Tuple.from(user.getUuid())))).hasSize(2);
  }

  @Test
  void largeManifestAndItemsRoundTrip() {
    final StorageManager storageManager = storageManager(FOUNDATION_DB.getStorage());
    final User user = new User(UUID.randomUUID());

    final StorageManifest manifest = manifest(1, 350_000);
    final List<StorageItem> items = new ArrayList<>();
    items.add(item(randomBytes(16), randomBytes(300_000)));
    items.add(item(randomBytes(16), ByteString.EMPTY));
    IntStream.range(0, 100).forEach(i -> items.add(item(randomBytes(16), randomBytes(200))));

    assertThat(storageManager.set(user, manifest, items, List.of()).join()).isEmpty();
    assertThat(storageManager.getManifest(user).join()).contains(manifest);
    assertThat(storageManager.getManifestIfNotVersion(user, 1).join()).isEmpty();
    assertThat(storageManager.getManifestIfNotVersion(user, 0).join()).contains(manifest);

    // in key order (unsigned bytes), each key once, absent keys left out
    final List<ByteString> keys = new ArrayList<>(items.stream().map(StorageItem::getKey).toList());
    keys.add(items.getFirst().getKey());
    keys.add(randomBytes(16));
    final List<StorageItem> expected = items.stream()
        .sorted((a, b) -> ByteString.unsignedLexicographicalComparator().compare(a.getKey(), b.getKey()))
        .toList();
    assertThat(storageManager.getItems(user, keys).join()).containsExactlyElementsOf(expected);
  }

  @Test
  void racingConditionalWritesHaveExactlyOneWinner() {
    final GroupsManager groupsManager = groupsManager(FOUNDATION_DB.getStorage());
    final int writers = 8;

    // create
    final ByteString groupId = randomGroupId();
    final List<Group> candidates = IntStream.range(0, writers)
        .mapToObj(i -> Group.newBuilder().setPublicKey(randomBytes(97)).setTitle(ByteString.copyFromUtf8("t" + i)).setVersion(0).build())
        .toList();
    final List<Boolean> created = join(candidates.stream().map(group -> groupsManager.createGroup(groupId, group)).toList());
    assertThat(created.stream().filter(Boolean::booleanValue)).hasSize(1);
    final Group winner = candidates.get(created.indexOf(true));
    assertThat(groupsManager.getGroup(groupId).join()).contains(winner);

    // update from version 0 to 1
    final List<Group> updates = IntStream.range(0, writers)
        .mapToObj(i -> winner.toBuilder().setTitle(ByteString.copyFromUtf8("u" + i)).setVersion(1).build())
        .toList();
    final List<Optional<Group>> updated = join(updates.stream().map(group -> groupsManager.updateGroup(groupId, group)).toList());
    assertThat(updated.stream().filter(Optional::isEmpty)).hasSize(1);
    final Group updateWinner = updates.get(updated.indexOf(Optional.empty()));
    // every loser is told the state that won, as upstream's GroupsManager.updateGroup does
    assertThat(updated.stream().filter(Optional::isPresent).map(Optional::get)).allMatch(updateWinner::equals);

    // log entries for the same version
    final List<Boolean> appended = join(IntStream.range(0, writers)
        .mapToObj(i -> groupsManager.appendChangeRecord(groupId, 1,
            GroupChange.newBuilder().setActions(ByteString.copyFromUtf8("change " + i)).build(), updates.get(i)))
        .toList());
    assertThat(appended.stream().filter(Boolean::booleanValue)).hasSize(1);

    // manifests: every writer moves version 1 to version 2
    final StorageManager storageManager = storageManager(FOUNDATION_DB.getStorage());
    final User user = new User(UUID.randomUUID());
    assertThat(storageManager.set(user, manifest(1, 100), List.of(), List.of()).join()).isEmpty();
    final List<StorageManifest> manifests = IntStream.range(0, writers).mapToObj(i -> manifest(2, 100)).toList();
    final List<Optional<StorageManifest>> results = join(manifests.stream()
        .map(manifest -> storageManager.set(user, manifest, List.of(), List.of()))
        .toList());
    assertThat(results.stream().filter(Optional::isEmpty)).hasSize(1);
    final StorageManifest manifestWinner = manifests.get(results.indexOf(Optional.empty()));
    assertThat(results.stream().filter(Optional::isPresent).map(Optional::get)).allMatch(manifestWinner::equals);
  }

  /// After a `commit_unknown_result` the retry sees `mayHaveCommitted == true`: if the stored record is exactly the
  /// one it meant to write, the write did land and counts as done; anything else is still a conflict.
  @Test
  void maybeCommittedRetryRecognisesItsOwnWrite() {
    final FoundationDbStorage storage = FOUNDATION_DB.getStorage();
    final FoundationDbStorage retrying = new FoundationDbStorage(storage.getDatabase(), storage.getDirectory(),
        storage.getGroups(), storage.getGroupLogs(), storage.getStorageManifests(), storage.getStorageItems(), false) {
      @Override
      public <T> CompletableFuture<T> write(final BiFunction<Transaction, Boolean, CompletableFuture<T>> body) {
        return super.write((transaction, mayHaveCommitted) -> body.apply(transaction, true));
      }
    };

    final FoundationDbGroupsTable firstAttempt = new FoundationDbGroupsTable(storage);
    final FoundationDbGroupsTable retry = new FoundationDbGroupsTable(retrying);
    final ByteString groupId = randomGroupId();
    final Group group = Group.newBuilder().setPublicKey(randomBytes(97)).setVersion(0).build();
    final Group other = Group.newBuilder().setPublicKey(randomBytes(97)).setVersion(0).build();

    assertThat(firstAttempt.createGroup(groupId, group).join()).isTrue();
    // an identical create: a conflict on a first attempt (as upstream), success on a maybe-committed retry
    assertThat(firstAttempt.createGroup(groupId, group).join()).isFalse();
    assertThat(retry.createGroup(groupId, group).join()).isTrue();
    assertThat(retry.createGroup(groupId, other).join()).isFalse();

    final Group next = group.toBuilder().setVersion(1).build();
    assertThat(firstAttempt.updateGroup(groupId, next).join()).isTrue();
    assertThat(firstAttempt.updateGroup(groupId, next).join()).isFalse();
    assertThat(retry.updateGroup(groupId, next).join()).isTrue();
    assertThat(retry.updateGroup(groupId, other.toBuilder().setVersion(1).build()).join()).isFalse();

    final FoundationDbStorageManifestsTable manifests = new FoundationDbStorageManifestsTable(storage);
    final FoundationDbStorageManifestsTable retryManifests = new FoundationDbStorageManifestsTable(retrying);
    final User user = new User(UUID.randomUUID());
    assertThat(manifests.set(user, manifest(1, 10)).join()).isTrue();
    final StorageManifest second = manifest(2, 10);
    assertThat(manifests.set(user, second).join()).isTrue();
    assertThat(manifests.set(user, second).join()).isFalse();
    assertThat(retryManifests.set(user, second).join()).isTrue();
    assertThat(retryManifests.set(user, manifest(2, 10)).join()).isFalse();

    final FoundationDbGroupLogTable logs = new FoundationDbGroupLogTable(storage);
    final FoundationDbGroupLogTable retryLogs = new FoundationDbGroupLogTable(retrying);
    final GroupChange change = GroupChange.newBuilder().setActions(randomBytes(10)).build();
    assertThat(logs.append(groupId, 1, change, next).join()).isTrue();
    assertThat(logs.append(groupId, 1, change, next).join()).isFalse();
    assertThat(retryLogs.append(groupId, 1, change, next).join()).isTrue();
    assertThat(retryLogs.append(groupId, 1, GroupChange.newBuilder().setActions(randomBytes(10)).build(), next).join())
        .isFalse();
  }

  @Test
  void logReadsPageThroughEntriesThatStraddlePages() {
    final GroupsManager groupsManager = groupsManager(FOUNDATION_DB.getStorage());
    final ByteString groupId = randomGroupId();

    // 60 entries of 5 key-values each (a 3-chunk state, a 1-chunk change, the version): 300 key-values, so the
    // 256-key-value page ends in the middle of an entry
    final int versions = 60;
    Group latest = null;
    for (int version = 1; version <= versions; version++) {
      latest = Group.newBuilder().setVersion(version).setTitle(randomBytes(250_000)).build();
      final GroupChange change = GroupChange.newBuilder()
          .setActions(ByteString.copyFromUtf8("change " + version)).setChangeEpoch(version % 3).build();
      assertThat(groupsManager.appendChangeRecord(groupId, version, change, latest).join()).isTrue();
    }
    assertThat(versions * 5).isGreaterThan(FoundationDbGroupLogTable.PAGE_KEY_VALUES);

    final List<GroupChangeState> records = groupsManager.getChangeRecords(groupId, latest, null, false, false, 1, versions + 1).join();
    assertThat(records).hasSize(versions);
    for (int i = 0; i < versions; i++) {
      assertThat(records.get(i).getGroupChange().getActions().toStringUtf8()).isEqualTo("change " + (i + 1));
      assertThat(records.get(i).getGroupState().getVersion()).isEqualTo(i + 1);
    }

    // with an epoch limit only the first and last states, and those of newer epochs, are included
    final List<GroupChangeState> limited = groupsManager.getChangeRecords(groupId, latest, 1, true, true, 1, versions + 1).join();
    assertThat(limited).hasSize(versions);
    for (int i = 0; i < versions; i++) {
      final int version = i + 1;
      final boolean expectState = version == 1 || version == versions || version % 3 > 1;
      assertThat(limited.get(i).hasGroupState()).as("state of version %d", version).isEqualTo(expectState);
    }

    // a range beyond the log, and a range with a hole, return what exists
    assertThat(groupsManager.getChangeRecords(groupId, latest, null, false, false, versions + 1, versions + 10).join())
        .isEmpty();
  }

  @Test
  void readinessWarmsUpThenAnswersReady() {
    final FoundationDbReadinessController controller = new FoundationDbReadinessController(FOUNDATION_DB.getStorage(), 2);
    assertThat(controller.isReady()).isEqualTo("ready");
    assertThat(controller.isReady()).isEqualTo("ready");
    assertThat(controller.isReady()).isEqualTo("ready");

    final FoundationDbStorage unreachable = new FoundationDbStorage(FOUNDATION_DB.getDatabase(), List.of("unused"),
        new Subspace(Tuple.from("g")), new Subspace(Tuple.from("l")), new Subspace(Tuple.from("m")),
        new Subspace(Tuple.from("i")), false) {
      @Override
      public CompletableFuture<Void> warmUp() {
        return CompletableFuture.failedFuture(new IllegalStateException("cluster unavailable"));
      }
    };
    final FoundationDbReadinessController failing = new FoundationDbReadinessController(unreachable, 1);
    assertThatThrownBy(failing::isReady).hasRootCauseMessage("cluster unavailable");
    // after the warm-up calls, as upstream, no more reads
    assertThat(failing.isReady()).isEqualTo("ready");
  }

  @Test
  void nothingIsWrittenOutsideTheDirectory() {
    final FoundationDbStorage storage = FOUNDATION_DB.getStorage();
    final Set<String> before = keysOutsideDirectoryMetadata();

    final GroupsManager groupsManager = groupsManager(storage);
    final ByteString groupId = randomGroupId();
    final Group group = Group.newBuilder().setPublicKey(randomBytes(97)).setVersion(0).build();
    groupsManager.createGroup(groupId, group).join();
    groupsManager.appendChangeRecord(groupId, 0, GroupChange.newBuilder().setActions(randomBytes(5)).build(), group).join();
    final StorageManager storageManager = storageManager(storage);
    final User user = new User(UUID.randomUUID());
    storageManager.set(user, manifest(1, 10), List.of(item(randomBytes(16), randomBytes(10))), List.of()).join();

    final Set<String> added = keysOutsideDirectoryMetadata();
    added.removeAll(before);

    final List<Subspace> subdirectories = List.of(storage.getGroups(), storage.getGroupLogs(),
        storage.getStorageManifests(), storage.getStorageItems());
    assertThat(added).isNotEmpty();
    assertThat(added).allSatisfy(key -> assertThat(subdirectories.stream()
        .anyMatch(subspace -> subspace.contains(HexFormat.of().parseHex(key)))).isTrue());

    // another directory sees none of it
    final FoundationDbStorage other = FoundationDbStorage.open(FOUNDATION_DB.getDatabase(),
        List.of(FoundationDbExtension.TEST_ROOT, UUID.randomUUID().toString()), false).join();
    try {
      assertThat(groupsManager(other).getGroup(groupId).join()).isEmpty();
      assertThat(storageManager(other).getManifest(user).join()).isEmpty();
    } finally {
      DirectoryLayer.getDefault()
          .removeIfExists(FOUNDATION_DB.getDatabase(), other.getDirectory()).join();
    }
  }

  /// Every key below `\xFE` (FoundationDB's directory metadata and system keys live at and above it), as hex.
  private static Set<String> keysOutsideDirectoryMetadata() {
    final Set<String> keys = new TreeSet<>();
    byte[] begin = new byte[0];
    final byte[] end = new byte[] {(byte) 0xfe};
    while (true) {
      final byte[] pageBegin = begin;
      final List<KeyValue> page = FOUNDATION_DB.getDatabase().read(transaction ->
          transaction.getRange(pageBegin, end, 10_000).asList().join());
      page.forEach(keyValue -> keys.add(HexFormat.of().formatHex(keyValue.getKey())));
      if (page.size() < 10_000) {
        return keys;
      }
      final byte[] last = page.getLast().getKey();
      begin = Arrays.copyOf(last, last.length + 1);
    }
  }

  private static StorageManifest manifest(final long version, final int bytes) {
    return StorageManifest.newBuilder().setVersion(version).setValue(randomBytes(bytes)).build();
  }

  private static StorageItem item(final ByteString key, final ByteString value) {
    return StorageItem.newBuilder().setKey(key).setValue(value).build();
  }

  private static <T> List<T> join(final List<CompletableFuture<T>> futures) {
    CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
    return futures.stream().map(CompletableFuture::join).toList();
  }
}
