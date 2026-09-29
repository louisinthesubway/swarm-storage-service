/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.zkgroup.groups.GroupPublicParams;
import org.signal.libsignal.zkgroup.groups.GroupSecretParams;
import org.signal.storageservice.storage.protos.groups.AccessControl;
import org.signal.storageservice.storage.protos.groups.Group;
import org.signal.storageservice.storage.protos.groups.GroupChange;
import org.signal.storageservice.storage.protos.groups.GroupChange.Actions;
import org.signal.storageservice.storage.protos.groups.GroupChange.Actions.ModifyTitleAction;
import org.signal.storageservice.storage.protos.groups.GroupChanges.GroupChangeState;
import org.signal.storageservice.util.AuthHelper;

/// SWARM: upstream's GroupsManagerTest, run against every storage backend. The test methods are upstream's; where
/// upstream read raw Bigtable rows, the tests ask the backend through [#storedGroup] and [#storedLogEntry]. Subclasses:
/// [BigtableGroupsManagerTest] (upstream's emulator setup) and `FoundationDbGroupsManagerTest`. See
/// docs/SWARM-CHANGES.md, section 3.10.
abstract class GroupsManagerTest {

  /// What a backend stores for a group: the version as text and the `Group` bytes (Bigtable: `g:ver`, `g:gr`).
  protected record StoredGroup(String version, ByteString groupData) {
  }

  /// What a backend stores for one log entry (Bigtable: `l:v`, `l:c`, `l:s`).
  protected record StoredLogEntry(String version, ByteString change, ByteString state) {
  }

  /// @return a manager on the backend under test
  protected abstract GroupsManager groupsManager();

  /// @return a manager whose reads of group state fail with `failure`
  protected abstract GroupsManager groupsManagerWithFailingReads(RuntimeException failure);

  /// @return what the backend stores for the group, read directly from the backend
  protected abstract Optional<StoredGroup> storedGroup(ByteString groupId) throws Exception;

  /// @return what the backend stores for the log entry, read directly from the backend
  protected abstract Optional<StoredLogEntry> storedLogEntry(ByteString groupId, int version) throws Exception;

  @Test
  void testCreateGroup() throws Exception {
    GroupsManager groupsManager = groupsManager();

    GroupSecretParams groupSecretParams = GroupSecretParams.generate();
    GroupPublicParams groupPublicParams = groupSecretParams.getPublicParams();
    ByteString        groupId           = ByteString.copyFrom(groupPublicParams.getGroupIdentifier().serialize());

    Group group = Group.newBuilder()
                       .setVersion(0)
                       .setTitle(ByteString.copyFromUtf8("Some title"))
                       .setAvatarUrl("Some avatar")
                       .setAccessControl(AccessControl.newBuilder()
                                                      .setMembers(AccessControl.AccessRequired.MEMBER)
                                                      .setAttributes(AccessControl.AccessRequired.MEMBER))
                       .setPublicKey(ByteString.copyFrom(groupPublicParams.serialize()))
                       .build();

    CompletableFuture<Boolean> result = groupsManager.createGroup(groupId, group);
    assertTrue(result.get());

    StoredGroup stored = storedGroup(groupId).orElseThrow();

    assertThat(stored.version()).isEqualTo("0");
    assertThat(Group.parseFrom(stored.groupData())).isEqualTo(group);
  }

  @Test
  void testCreateGroupConflict() throws Exception {
    GroupsManager groupsManager = groupsManager();

    GroupSecretParams groupSecretParams = GroupSecretParams.generate();
    GroupPublicParams groupPublicParams = groupSecretParams.getPublicParams();
    ByteString        groupId           = ByteString.copyFrom(groupPublicParams.getGroupIdentifier().serialize());

    Group group = Group.newBuilder()
                       .setVersion(0)
                       .setTitle(ByteString.copyFromUtf8("Some title"))
                       .setAvatarUrl("Some avatar")
                       .setAccessControl(AccessControl.newBuilder()
                                                      .setMembers(AccessControl.AccessRequired.MEMBER)
                                                      .setAttributes(AccessControl.AccessRequired.MEMBER))
                       .setPublicKey(ByteString.copyFrom(groupPublicParams.serialize()))
                       .build();

    CompletableFuture<Boolean> result = groupsManager.createGroup(groupId, group);
    assertTrue(result.get());

    Group conflictingGroup = Group.newBuilder()
                                  .setVersion(0)
                                  .setTitle(ByteString.copyFromUtf8("Another title"))
                                  .setAvatarUrl("Another avatar")
                                  .setAccessControl(AccessControl.newBuilder()
                                                                 .setMembers(AccessControl.AccessRequired.MEMBER)
                                                                 .setAttributes(AccessControl.AccessRequired.MEMBER))
                                  .setPublicKey(ByteString.copyFrom(groupPublicParams.serialize()))
                                  .build();

    CompletableFuture<Boolean> conflicting = groupsManager.createGroup(groupId, group);
    assertFalse(conflicting.get());

    StoredGroup stored = storedGroup(groupId).orElseThrow();

    assertThat(stored.version()).isEqualTo("0");
    assertThat(Group.parseFrom(stored.groupData())).isEqualTo(group);
    assertThat(Group.parseFrom(stored.groupData())).isNotEqualTo(conflictingGroup);
  }

  @Test
  void testUpdateGroup() throws Exception {
    GroupsManager groupsManager = groupsManager();

    GroupSecretParams groupSecretParams = GroupSecretParams.generate();
    GroupPublicParams groupPublicParams = groupSecretParams.getPublicParams();
    ByteString        groupId           = ByteString.copyFrom(groupPublicParams.getGroupIdentifier().serialize());

    Group group = Group.newBuilder()
                       .setVersion(0)
                       .setTitle(ByteString.copyFromUtf8("Some title"))
                       .setAvatarUrl("Some avatar")
                       .setAccessControl(AccessControl.newBuilder()
                                                      .setMembers(AccessControl.AccessRequired.MEMBER)
                                                      .setAttributes(AccessControl.AccessRequired.MEMBER))
                       .setPublicKey(ByteString.copyFrom(groupPublicParams.serialize()))
                       .build();

    CompletableFuture<Boolean> result = groupsManager.createGroup(groupId, group);
    assertTrue(result.get());

    Group updated = group.toBuilder()
                         .setVersion(1)
                         .setTitle(ByteString.copyFromUtf8("Updated title"))
                         .build();

    CompletableFuture<Optional<Group>> update = groupsManager.updateGroup(groupId, updated);
    assertThat(update.get()).isEmpty();

    StoredGroup stored = storedGroup(groupId).orElseThrow();

    assertThat(stored.version()).isEqualTo("1");
    assertThat(Group.parseFrom(stored.groupData())).isEqualTo(updated);
    assertThat(Group.parseFrom(stored.groupData())).isNotEqualTo(group);
  }

  @Test
  void testUpdateStaleGroup() throws Exception {
    GroupsManager groupsManager = groupsManager();

    GroupSecretParams groupSecretParams = GroupSecretParams.generate();
    GroupPublicParams groupPublicParams = groupSecretParams.getPublicParams();
    ByteString        groupId           = ByteString.copyFrom(groupPublicParams.getGroupIdentifier().serialize());

    Group group = Group.newBuilder()
                       .setVersion(0)
                       .setTitle(ByteString.copyFromUtf8("Some title"))
                       .setAvatarUrl("Some avatar")
                       .setAccessControl(AccessControl.newBuilder()
                                                      .setMembers(AccessControl.AccessRequired.MEMBER)
                                                      .setAttributes(AccessControl.AccessRequired.MEMBER))
                       .setPublicKey(ByteString.copyFrom(groupPublicParams.serialize()))
                       .build();

    CompletableFuture<Boolean> result = groupsManager.createGroup(groupId, group);
    assertTrue(result.get());

    Group updated = group.toBuilder()
                         .setVersion(0)
                         .setTitle(ByteString.copyFromUtf8("Updated title"))
                         .build();

    CompletableFuture<Optional<Group>> update = groupsManager.updateGroup(groupId, updated);
    assertThat(update.get()).isPresent()
        .get().isEqualTo(group);

    StoredGroup stored = storedGroup(groupId).orElseThrow();

    assertThat(stored.version()).isEqualTo("0");
    assertThat(Group.parseFrom(stored.groupData())).isEqualTo(group);
    assertThat(Group.parseFrom(stored.groupData())).isNotEqualTo(updated);
  }

  @Test
  void testGetGroup() throws Exception {
    GroupsManager groupsManager = groupsManager();

    GroupSecretParams groupSecretParams = GroupSecretParams.generate();
    GroupPublicParams groupPublicParams = groupSecretParams.getPublicParams();
    ByteString        groupId           = ByteString.copyFrom(groupPublicParams.getGroupIdentifier().serialize());

    Group group = Group.newBuilder()
                       .setVersion(0)
                       .setTitle(ByteString.copyFromUtf8("Some title"))
                       .setAvatarUrl("Some avatar")
                       .setAccessControl(AccessControl.newBuilder()
                                                      .setMembers(AccessControl.AccessRequired.MEMBER)
                                                      .setAttributes(AccessControl.AccessRequired.MEMBER))
                       .setPublicKey(ByteString.copyFrom(groupPublicParams.serialize()))
                       .build();

    CompletableFuture<Boolean> result = groupsManager.createGroup(groupId, group);
    assertTrue(result.get());

    CompletableFuture<Optional<Group>> retrieved = groupsManager.getGroup(groupId);
    assertThat(retrieved.get().isPresent()).isTrue();
    assertThat(retrieved.get().get()).isEqualTo(group);
  }

  @Test
  void testGetGroupNotFound() throws Exception {
    GroupsManager groupsManager = groupsManager();

    GroupSecretParams groupSecretParams = GroupSecretParams.generate();
    GroupPublicParams groupPublicParams = groupSecretParams.getPublicParams();
    ByteString        groupId           = ByteString.copyFrom(groupPublicParams.getGroupIdentifier().serialize());

    Group group = Group.newBuilder()
                       .setVersion(0)
                       .setTitle(ByteString.copyFromUtf8("Some title"))
                       .setAvatarUrl("Some avatar")
                       .setAccessControl(AccessControl.newBuilder()
                                                      .setMembers(AccessControl.AccessRequired.MEMBER)
                                                      .setAttributes(AccessControl.AccessRequired.MEMBER))
                       .setPublicKey(ByteString.copyFrom(groupPublicParams.serialize()))
                       .build();

    CompletableFuture<Boolean> result = groupsManager.createGroup(groupId, group);
    assertTrue(result.get());

    CompletableFuture<Optional<Group>> retrieved = groupsManager.getGroup(ByteString.copyFrom(GroupSecretParams.generate().getPublicParams().getGroupIdentifier().serialize()));
    assertThat(retrieved.get().isPresent()).isFalse();
    assertThat(retrieved.get().isEmpty()).isTrue();
  }


  @Test
  void testReadError() {
    GroupsManager groupsManager = groupsManagerWithFailingReads(new RuntimeException("Bad news"));

    assertThatThrownBy(() -> groupsManager.getGroup(ByteString.copyFrom(new byte[16])).get())
        .isInstanceOf(ExecutionException.class)
        .hasRootCauseMessage("Bad news");
  }

  @Test
  void testAppendLog() throws Exception {
    GroupsManager     groupsManager     = groupsManager();
    GroupSecretParams groupSecretParams = GroupSecretParams.generate();
    GroupPublicParams groupPublicParams = groupSecretParams.getPublicParams();
    ByteString        groupId           = ByteString.copyFrom(groupPublicParams.getGroupIdentifier().serialize());

    Actions actions = Actions.newBuilder()
                             .setModifyTitle(ModifyTitleAction.newBuilder()
                                                              .setTitle(ByteString.copyFromUtf8("Some new title")))
                             .build();

    GroupChange change = GroupChange.newBuilder()
                                    .setActions(actions.toByteString())
                                    .setServerSignature(ByteString.copyFrom(AuthHelper.GROUPS_SERVER_KEY.sign(actions.toByteArray()).serialize()))
                                    .build();

    Group groupState = Group.newBuilder()
                            .setTitle(ByteString.copyFromUtf8("Some new title"))
                            .setAvatarUrl("some avatar")
                            .build();

    CompletableFuture<Boolean> insert = groupsManager.appendChangeRecord(groupId, 1, change, groupState);
    assertTrue(insert.get());

    StoredLogEntry stored = storedLogEntry(groupId, 1).orElseThrow();

    assertThat(stored.version()).isEqualTo("1");
    assertThat(GroupChange.parseFrom(stored.change())).isEqualTo(change);
    assertThat(Group.parseFrom(stored.state())).isEqualTo(groupState);
  }

  @Test
  void testQueryLog() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
    GroupsManager     groupsManager     = groupsManager();
    GroupSecretParams groupSecretParams = GroupSecretParams.generate();
    GroupPublicParams groupPublicParams = groupSecretParams.getPublicParams();
    ByteString        groupId           = ByteString.copyFrom(groupPublicParams.getGroupIdentifier().serialize());

    Group latestGroupState = null;
    for (int i=1;i<2000;i++) {
      Actions actions = Actions.newBuilder()
                               .setModifyTitle(ModifyTitleAction.newBuilder()
                                                                .setTitle(ByteString.copyFromUtf8("Some new title " + i)))
                               .build();

      GroupChange change = GroupChange.newBuilder()
                                      .setActions(actions.toByteString())
                                      .setServerSignature(ByteString.copyFrom(AuthHelper.GROUPS_SERVER_KEY.sign(actions.toByteArray()).serialize()))
                                      .setChangeEpoch(i%10)  // spread some change epoch versions throughout
                                      .build();

      Group groupState = Group.newBuilder()
                              .setTitle(ByteString.copyFromUtf8("Some new title " + i))
                              .setVersion(i)
                              .build();
      latestGroupState = groupState;

      CompletableFuture<Boolean> insert = groupsManager.appendChangeRecord(groupId, i, change, groupState);
      assertTrue(insert.get());
    }

    assertThat(latestGroupState).isNotNull();
    List<GroupChangeState> changes = groupsManager.getChangeRecords(groupId, latestGroupState, null, false, false, 1, 20).get();
    assertThat(changes.size()).isEqualTo(19);

    for (int i=1;i<20;i++) {
      assertThat(Actions.parseFrom(changes.get(i-1).getGroupChange().getActions()).getModifyTitle().getTitle().toStringUtf8()).isEqualTo("Some new title " + i);
      assertThat(changes.get(i-1).getGroupState().getTitle().toStringUtf8()).isEqualTo("Some new title " + i);
    }

    changes = groupsManager.getChangeRecords(groupId, latestGroupState, null, false, false, 10, 200).get();
    assertThat(changes.size()).isEqualTo(190);

    for (int i=10;i<200;i++) {
      assertThat(Actions.parseFrom(changes.get(i-10).getGroupChange().getActions()).getModifyTitle().getTitle().toStringUtf8()).isEqualTo("Some new title " + i);
      assertThat(changes.get(i-10).getGroupState().getTitle().toStringUtf8()).isEqualTo("Some new title " + i);
    }

    changes = groupsManager.getChangeRecords(groupId, latestGroupState, 5, false, false, 1, 20).get();
    assertThat(changes.size()).isEqualTo(19);
    for (int i=1;i<20;i++) {
      GroupChangeState change = changes.get(i - 1);
      assertThat(Actions.parseFrom(change.getGroupChange().getActions()).getModifyTitle().getTitle().toStringUtf8()).isEqualTo("Some new title " + i);
      if (i % 10 > 5) {
        assertThat(change.getGroupState().getTitle().toStringUtf8()).isEqualTo("Some new title " + i);
      } else {
        assertThat(change.hasGroupState()).as("change %d does not have group state set", i).isFalse();
      }
    }

    changes = groupsManager.getChangeRecords(groupId, latestGroupState, 5, true, true, 2, 5).get();
    assertThat(changes.size()).isEqualTo(3);
    for (int i=2;i<5;i++) {
      GroupChangeState change = changes.get(i - 2);
      assertThat(Actions.parseFrom(change.getGroupChange().getActions()).getModifyTitle().getTitle().toStringUtf8()).isEqualTo("Some new title " + i);
      if (i == 3) {
        assertThat(change.hasGroupState()).as("change %d does not have group state set since it is neither first nor last", i).isFalse();
      } else {
        assertThat(change.hasGroupState()).as("change %d has group state set since it is first or last", i).isTrue();
        assertThat(change.getGroupState().getTitle().toStringUtf8()).isEqualTo("Some new title " + i);
      }
    }
  }
}
