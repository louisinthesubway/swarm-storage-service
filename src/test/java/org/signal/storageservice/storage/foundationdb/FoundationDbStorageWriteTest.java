/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage.foundationdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/// SWARM: the retry loop of [FoundationDbStorage#write] with a scripted transaction: which errors are retried, and
/// when the body is told that an earlier attempt may have committed. Uses the FoundationDB client library for the
/// error predicates, so it runs where the other FoundationDB tests run (see [FoundationDbExtension]).
class FoundationDbStorageWriteTest {

  @RegisterExtension
  static final FoundationDbExtension FOUNDATION_DB = new FoundationDbExtension();

  private static final FDBException COMMIT_UNKNOWN_RESULT = new FDBException("commit_unknown_result", 1021);
  private static final FDBException NOT_COMMITTED = new FDBException("not_committed", 1020);
  private static final FDBException TRANSACTION_TOO_LARGE = new FDBException("transaction_too_large", 2101);

  private Database database;
  private Transaction transaction;
  private FoundationDbStorage storage;

  @BeforeEach
  void setUp() {
    database = mock(Database.class);
    transaction = mock(Transaction.class);
    when(database.createTransaction()).thenReturn(transaction);

    storage = new FoundationDbStorage(database, List.of("unused"), new Subspace(Tuple.from("g")),
        new Subspace(Tuple.from("l")), new Subspace(Tuple.from("m")), new Subspace(Tuple.from("i")), false);
  }

  @SafeVarargs
  private void commitOutcomes(final CompletableFuture<Void>... outcomes) {
    var stubbing = when(transaction.commit());
    for (final CompletableFuture<Void> outcome : outcomes) {
      stubbing = stubbing.thenReturn(outcome);
    }
  }

  @Test
  void aMaybeCommittedErrorIsReportedToTheRetry() {
    commitOutcomes(CompletableFuture.failedFuture(COMMIT_UNKNOWN_RESULT), CompletableFuture.completedFuture(null));
    when(transaction.onError(any())).thenReturn(CompletableFuture.completedFuture(transaction));

    final List<Boolean> flags = new CopyOnWriteArrayList<>();
    final String result = storage.write((tr, mayHaveCommitted) -> {
      flags.add(mayHaveCommitted);
      return CompletableFuture.completedFuture("attempt " + flags.size());
    }).join();

    assertThat(result).isEqualTo("attempt 2");
    assertThat(flags).containsExactly(false, true);
    verify(transaction, atLeastOnce()).close();
  }

  @Test
  void aConflictIsRetriedWithoutTheFlag() {
    commitOutcomes(CompletableFuture.failedFuture(NOT_COMMITTED), CompletableFuture.completedFuture(null));
    when(transaction.onError(any())).thenReturn(CompletableFuture.completedFuture(transaction));

    final List<Boolean> flags = new CopyOnWriteArrayList<>();
    storage.write((tr, mayHaveCommitted) -> {
      flags.add(mayHaveCommitted);
      return CompletableFuture.completedFuture(null);
    }).join();

    assertThat(flags).containsExactly(false, false);
  }

  @Test
  void theFlagStaysSetForLaterRetries() {
    commitOutcomes(CompletableFuture.failedFuture(COMMIT_UNKNOWN_RESULT), CompletableFuture.failedFuture(NOT_COMMITTED),
        CompletableFuture.completedFuture(null));
    when(transaction.onError(any())).thenReturn(CompletableFuture.completedFuture(transaction));

    final List<Boolean> flags = new CopyOnWriteArrayList<>();
    storage.write((tr, mayHaveCommitted) -> {
      flags.add(mayHaveCommitted);
      return CompletableFuture.completedFuture(null);
    }).join();

    assertThat(flags).containsExactly(false, true, true);
  }

  @Test
  void anErrorOnErrorRefusesToRetryFailsTheWrite() {
    commitOutcomes(CompletableFuture.failedFuture(TRANSACTION_TOO_LARGE));
    // onError answers a non-retryable error by failing with it
    when(transaction.onError(any())).thenReturn(CompletableFuture.failedFuture(TRANSACTION_TOO_LARGE));

    assertThatThrownBy(() -> storage.write((tr, mayHaveCommitted) -> CompletableFuture.completedFuture(null)).join())
        .hasRootCauseMessage("transaction_too_large");
    verify(transaction, atLeastOnce()).close();
  }

  @Test
  void aFailingBodyIsNotCommitted() {
    when(transaction.onError(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("bad data")));

    assertThatThrownBy(() -> storage.write((tr, mayHaveCommitted) -> {
      throw new IllegalStateException("bad data");
    }).join()).hasRootCauseMessage("bad data");
    verify(transaction, never()).commit();
  }
}
