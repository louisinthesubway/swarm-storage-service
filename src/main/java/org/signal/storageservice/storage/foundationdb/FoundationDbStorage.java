/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import io.dropwizard.lifecycle.Managed;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.signal.storageservice.configuration.FoundationDbConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// SWARM: the FoundationDB database and the four subdirectories the storage service keeps its data in, plus the
/// transaction loops the four FoundationDB tables share. See docs/SWARM-CHANGES.md, section 3.
///
/// The directory named in the configuration (for example `[swarm-storage-service]`) gets one subdirectory per
/// Bigtable table: [#GROUPS], [#GROUP_LOGS], [#STORAGE_MANIFESTS] and [#STORAGE_ITEMS].
public class FoundationDbStorage implements Managed {

  public static final String GROUPS = "groups";
  public static final String GROUP_LOGS = "group-logs";
  public static final String STORAGE_MANIFESTS = "storage-manifests";
  public static final String STORAGE_ITEMS = "storage-items";

  public static final List<String> SUBDIRECTORIES = List.of(GROUPS, GROUP_LOGS, STORAGE_MANIFESTS, STORAGE_ITEMS);

  private static final Logger log = LoggerFactory.getLogger(FoundationDbStorage.class);

  private final Database database;
  private final List<String> directory;
  private final Subspace groups;
  private final Subspace groupLogs;
  private final Subspace storageManifests;
  private final Subspace storageItems;
  private final boolean ownsDatabase;

  public FoundationDbStorage(final Database database, final List<String> directory, final Subspace groups,
      final Subspace groupLogs, final Subspace storageManifests, final Subspace storageItems,
      final boolean ownsDatabase) {

    this.database = database;
    this.directory = List.copyOf(directory);
    this.groups = groups;
    this.groupLogs = groupLogs;
    this.storageManifests = storageManifests;
    this.storageItems = storageItems;
    this.ownsDatabase = ownsDatabase;
  }

  /// Opens the cluster named by `configuration.clusterFile()` and creates or opens the configured directory.
  ///
  /// @param disableShutdownHook whether to turn off the FoundationDB client's JVM shutdown hook. The service does,
  ///                            like the chat server: the hook would stop the client while Jetty still serves requests.
  public static FoundationDbStorage open(final FoundationDbConfiguration configuration,
      final boolean disableShutdownHook) {

    final FDB fdb = FDB.selectAPIVersion(FoundationDbVersion.getFoundationDbApiVersion());
    if (disableShutdownHook) {
      fdb.disableShutdownHook();
    }

    final Database database = fdb.open(configuration.clusterFile());
    database.options().setTransactionTimeout(configuration.transactionTimeout().toMillis());
    database.options().setTransactionRetryLimit(configuration.transactionRetryLimit());

    try {
      final FoundationDbStorage storage = open(database, configuration.directory(), true).join();
      log.info("FoundationDB storage backend: cluster file {}, directory {}, client {} (API {})",
          configuration.clusterFile(), configuration.directory(), FoundationDbVersion.getFoundationDbVersion(),
          FoundationDbVersion.getFoundationDbApiVersion());
      return storage;
    } catch (final RuntimeException e) {
      database.close();
      throw e;
    }
  }

  /// Creates (if needed) and opens `directory` and its four subdirectories in one transaction.
  ///
  /// @param ownsDatabase whether [#stop()] should close `database`
  public static CompletableFuture<FoundationDbStorage> open(final Database database, final List<String> directory,
      final boolean ownsDatabase) {

    return database.runAsync(transaction -> {
          final List<DirectorySubspace> subdirectories = new ArrayList<>();
          CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);

          // One after the other: directory operations in one transaction must not run concurrently.
          for (final String subdirectory : SUBDIRECTORIES) {
            final List<String> path = Stream.concat(directory.stream(), Stream.of(subdirectory)).toList();
            chain = chain.thenCompose(ignored -> DirectoryLayer.getDefault().createOrOpen(transaction, path))
                .thenAccept(subdirectories::add);
          }

          return chain.thenApply(ignored -> List.copyOf(subdirectories));
        })
        .thenApply(subdirectories -> new FoundationDbStorage(database, directory,
            subdirectories.get(0), subdirectories.get(1), subdirectories.get(2), subdirectories.get(3),
            ownsDatabase));
  }

  public Database getDatabase() {
    return database;
  }

  public List<String> getDirectory() {
    return directory;
  }

  public Subspace getGroups() {
    return groups;
  }

  public Subspace getGroupLogs() {
    return groupLogs;
  }

  public Subspace getStorageManifests() {
    return storageManifests;
  }

  public Subspace getStorageItems() {
    return storageItems;
  }

  /// Runs `body` in a read-only transaction; retryable errors run it again (FoundationDB's own retry loop).
  public <T> CompletableFuture<T> read(final Function<? super ReadTransaction, CompletableFuture<T>> body) {
    return database.readAsync(body);
  }

  /// Runs `body` in a transaction and commits it, retrying on retryable errors the way [Database#runAsync] does, with
  /// one addition: the `Boolean` passed to `body` is `true` when an earlier attempt of this same call failed with an
  /// error after which the commit may nevertheless have landed (`commit_unknown_result`). A conditional write uses it
  /// to recognise its own earlier write instead of reporting it as someone else's (docs/SWARM-CHANGES.md, 3.5). On a
  /// first attempt it is always `false`.
  public <T> CompletableFuture<T> write(final BiFunction<Transaction, Boolean, CompletableFuture<T>> body) {
    final AtomicReference<Transaction> transaction = new AtomicReference<>(database.createTransaction());
    final AtomicBoolean mayHaveCommitted = new AtomicBoolean(false);
    final AtomicReference<T> result = new AtomicReference<>();

    return AsyncUtil.whileTrue(() -> {
          final Transaction attempt = transaction.get();

          return AsyncUtil.applySafely(tr -> body.apply(tr, mayHaveCommitted.get()), attempt)
              .thenCompose(value -> attempt.commit().thenApply(ignored -> {
                result.set(value);
                return false;
              }))
              .handle((done, error) -> {
                if (error == null) {
                  return CompletableFuture.completedFuture(false);
                }

                final Throwable cause = unwrap(error);
                if (cause instanceof FDBException fdbException && fdbException.isMaybeCommitted()) {
                  mayHaveCommitted.set(true);
                }

                // onError waits out the backoff and hands back a reset transaction, or fails with `cause` itself if
                // the error is not retryable or the retry limit or the timeout has been reached.
                return attempt.onError(cause).thenApply(next -> {
                  transaction.set(next);
                  return true;
                });
              })
              .thenCompose(Function.identity());
        })
        .thenApply(ignored -> result.get())
        .whenComplete((ignored, error) -> transaction.get().close());
  }

  /// Reads one key-value from each of the four subdirectories, like upstream's readiness warm-up reads one row from
  /// each Bigtable table.
  public CompletableFuture<Void> warmUp() {
    return read(transaction -> CompletableFuture.allOf(Stream.of(groups, groupLogs, storageManifests, storageItems)
        .map(subspace -> transaction.getRange(subspace.range(), 1).asList())
        .toArray(CompletableFuture[]::new)));
  }

  static Throwable unwrap(Throwable throwable) {
    while ((throwable instanceof CompletionException || throwable instanceof ExecutionException)
        && throwable.getCause() != null) {
      throwable = throwable.getCause();
    }
    return throwable;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
    if (ownsDatabase) {
      database.close();
    }
  }
}
