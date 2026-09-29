/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/// SWARM: gives each test a [FoundationDbStorage] in a fresh directory of a real FoundationDB cluster, and removes the
/// directory afterwards.
///
/// The cluster is named by the system property [#CLUSTER_FILE_PROPERTY] (a cluster file, for example
/// `docker:docker@foundationdb0:4500` for a service container). Without it the tests of the class are skipped, unless
/// [#REQUIRED_PROPERTY] is `true` (as in CI), in which case they fail. The FoundationDB client library `libfdb_c` of
/// the version in pom.xml must be installed.
public class FoundationDbExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback {

  public static final String CLUSTER_FILE_PROPERTY = "swarm.foundationdb.clusterFile";
  public static final String REQUIRED_PROPERTY = "swarm.foundationdb.required";

  /// Every test directory lives below this one.
  public static final String TEST_ROOT = "swarm-storage-service-test";

  private static Database database;

  private List<String> directory;
  private FoundationDbStorage storage;

  @Override
  public void beforeAll(final ExtensionContext context) {
    openDatabase();
  }

  /// Opens the shared database, or skips (or fails) the calling test if no cluster is configured.
  public static synchronized Database openDatabase() {
    if (database == null) {
      final String clusterFile = System.getProperty(CLUSTER_FILE_PROPERTY);

      if (clusterFile == null || clusterFile.isBlank()) {
        if (Boolean.getBoolean(REQUIRED_PROPERTY)) {
          throw new IllegalStateException(REQUIRED_PROPERTY + " is true but " + CLUSTER_FILE_PROPERTY + " is not set");
        }
        Assumptions.abort("no FoundationDB cluster: set -D" + CLUSTER_FILE_PROPERTY + "=<cluster file>");
      }

      database = FDB.selectAPIVersion(FoundationDbVersion.getFoundationDbApiVersion()).open(clusterFile);
      database.options().setTransactionTimeout(30_000);
    }

    return database;
  }

  @Override
  public void beforeEach(final ExtensionContext context) {
    directory = List.of(TEST_ROOT, UUID.randomUUID().toString());
    storage = FoundationDbStorage.open(openDatabase(), directory, false).join();
  }

  @Override
  public void afterEach(final ExtensionContext context) {
    if (directory != null) {
      DirectoryLayer.getDefault().removeIfExists(openDatabase(), directory).join();
    }
    directory = null;
    storage = null;
  }

  public FoundationDbStorage getStorage() {
    return storage;
  }

  public List<String> getDirectory() {
    return directory;
  }

  public Database getDatabase() {
    return openDatabase();
  }
}
