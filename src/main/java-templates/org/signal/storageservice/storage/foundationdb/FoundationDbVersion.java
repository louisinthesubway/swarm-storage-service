/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage.foundationdb;

/// SWARM: the FoundationDB client version and API version, filled in from pom.xml (`foundationdb.version`,
/// `foundationdb.api-version`), the same values swarm-messenger-server pins for the chat server. Mirrors that
/// repository's `FoundationDbVersion`.
public class FoundationDbVersion {

  private static final String VERSION = "${foundationdb.version}";
  private static final int API_VERSION = ${foundationdb.api-version};

  public static String getFoundationDbVersion() {
    return VERSION;
  }

  public static int getFoundationDbApiVersion() {
    return API_VERSION;
  }
}
