/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.controllers;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorage;

/// SWARM: `/_ready` for the FoundationDB backend, with the contract of upstream's [ReadinessController]: the first
/// `clientWarmups` calls read one key-value from each of the four subdirectories (upstream: one row from each
/// Bigtable table), later calls answer `ready` straight away, and a failing read fails the call. Registered instead
/// of [ReadinessController] when `storage.backend` is `foundationdb`. See docs/SWARM-CHANGES.md, section 3.7.
@Path("_ready")
public class FoundationDbReadinessController {

  private final FoundationDbStorage storage;
  private final AtomicInteger clientWarmups;

  public FoundationDbReadinessController(final FoundationDbStorage storage, final int clientWarmups) {
    this.storage = storage;
    this.clientWarmups = new AtomicInteger(clientWarmups);
  }

  @GET
  public String isReady() {
    if (clientWarmups.getAndDecrement() > 0) {
      storage.warmUp().join();
    }

    return "ready";
  }
}
