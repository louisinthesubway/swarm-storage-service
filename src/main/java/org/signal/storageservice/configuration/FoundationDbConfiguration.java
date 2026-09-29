/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.configuration;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import java.util.List;
import javax.annotation.Nullable;

/// SWARM: where the FoundationDB backend keeps its data. See docs/SWARM-CHANGES.md, section 3.
///
/// @param clusterFile           path of the FoundationDB cluster file; the service only reads it
/// @param directory             the Directory-layer path the service owns, for example `[swarm-storage-service]`; four
///                              subdirectories are created below it
/// @param transactionTimeout    the time limit for one transaction including all its retries; the default is
///                              [#DEFAULT_TRANSACTION_TIMEOUT]
/// @param transactionRetryLimit the maximum number of retries of one transaction; the default is
///                              [#DEFAULT_TRANSACTION_RETRY_LIMIT]
public record FoundationDbConfiguration(@NotBlank String clusterFile,
                                        @NotEmpty List<@NotBlank String> directory,
                                        @Nullable Duration transactionTimeout,
                                        @Nullable @Positive Integer transactionRetryLimit) {

  public static final Duration DEFAULT_TRANSACTION_TIMEOUT = Duration.ofSeconds(10);
  public static final int DEFAULT_TRANSACTION_RETRY_LIMIT = 100;

  public FoundationDbConfiguration {
    if (transactionTimeout == null) {
      transactionTimeout = DEFAULT_TRANSACTION_TIMEOUT;
    }
    if (transactionRetryLimit == null) {
      transactionRetryLimit = DEFAULT_TRANSACTION_RETRY_LIMIT;
    }
  }
}
