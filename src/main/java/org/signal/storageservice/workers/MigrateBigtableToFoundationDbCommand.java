/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.workers;

import com.apple.foundationdb.Database;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import io.dropwizard.core.cli.ConfiguredCommand;
import io.dropwizard.core.setup.Bootstrap;
import java.util.List;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.signal.storageservice.StorageServiceConfiguration;
import org.signal.storageservice.configuration.FoundationDbConfiguration;
import org.signal.storageservice.storage.BigtableClients;
import org.signal.storageservice.storage.foundationdb.FoundationDbStorage;

/// SWARM: `migrate-bigtable-to-foundationdb [--apply] <config.yml>`. Copies groups, group logs, storage manifests and
/// storage items from the Bigtable tables named in the configuration's `bigtable:` block (the emulator, through
/// `BIGTABLE_EMULATOR_HOST`) into the FoundationDB directory named in `storage.foundationdb`, whatever
/// `storage.backend` says. A dry run unless `--apply` is given, and a dry run writes nothing anywhere (it does not
/// even create the FoundationDB directory); idempotent; never deletes from Bigtable. See
/// [BigtableToFoundationDbMigrator] and docs/SWARM-CHANGES.md, section 3.9.
public class MigrateBigtableToFoundationDbCommand extends ConfiguredCommand<StorageServiceConfiguration> {

  public MigrateBigtableToFoundationDbCommand() {
    super("migrate-bigtable-to-foundationdb",
        "Copy the storage service's Bigtable tables into FoundationDB (a dry run unless --apply is given)");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--apply")
        .dest("apply")
        .action(Arguments.storeTrue())
        .setDefault(false)
        .help("copy the records that are missing in FoundationDB; without it nothing is written");
  }

  @Override
  protected void run(final Bootstrap<StorageServiceConfiguration> bootstrap, final Namespace namespace,
      final StorageServiceConfiguration configuration) throws Exception {

    final boolean apply = namespace.getBoolean("apply");

    if (configuration.getBigTableConfiguration() == null) {
      throw new IllegalArgumentException("the configuration has no bigtable block: nothing to migrate from");
    }

    final FoundationDbConfiguration foundationDbConfiguration =
        configuration.getStorageBackendConfiguration().getFoundationDbConfiguration();
    if (foundationDbConfiguration == null) {
      throw new IllegalArgumentException("the configuration has no storage.foundationdb block: nothing to migrate to");
    }

    final List<BigtableToFoundationDbMigrator.TableReport> reports;
    try (final BigtableDataClient source = BigtableClients.create(configuration.getBigTableConfiguration());
        final Database database = FoundationDbStorage.openDatabase(foundationDbConfiguration, false)) {

      // A dry run only reads FoundationDB: it opens the directory read-only and, if it does not exist yet, compares
      // with nothing instead of creating it. Copying creates it, as the service does at start.
      final FoundationDbStorage target = apply
          ? FoundationDbStorage.open(database, foundationDbConfiguration.directory(), false).join()
          : FoundationDbStorage.openIfExists(database, foundationDbConfiguration.directory(), false).join().orElse(null);

      reports = new BigtableToFoundationDbMigrator(source, configuration.getBigTableConfiguration(), target,
          foundationDbConfiguration.directory(), System.out).run(apply);
    }

    final long problems = reports.stream()
        .mapToLong(report -> report.conflicts() + report.unreadable())
        .sum();

    if (problems > 0) {
      throw new IllegalStateException(problems + " row(s) were not copied (conflicts or unreadable rows); see above");
    }
  }
}
