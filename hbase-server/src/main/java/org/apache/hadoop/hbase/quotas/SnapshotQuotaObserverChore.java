/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MetricsMaster;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * A Master-invoked {@code Chore} that computes the size of each snapshot which was created from
 * a table which has a space quota.
 */
@InterfaceAudience.Private
public class SnapshotQuotaObserverChore extends ScheduledChore {
  private static final Log LOG = LogFactory.getLog(SnapshotQuotaObserverChore.class);
  static final String SNAPSHOT_QUOTA_CHORE_PERIOD_KEY =
      "hbase.master.quotas.snapshot.chore.period";
  static final int SNAPSHOT_QUOTA_CHORE_PERIOD_DEFAULT = 1000 * 60 * 5; // 5 minutes in millis

  static final String SNAPSHOT_QUOTA_CHORE_DELAY_KEY =
      "hbase.master.quotas.snapshot.chore.delay";
  static final long SNAPSHOT_QUOTA_CHORE_DELAY_DEFAULT = 1000L * 60L; // 1 minute in millis

  static final String SNAPSHOT_QUOTA_CHORE_TIMEUNIT_KEY =
      "hbase.master.quotas.snapshot.chore.timeunit";
  static final String SNAPSHOT_QUOTA_CHORE_TIMEUNIT_DEFAULT = TimeUnit.MILLISECONDS.name();

  private final Connection conn;
  private final Configuration conf;
  private final MetricsMaster metrics;
  private final FileSystem fs;

  public SnapshotQuotaObserverChore(HMaster master, MetricsMaster metrics) {
    this(
        master.getConnection(), master.getConfiguration(), master.getFileSystem(), master, metrics);
  }

  SnapshotQuotaObserverChore(
      Connection conn, Configuration conf, FileSystem fs, Stoppable stopper,
      MetricsMaster metrics) {
    super(
        QuotaObserverChore.class.getSimpleName(), stopper, getPeriod(conf),
        getInitialDelay(conf), getTimeUnit(conf));
    this.conn = conn;
    this.conf = conf;
    this.metrics = metrics;
    this.fs = fs;
  }

  @Override
  protected void chore() {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Computing sizes of snapshots for quota management.");
      }
      long start = System.nanoTime();
      _chore();
      if (null != metrics) {
        metrics.incrementSnapshotObserverTime((System.nanoTime() - start) / 1_000_000);
      }
    } catch (IOException e) {
      LOG.warn("Failed to compute the size of snapshots, will retry", e);
    }
  }

  void _chore() throws IOException {
    // Gets all tables with quotas that also have snapshots.
    // This values are all of the snapshots that we need to compute the size of.
    long start = System.nanoTime();
    Multimap<TableName,String> snapshotsToComputeSize = getSnapshotsToComputeSize();
    if (null != metrics) {
      metrics.incrementSnapshotFetchTime((System.nanoTime() - start) / 1_000_000);
    }

    // For each table, compute the size of each snapshot
    Map<String,Long> namespaceSnapshotSizes = computeSnapshotSizes(snapshotsToComputeSize);

    // Write the size data by namespaces to the quota table.
    // We need to do this "globally" since each FileArchiverNotifier is limited to its own Table.
    persistSnapshotSizesForNamespaces(namespaceSnapshotSizes);
  }

  /**
   * Fetches each table with a quota (table or namespace quota), and then fetch the name of each
   * snapshot which was created from that table.
   *
   * @return A mapping of table to snapshots created from that table
   */
  Multimap<TableName,String> getSnapshotsToComputeSize() throws IOException {
    Set<TableName> tablesToFetchSnapshotsFrom = new HashSet<>();
    QuotaFilter filter = new QuotaFilter();
    filter.addTypeFilter(QuotaType.SPACE);
    // Pull all of the tables that have quotas (direct, or from namespace)
    for (QuotaSettings qs : QuotaRetriever.open(conf, filter)) {
      String ns = qs.getNamespace();
      TableName tn = qs.getTableName();
      if ((null == ns && null == tn) || (null != ns && null != tn)) {
        throw new IllegalStateException("Expected one of namespace and tablename to be null");
      }
      // Collect either the table name itself, or all of the tables in the namespace
      if (null != ns) {
        try (Admin admin = conn.getAdmin()) {
          tablesToFetchSnapshotsFrom.addAll(Arrays.asList(admin.listTableNamesByNamespace(ns)));
        }
      } else {
        tablesToFetchSnapshotsFrom.add(tn);
      }
    }
    // Fetch all snapshots that were created from these tables
    return getSnapshotsFromTables(tablesToFetchSnapshotsFrom);
  }

  /**
   * Computes a mapping of originating {@code TableName} to snapshots, when the {@code TableName}
   * exists in the provided {@code Set}.
   */
  Multimap<TableName,String> getSnapshotsFromTables(
      Set<TableName> tablesToFetchSnapshotsFrom) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      Multimap<TableName,String> snapshotsToCompute = HashMultimap.create();
      for (org.apache.hadoop.hbase.client.SnapshotDescription sd : admin.listSnapshots()) {
        TableName tn = sd.getTableName();
        if (tablesToFetchSnapshotsFrom.contains(tn)) {
          snapshotsToCompute.put(tn, sd.getName());
        }
      }
      return snapshotsToCompute;
    }
  }

  /**
   * Computes the size of each snapshot provided given the current files referenced by the table.
   * The size of each individual snapshot is recorded to the quota table as a part of this method.
   *
   * @param snapshotsToComputeSize The snapshots to compute the size of
   * @return A mapping of snapshot sizes for each namespace
   */
  Map<String,Long> computeSnapshotSizes(
      Multimap<TableName,String> snapshotsToComputeSize) throws IOException {
    final Map<String,Long> snapshotSizesByNamespace = new HashMap<>();
    final long start = System.nanoTime();
    for (Entry<TableName,Collection<String>> entry : snapshotsToComputeSize.asMap().entrySet()) {
      final TableName tn = entry.getKey();
      final Collection<String> snapshotNames = entry.getValue();

      // Get our notifier instance, this is tracking archivals that happen out-of-band of this chore
      FileArchiverNotifier notifier = FileArchiverNotifierFactoryImpl.getInstance().get(
          conn, conf, fs, tn);

      // The total size consumed by all snapshots against this table
      long totalSnapshotSize = notifier.computeAndStoreSnapshotSizes(snapshotNames);
      // Bucket that size into the appropriate namespace
      snapshotSizesByNamespace.merge(tn.getNamespaceAsString(), totalSnapshotSize, Long::sum);
    }

    // Update the amount of time it took to compute the size of the snapshots for a table
    if (null != metrics) {
      metrics.incrementSnapshotSizeComputationTime((System.nanoTime() - start) / 1_000_000);
    }

    return snapshotSizesByNamespace;
  }

  /**
   * Writes the size used by snapshots for each namespace to the quota table.
   */
  void persistSnapshotSizesForNamespaces(
      Map<String,Long> snapshotSizesByNamespace) throws IOException {
    try (Table quotaTable = conn.getTable(QuotaUtil.QUOTA_TABLE_NAME)) {
      quotaTable.put(snapshotSizesByNamespace.entrySet().stream()
          .map(e -> QuotaTableUtil.createPutNamespaceSnapshotSize(e.getKey(), e.getValue()))
          .collect(Collectors.toList()));
    }
  }

  /**
   * Extracts the period for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore period or the default value.
   */
  static int getPeriod(Configuration conf) {
    return conf.getInt(SNAPSHOT_QUOTA_CHORE_PERIOD_KEY,
        SNAPSHOT_QUOTA_CHORE_PERIOD_DEFAULT);
  }

  /**
   * Extracts the initial delay for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore initial delay or the default value.
   */
  static long getInitialDelay(Configuration conf) {
    return conf.getLong(SNAPSHOT_QUOTA_CHORE_DELAY_KEY,
        SNAPSHOT_QUOTA_CHORE_DELAY_DEFAULT);
  }

  /**
   * Extracts the time unit for the chore period and initial delay from the configuration. The
   * configuration value for {@link #SNAPSHOT_QUOTA_CHORE_TIMEUNIT_KEY} must correspond to
   * a {@link TimeUnit} value.
   *
   * @param conf The configuration object.
   * @return The configured time unit for the chore period and initial delay or the default value.
   */
  static TimeUnit getTimeUnit(Configuration conf) {
    return TimeUnit.valueOf(conf.get(SNAPSHOT_QUOTA_CHORE_TIMEUNIT_KEY,
        SNAPSHOT_QUOTA_CHORE_TIMEUNIT_DEFAULT));
  }
}
