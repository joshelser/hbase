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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.quotas.QuotaViolationStore.ViolationState;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceViolation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

/**
 * Reads the currently received Region filesystem-space use reports and acts on those which
 * violate a defined quota.
 */
public class QuotaObserverChore extends ScheduledChore {
  private static final Log LOG = LogFactory.getLog(QuotaObserverChore.class);
  static final String VIOLATION_OBSERVER_CHORE_PERIOD_KEY =
      "hbase.master.quotas.violation.observer.chore.period";
  static final int VIOLATION_OBSERVER_CHORE_PERIOD_DEFAULT = 1000 * 60 * 5; // 5 minutes in millis

  static final String VIOLATION_OBSERVER_CHORE_DELAY_KEY =
      "hbase.master.quotas.violation.observer.chore.delay";
  static final long VIOLATION_OBSERVER_CHORE_DELAY_DEFAULT = 1000L * 60L; // 1 minute

  static final String VIOLATION_OBSERVER_CHORE_TIMEUNIT_KEY =
      "hbase.master.quotas.violation.observer.chore.timeunit";
  static final String VIOLATION_OBSERVER_CHORE_TIMEUNIT_DEFAULT = TimeUnit.MILLISECONDS.name();

  static final String VIOLATION_OBSERVER_CHORE_REPORT_PERCENT_KEY =
      "hbase.master.quotas.violation.observer.report.percent";
  static final double VIOLATION_OBSERVER_CHORE_REPORT_PERCENT_DEFAULT= 0.95;


  private final HMaster master;
  private final MasterQuotaManager quotaManager;
  /*
   * Callback that changes in quota violation are passed to.
   */
  private final SpaceQuotaViolationNotifier violationNotifier;

  /*
   * Preserves the state of quota violations for tables and namespaces
   */
  private final Map<TableName,SpaceQuotaSnapshot> tableQuotaViolationStates;
  private final Map<String,SpaceQuotaSnapshot> namespaceQuotaViolationStates;

  /*
   * Encapsulates logic for moving tables/namespaces into or out of quota violation
   */
  private QuotaViolationStore<TableName> tableViolationStore;
  private QuotaViolationStore<String> namespaceViolationStore;

  public QuotaObserverChore(HMaster master) {
    this(master, master.getSpaceQuotaViolationNotifier());
  }

  QuotaObserverChore(HMaster master, SpaceQuotaViolationNotifier violationNotifier) {
    super(QuotaObserverChore.class.getSimpleName(), master, getPeriod(master.getConfiguration()),
        getInitialDelay(master.getConfiguration()), getTimeUnit(master.getConfiguration()));
    this.master = master;
    this.quotaManager = this.master.getMasterQuotaManager();
    this.violationNotifier = violationNotifier;
    this.tableQuotaViolationStates = new HashMap<>();
    this.namespaceQuotaViolationStates = new HashMap<>();
  }

  @Override
  protected void chore() {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Refreshing space quotas in RegionServer");
      }
      _chore();
    } catch (IOException e) {
      LOG.warn("Failed to process quota reports and update quota violation state. Will retry.", e);
    }
  }

  void _chore() throws IOException {
    // Get the total set of tables that have quotas defined. Includes table quotas
    // and tables included by namespace quotas.
    TablesWithQuotas tablesWithQuotas = fetchAllTablesWithQuotasDefined();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Found following tables with quotas: " + tablesWithQuotas);
    }

    // The current "view" of region space use. Used henceforth.
    final Map<HRegionInfo,Long> reportedRegionSpaceUse = quotaManager.snapshotRegionSizes();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Using " + reportedRegionSpaceUse.size() + " region space use reports");
    }

    // Create the stores to track table and namespace violations
    initializeViolationStores(reportedRegionSpaceUse);

    // Filter out tables for which we don't have adequate regionspace reports yet.
    // Important that we do this after we instantiate the stores above
    tablesWithQuotas.filterInsufficientlyReportedTables(tableViolationStore);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Filtered insufficiently reported tables, left with " +
          reportedRegionSpaceUse.size() + " regions reported");
    }

    // Transition each table to/from quota violation based on the current and target state.
    // Only table quotas are enacted.
    final Set<TableName> tablesWithTableQuotas = tablesWithQuotas.getTableQuotaTables();
    for (TableName table : tablesWithTableQuotas) {
      final SpaceQuota spaceQuota = tableViolationStore.getSpaceQuota(table);
      if (null == spaceQuota) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unexpectedly did not find a space quota for " + table
              + ", maybe it was recently deleted.");
        }
        continue;
      }
      final SpaceQuotaSnapshot currentSnapshot = tableViolationStore.getCurrentState(table);
      final SpaceQuotaSnapshot targetSnapshot = tableViolationStore.getTargetState(table, spaceQuota);
      updateTableQuota(table, currentSnapshot, targetSnapshot);
    }

    // For each Namespace quota, transition each table in the namespace in or out of violation
    // only if a table quota violation policy has not already been applied.
    final Set<String> namespacesWithQuotas = tablesWithQuotas.getNamespacesWithQuotas();
    final Multimap<String,TableName> tablesByNamespace = tablesWithQuotas.getTablesByNamespace();
    for (String namespace : namespacesWithQuotas) {
      // Get the quota definition for the namespace
      final SpaceQuota spaceQuota = namespaceViolationStore.getSpaceQuota(namespace);
      if (null == spaceQuota) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Could not get Namespace space quota for " + namespace
              + ", maybe it was recently deleted.");
        }
        continue;
      }
      final SpaceQuotaSnapshot currentSnapshot = namespaceViolationStore.getCurrentState(namespace);
      final SpaceQuotaSnapshot targetSnapshot = namespaceViolationStore.getTargetState(namespace, spaceQuota);
      updateNamespaceQuota(namespace, currentSnapshot, targetSnapshot, tablesByNamespace);
    }
  }

  /**
   * Updates the hbase:quota table with the new quota policy for this <code>table</code>
   * if necessary.
   *
   * @param table The table being checked
   * @param currentSnapshot The state of the quota on this table from the previous invocation.
   * @param targetSnapshot The state the quota should be in for this table.
   */
  void updateTableQuota(
      TableName table, SpaceQuotaSnapshot currentSnapshot, SpaceQuotaSnapshot targetSnapshot)
          throws IOException {
    final SpaceViolationPolicy currentPolicy = currentSnapshot.getPolicy();
    final SpaceViolationPolicy targetPolicy = targetSnapshot.getPolicy();

    // The policies are different, we're changing something
    if (currentPolicy != targetPolicy) {
      // If the target is none, we're moving out of violation. Update the hbase:quota table
      if (targetPolicy == SpaceViolationPolicy.NONE) {
        LOG.info(table + " moving into observance of table space quota.");
        transitionTableToObservance(table, targetSnapshot);
      } else {
        // We're either moving into violation or changing violation policies
        LOG.info(table + " moving into violation of table space quota with policy of " + targetPolicy);
        transitionTableToViolation(table, targetSnapshot);
      }
      // Update it in memory
      tableViolationStore.setCurrentState(table, targetSnapshot);
    } else if (LOG.isTraceEnabled()) {
      // Policies are the same, so we have nothing to do except log this
      if (currentPolicy == SpaceViolationPolicy.NONE) {
        LOG.trace(table + " remains in observance of quota.");
      } else {
        LOG.trace(table + " remains in violation of quota.");
      }
    }
  }
      
  /**
   * Updates the hbase:quota table with the target quota policy for this <code>namespace</code>
   * if necessary.
   *
   * @param namespace The namespace being checked
   * @param currentSnapshot The state of the quota on this namespace from the previous invocation
   * @param targetSnapshot The state the quota should be in for this namespace
   * @param tablesByNamespace A mapping of tables in namespaces.
   */
  void updateNamespaceQuota(
      String namespace, SpaceQuotaSnapshot currentSnapshot, SpaceQuotaSnapshot targetSnapshot,
      final Multimap<String,TableName> tablesByNamespace) throws IOException {
    final SpaceViolationPolicy currentPolicy = currentSnapshot.getPolicy();
    final SpaceViolationPolicy targetPolicy = targetSnapshot.getPolicy();

    // When the policies differ, we need to move into or out of violatino
    if (currentPolicy != targetPolicy) {
      // We want to have a policy of "NONE", moving out of violation
      if (targetPolicy == SpaceViolationPolicy.NONE) {
        for (TableName tableInNS : tablesByNamespace.get(namespace)) {
          if (SpaceViolationPolicy.NONE != tableViolationStore.getCurrentState(tableInNS).getPolicy()) {
            // Table-level quota violation policy is being applied here.
            if (LOG.isTraceEnabled()) {
              LOG.trace("Not activating Namespace violation policy because a Table violation"
                  + " policy is already in effect for " + tableInNS);
            }
          } else {
            LOG.info(tableInNS + " moving into observance of namespace space quota");
            transitionTableToObservance(tableInNS, targetSnapshot);
          }
        }
      } else {
        // Moving tables in the namespace into violation or to a different violation policy
        for (TableName tableInNS : tablesByNamespace.get(namespace)) {
          if (SpaceViolationPolicy.NONE != tableViolationStore.getCurrentState(tableInNS).getPolicy()) {
            // Table-level quota violation policy is being applied here.
            if (LOG.isTraceEnabled()) {
              LOG.trace("Not activating Namespace violation policy because a Table violation"
                  + " policy is already in effect for " + tableInNS);
            }
          } else {
            LOG.info(tableInNS + " moving into violation of namespace space quota with policy " + targetPolicy);
            transitionTableToViolation(tableInNS, targetSnapshot);
          }
        }
      }
    } else {
      // Policies are the same
      if (targetPolicy == SpaceViolationPolicy.NONE) {
        // Both are NONE, so we remain in observance
        if (LOG.isTraceEnabled()) {
          LOG.trace(namespace + " remains in observance of quota.");
        }
      } else {
        // Namespace quota is still in violation, need to enact if the table quota is not taking priority.
        for (TableName tableInNS : tablesByNamespace.get(namespace)) {
          // Does a table policy exist
          if (SpaceViolationPolicy.NONE != tableViolationStore.getCurrentState(tableInNS).getPolicy()) {
            // Table-level quota violation policy is being applied here.
            if (LOG.isTraceEnabled()) {
              LOG.trace("Not activating Namespace violation policy because Table violation"
                  + " policy is already in effect for " + tableInNS);
            }
          } else {
            // No table policy, so enact namespace policy
            LOG.info(tableInNS + " moving into violation of namespace space quota");
            transitionTableToViolation(tableInNS, targetSnapshot);
          }
        }
      }
    }
  }

  void initializeViolationStores(Map<HRegionInfo,Long> regionSizes) {
    Map<HRegionInfo,Long> immutableRegionSpaceUse = Collections.unmodifiableMap(regionSizes);
    tableViolationStore = new TableQuotaViolationStore(master.getConnection(), this,
        immutableRegionSpaceUse);
    namespaceViolationStore = new NamespaceQuotaViolationStore(master.getConnection(), this,
        immutableRegionSpaceUse);
  }

  /**
   * Computes the set of all tables that have quotas defined. This includes tables with quotas
   * explicitly set on them, in addition to tables that exist namespaces which have a quota
   * defined.
   */
  TablesWithQuotas fetchAllTablesWithQuotasDefined() throws IOException {
    final Scan scan = QuotaTableUtil.makeScan(null);
    final QuotaRetriever scanner = new QuotaRetriever();
    final TablesWithQuotas tablesWithQuotas = new TablesWithQuotas(master.getConnection(),
        master.getConfiguration());
    try {
      scanner.init(master.getConnection(), scan);
      for (QuotaSettings quotaSettings : scanner) {
        // Only one of namespace and tablename should be 'null'
        final String namespace = quotaSettings.getNamespace();
        final TableName tableName = quotaSettings.getTableName();
        if (QuotaType.SPACE != quotaSettings.getQuotaType()) {
          continue;
        }

        if (null != namespace) {
          assert null == quotaSettings.getTableName();
          // Collect all of the tables in the namespace
          TableName[] tablesInNS = master.getConnection().getAdmin()
              .listTableNamesByNamespace(namespace);
          for (TableName tableUnderNs : tablesInNS) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Adding " + tableUnderNs + " under " +  namespace
                  + " as having a namespace quota");
            }
            tablesWithQuotas.addNamespaceQuotaTable(tableUnderNs);
          }
        } else {
          assert null != tableName;
          if (LOG.isTraceEnabled()) {
            LOG.trace("Adding " + tableName + " as having table quota.");
          }
          // namespace is already null, must be a non-null tableName
          tablesWithQuotas.addTableQuotaTable(tableName);
        }
      }
      return tablesWithQuotas;
    } finally {
      if (null != scanner) {
        scanner.close();
      }
    }
  }

  @VisibleForTesting
  QuotaViolationStore<TableName> getTableViolationStore() {
    return tableViolationStore;
  }

  @VisibleForTesting
  QuotaViolationStore<String> getNamespaceViolationStore() {
    return namespaceViolationStore;
  }

  /**
   * Transitions the given table to violation of its quota, enabling the violation policy.
   */
  private void transitionTableToViolation(TableName table, SpaceQuotaSnapshot violation)
      throws IOException {
    this.violationNotifier.transitionTable(table, violation);
  }

  /**
   * Transitions the given table to observance of its quota, disabling the violation policy.
   */
  private void transitionTableToObservance(TableName table, SpaceQuotaSnapshot violation) throws IOException {
    this.violationNotifier.transitionTable(table, violation);
  }

  /**
   * Fetch the {@link ViolationState} for the given table.
   */
  SpaceQuotaSnapshot getTableQuotaViolation(TableName table) {
    // TODO Can one instance of a Chore be executed concurrently?
    SpaceQuotaSnapshot state = this.tableQuotaViolationStates.get(table);
    if (null == state) {
      // No tracked state implies observance.
      return QuotaViolationStore.NO_QUOTA;
    }
    return state;
  }

  /**
   * Stores the quota violation state for the given table.
   */
  void setTableQuotaViolation(TableName table, SpaceQuotaSnapshot snapshot) {
    this.tableQuotaViolationStates.put(table, snapshot);
  }

  /**
   * Fetches the {@link ViolationState} for the given namespace.
   */
  SpaceQuotaSnapshot getNamespaceQuotaViolation(String namespace) {
    // TODO Can one instance of a Chore be executed concurrently?
    SpaceQuotaSnapshot state = this.namespaceQuotaViolationStates.get(namespace);
    if (null == state) {
      // No tracked state implies observance.
      return QuotaViolationStore.NO_QUOTA;
    }
    return state;
  }

  /**
   * Stores the quota violation state for the given namespace.
   */
  void setNamespaceQuotaViolation(String namespace, SpaceQuotaSnapshot snapshot) {
    this.namespaceQuotaViolationStates.put(namespace, snapshot);
  }

  /**
   * Constructs a {@link SpaceViolation} from the violation policy and usage.
   * @throws IllegalArgumentException If the SpaceQuota lacks a ViolationPolicy or softLimit
   */
  SpaceViolation getViolation(SpaceQuota spaceQuota, long use) {
    if (!spaceQuota.hasViolationPolicy()) {
      throw new IllegalArgumentException("SpaceQuota had no associated violation policy: "
          + spaceQuota);
    }
    if (!spaceQuota.hasSoftLimit()) {
      throw new IllegalArgumentException("SpaceQuota had no associated softLimit: " + spaceQuota);
    }
    return SpaceViolation.newBuilder().setLimit(spaceQuota.getSoftLimit())
        .setPolicy(spaceQuota.getViolationPolicy()).setUsage(use)
        .build();
  }

  /**
   * Extracts the period for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore period or the default value.
   */
  static int getPeriod(Configuration conf) {
    return conf.getInt(VIOLATION_OBSERVER_CHORE_PERIOD_KEY,
        VIOLATION_OBSERVER_CHORE_PERIOD_DEFAULT);
  }

  /**
   * Extracts the initial delay for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore initial delay or the default value.
   */
  static long getInitialDelay(Configuration conf) {
    return conf.getLong(VIOLATION_OBSERVER_CHORE_DELAY_KEY,
        VIOLATION_OBSERVER_CHORE_DELAY_DEFAULT);
  }

  /**
   * Extracts the time unit for the chore period and initial delay from the configuration. The
   * configuration value for {@link #VIOLATION_OBSERVER_CHORE_TIMEUNIT_KEY} must correspond to
   * a {@link TimeUnit} value.
   *
   * @param conf The configuration object.
   * @return The configured time unit for the chore period and initial delay or the default value.
   */
  static TimeUnit getTimeUnit(Configuration conf) {
    return TimeUnit.valueOf(conf.get(VIOLATION_OBSERVER_CHORE_TIMEUNIT_KEY,
        VIOLATION_OBSERVER_CHORE_TIMEUNIT_DEFAULT));
  }

  /**
   * Extracts the percent of Regions for a table to have been reported to enable quota violation
   * state change.
   *
   * @param conf The configuration object.
   * @return The percent of regions reported to use.
   */
  static Double getRegionReportPercent(Configuration conf) {
    return conf.getDouble(VIOLATION_OBSERVER_CHORE_REPORT_PERCENT_KEY,
        VIOLATION_OBSERVER_CHORE_REPORT_PERCENT_DEFAULT);
  }

  /**
   * A container which encapsulates the tables which have a table quota and the tables which
   * are contained in a namespace which have a namespace quota.
   */
  static class TablesWithQuotas {
    private final Set<TableName> tablesWithTableQuotas = new HashSet<>();
    private final Set<TableName> tablesWithNamespaceQuotas = new HashSet<>();
    private final Connection conn;
    private final Configuration conf;

    public TablesWithQuotas(Connection conn, Configuration conf) {
      this.conn = Objects.requireNonNull(conn);
      this.conf = Objects.requireNonNull(conf);
    }

    Configuration getConfiguration() {
      return conf;
    }

    /**
     * Adds a table with a table quota.
     */
    public void addTableQuotaTable(TableName tn) {
      tablesWithTableQuotas.add(tn);
    }

    /**
     * Adds a table with a namespace quota.
     */
    public void addNamespaceQuotaTable(TableName tn) {
      tablesWithNamespaceQuotas.add(tn);
    }

    /**
     * Returns true if the given table has a table quota.
     */
    public boolean hasTableQuota(TableName tn) {
      return tablesWithTableQuotas.contains(tn);
    }

    /**
     * Returns true if the table exists in a namespace with a namespace quota.
     */
    public boolean hasNamespaceQuota(TableName tn) {
      return tablesWithNamespaceQuotas.contains(tn);
    }

    /**
     * Returns an unmodifiable view of all tables with table quotas.
     */
    public Set<TableName> getTableQuotaTables() {
      return Collections.unmodifiableSet(tablesWithTableQuotas);
    }

    /**
     * Returns an unmodifiable view of all tables in namespaces that have
     * namespace quotas.
     */
    public Set<TableName> getNamespaceQuotaTables() {
      return Collections.unmodifiableSet(tablesWithNamespaceQuotas);
    }

    public Set<String> getNamespacesWithQuotas() {
      Set<String> namespaces = new HashSet<>();
      for (TableName tn : tablesWithNamespaceQuotas) {
        namespaces.add(tn.getNamespaceAsString());
      }
      return namespaces;
    }

    /**
     * Returns a view of all tables that reside in a namespace with a namespace
     * quota, grouped by the namespace itself.
     */
    public Multimap<String,TableName> getTablesByNamespace() {
      Multimap<String,TableName> tablesByNS = HashMultimap.create();
      for (TableName tn : tablesWithNamespaceQuotas) {
        tablesByNS.put(tn.getNamespaceAsString(), tn);
      }
      return tablesByNS;
    }

    /**
     * Filters out all tables for which the Master currently doesn't have enough region space
     * reports received from RegionServers yet.
     */
    public void filterInsufficientlyReportedTables(QuotaViolationStore<TableName> tableStore)
        throws IOException {
      final double percentRegionsReportedThreshold = getRegionReportPercent(getConfiguration());
      Set<TableName> tablesToRemove = new HashSet<>();
      for (TableName table : Iterables.concat(tablesWithTableQuotas, tablesWithNamespaceQuotas)) {
        // Don't recompute a table we've already computed
        if (tablesToRemove.contains(table)) {
          continue;
        }
        final int numRegionsInTable = getNumRegions(table);
        final int reportedRegionsInQuota = getNumReportedRegions(table, tableStore);
        final double ratioReported = ((double) reportedRegionsInQuota) / numRegionsInTable;
        if (ratioReported < percentRegionsReportedThreshold) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Filtering " + table + " because " + reportedRegionsInQuota  + " of " +
                numRegionsInTable + " regions were reported.");
          }
          tablesToRemove.add(table);
        } else if (LOG.isTraceEnabled()) {
          LOG.trace("Retaining " + table + " because " + reportedRegionsInQuota  + " of " +
              numRegionsInTable + " regions were reported.");
        }
      }
      for (TableName tableToRemove : tablesToRemove) {
        tablesWithTableQuotas.remove(tableToRemove);
        tablesWithNamespaceQuotas.remove(tableToRemove);
      }
    }

    /**
     * Computes the total number of regions in a table.
     */
    int getNumRegions(TableName table) throws IOException {
      return this.conn.getAdmin().getTableRegions(table).size();
    }

    /**
     * Computes the number of regions reported for a table.
     */
    int getNumReportedRegions(TableName table, QuotaViolationStore<TableName> tableStore)
        throws IOException {
      return Iterables.size(tableStore.filterBySubject(table));
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(32);
      sb.append(getClass().getSimpleName())
          .append(": tablesWithTableQuotas=")
          .append(this.tablesWithTableQuotas)
          .append(", tablesWithNamespaceQuotas=")
          .append(this.tablesWithNamespaceQuotas);
      return sb.toString();
    }
  }
}
