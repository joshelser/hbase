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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

/**
 * Reads the currently received Region filesystem-space use reports and acts on those which
 * violate a defined quota.
 */
public class QuotaObserverChore extends ScheduledChore {
  private static final Log LOG = LogFactory.getLog(QuotaObserverChore.class);
  static final String VIOLATION_OBSERVER_CHORE_PERIOD_KEY = "hbase.master.quotas.violation.observer.chore.period";
  static final int VIOLATION_OBSERVER_CHORE_PERIOD_DEFAULT = 1000 * 60 * 5; // 5 minutes in millis

  static final String VIOLATION_OBSERVER_CHORE_DELAY_KEY = "hbase.master.quotas.violation.observer.chore.delay";
  static final long VIOLATION_OBSERVER_CHORE_DELAY_DEFAULT = 1000L * 60L; // 1 minute

  static final String VIOLATION_OBSERVER_CHORE_TIMEUNIT_KEY = "hbase.master.quotas.violation.observer.chore.timeunit";
  static final String VIOLATION_OBSERVER_CHORE_TIMEUNIT_DEFAULT = TimeUnit.MILLISECONDS.name();

  static final String VIOLATION_OBSERVER_CHORE_REPORT_PERCENT_KEY = "hbase.master.quotas.violation.observer.report.percent";
  static final double VIOLATION_OBSERVER_CHORE_REPORT_PERCENT_DEFAULT= 0.95;

  /**
   * The current state of a table w.r.t. the policy set forth by a quota.
   */
  static enum ViolationState {
    IN_VIOLATION,
    IN_OBSERVANCE,
  }

  private final HMaster master;
  private final MasterQuotaManager quotaManager;
  private final Map<TableName,ViolationState> tableQuotaViolationStates;
  private final Map<String,ViolationState> namespaceQuotaViolationStates;

  public QuotaObserverChore(HMaster master) {
    super(QuotaObserverChore.class.getSimpleName(), master, getPeriod(master.getConfiguration()),
        getInitialDelay(master.getConfiguration()), getTimeUnit(master.getConfiguration()));
    this.master = master;
    this.quotaManager = this.master.getMasterQuotaManager();
    this.tableQuotaViolationStates = new HashMap<>();
    this.namespaceQuotaViolationStates = new HashMap<>();
  }

  @Override
  protected void chore() {
    try {
      _chore();
    } catch (IOException e) {
      LOG.warn("Failed to process quota reports and update quota violation state. Will retry.", e);
    }
  }

  void _chore() throws IOException {
    // Get the total set of tables that have quotas defined. Includes table quotas
    // and tables included by namespace quotas.
    TablesWithQuotas tablesWithQuotas = fetchAllTablesWithQuotasDefined();

    // The current "view" of region space use. Used henceforth.
    final Map<HRegionInfo,Long> reportedRegionSpaceUse = quotaManager.snapshotRegionSizes();

    // Filter out tables for which we don't have adequate regionspace reports yet.
    tablesWithQuotas.filterInsufficientlyReportedTables(reportedRegionSpaceUse);

    // Transition each table to/from quota violation based on the current and target state.
    // Only table quotas are enacted.
    final Set<TableName> tablesWithTableQuotas = tablesWithQuotas.getTableQuotaTables();
    final Set<TableName> tablesWithNamespaceQuotas = tablesWithQuotas.getNamespaceQuotaTables();
    for (TableName table : tablesWithTableQuotas) {
      final SpaceQuota spaceQuota = getSpaceQuotaForTable(table);
      if (null == spaceQuota) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unexpectedly did not find a space quota for " + table + ", maybe it was recently deleted.");
        }
        continue;
      }
      final ViolationState currentState = getCurrentTableViolationState(table);
      final ViolationState targetState = getTargetViolationState(table, spaceQuota, reportedRegionSpaceUse);

      if (currentState == ViolationState.IN_VIOLATION) {
        if (targetState == ViolationState.IN_OBSERVANCE) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(table + " moving to observance of quota.");
          }
          transitionTableToObservance(table);
          tableQuotaViolationStates.put(table, ViolationState.IN_OBSERVANCE);
        } else if (targetState == ViolationState.IN_VIOLATION) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(table + " remains in violation of quota.");
          }
          tableQuotaViolationStates.put(table, ViolationState.IN_VIOLATION);
        }
      } else if (currentState == ViolationState.IN_OBSERVANCE) {
        if (targetState == ViolationState.IN_VIOLATION) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(table + " moving to violation of quota.");
          }
          transitionTableToViolation(table, getViolationPolicy(spaceQuota));
          tableQuotaViolationStates.put(table, ViolationState.IN_VIOLATION);
        } else if (targetState == ViolationState.IN_OBSERVANCE) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(table + " remains in observance of quota.");
          }
          tableQuotaViolationStates.put(table, ViolationState.IN_OBSERVANCE);
        }
      }
    }

    // For each Namespace quota, transition each table in the namespace in or out of violation
    // only if a table quota violation policy has not already been applied.
    Map<String,Long> namespaceUtilizations = buildNamespaceSpaceUtilization(tablesWithNamespaceQuotas, reportedRegionSpaceUse);
    Multimap<String,TableName> tablesByNamespace = tablesWithQuotas.getTablesByNamespace();
    for (Entry<String,Long> entry : namespaceUtilizations.entrySet()) {
      final String namespace = entry.getKey();
      final Long namespaceUsage = entry.getValue();
      // Get the quota definition for the namespace
      final SpaceQuota spaceQuota = getSpaceQuotaForNamespace(namespace);
      if (null == spaceQuota) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Could not get Namespace space quota for " + entry.getKey() + ", maybe it was recently deleted.");
        }
        continue;
      }
      // The current state and the target state.
      final ViolationState currentNSViolationState = getCurrentNamespaceViolationState(namespace);
      final ViolationState nsViolationState = namespaceUsage < spaceQuota.getSoftLimit() ?
          ViolationState.IN_OBSERVANCE : ViolationState.IN_VIOLATION;
      // When in observance, check if we need to move to violation.
      if (ViolationState.IN_OBSERVANCE == currentNSViolationState) {
        if (ViolationState.IN_VIOLATION == nsViolationState) {
          for (TableName tableInNamespace : tablesByNamespace.get(namespace)) {
            if (ViolationState.IN_VIOLATION == tableQuotaViolationStates.get(tableInNamespace)) {
              // Table-level quota violation policy is being applied here.
              if (LOG.isTraceEnabled()) {
                LOG.trace("Not activating Namespace violation policy because Table violation policy is already in effect for " + tableInNamespace);
              }
              continue;
            } else {
              transitionTableToViolation(tableInNamespace, getViolationPolicy(spaceQuota));
            }
          }
        } else {
          // still in observance
          if (LOG.isTraceEnabled()) {
            LOG.trace(namespace + " remains in observance of quota.");
          }
        }
      } else if (ViolationState.IN_VIOLATION == currentNSViolationState) {
        // When in violation, check if we need to move to observance.
        if (ViolationState.IN_OBSERVANCE == nsViolationState) {
          for (TableName tableInNamespace : tablesByNamespace.get(namespace)) {
            if (ViolationState.IN_VIOLATION == tableQuotaViolationStates.get(tableInNamespace)) {
              // Table-level quota violation policy is being applied here.
              if (LOG.isTraceEnabled()) {
                LOG.trace("Not activating Namespace violation policy because Table violation policy is already in effect for " + tableInNamespace);
              }
              continue;
            } else {
              transitionTableToObservance(tableInNamespace);
            }
          }
        } else {
          // Remains in violation
          if (LOG.isTraceEnabled()) {
            LOG.trace(namespace + " remains in violation of quota.");
          }
        }
      }
    }
  }

  /**
   * Computes the set of all tables that have quotas defined. This includes tables with quotas
   * explicitly set on them, in addition to tables that exist namespaces which have a quota
   * defined.
   */
  TablesWithQuotas fetchAllTablesWithQuotasDefined() throws IOException {
    final Scan scan = QuotaTableUtil.makeScan(null);
    final QuotaRetriever scanner = new QuotaRetriever();
    final TablesWithQuotas tablesWithQuotas = new TablesWithQuotas(this);
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
          for (TableName tableUnderNs : master.getConnection().getAdmin().listTableNamesByNamespace(namespace)) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Adding " + tableUnderNs + " under " +  namespace + " as having a namespace quota");
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

  /**
   * Fetches the Quota for the given table. May be null.
   */
  SpaceQuota getSpaceQuotaForTable(TableName table) throws IOException {
    Quotas quotas = getQuotaForTable(table);
    if (null != quotas && quotas.hasSpace()) {
      return quotas.getSpace();
    }
    return getSpaceQuotaForNamespace(table.getNamespaceAsString());
  }

  /**
   * Fetches the table quota. Visible for mocking/testing.
   */
  Quotas getQuotaForTable(TableName table) throws IOException {
    return QuotaTableUtil.getTableQuota(master.getConnection(), table);
  }

  /**
   * Fetches the Quota for the given namespace. May be null.
   */
  SpaceQuota getSpaceQuotaForNamespace(String namespace) throws IOException {
    Quotas quotas = getQuotaForNamespace(namespace);
    if (null != quotas && quotas.hasSpace()) {
      return quotas.getSpace();
    }
    return null;
  }

  /**
   * Fetches the namespace quota. Visible for mocking/testing.
   */
  Quotas getQuotaForNamespace(String namespace) throws IOException {
    return QuotaTableUtil.getNamespaceQuota(master.getConnection(), namespace);
  }

  /**
   * Returns the current {@link ViolationState} for the given table.
   */
  private ViolationState getCurrentTableViolationState(TableName table) {
    // TODO Can one instance of a Chore be executed concurrently?
    ViolationState state = this.tableQuotaViolationStates.get(table);
    if (null == state) {
      // No tracked state implies observance.
      return ViolationState.IN_OBSERVANCE;
    }
    return state;
  }

  /**
   * Returns the current state of violation policy for the given namespace's quota
   */
  ViolationState getCurrentNamespaceViolationState(String namespace) {
    // TODO Can one instance of a Chore be executed concurrently?
    ViolationState state = this.namespaceQuotaViolationStates.get(namespace);
    if (null == state) {
      // No tracked state implies observance.
      return ViolationState.IN_OBSERVANCE;
    }
    return state;
  }

  /**
   * Computes the target {@link ViolationState} for the given table.
   */
  ViolationState getTargetViolationState(TableName table, SpaceQuota spaceQuota,
      Map<HRegionInfo,Long> reportedRegionSpaceUse) {
    final long sizeLimitInBytes = spaceQuota.getSoftLimit();
    long sum = 0L;
    for (Entry<HRegionInfo,Long> entry : filterByTable(reportedRegionSpaceUse, table)) {
      sum += entry.getValue();
      if (sum > sizeLimitInBytes) {
        // Short-circuit early
        return ViolationState.IN_VIOLATION;
      }
    }
    // Observance is defined as the size of the table being less than the limit
    return sum <= sizeLimitInBytes ? ViolationState.IN_OBSERVANCE : ViolationState.IN_VIOLATION;
  }

  /**
   * Filters out all regions which do not belong to the given table.
   */
  Iterable<Entry<HRegionInfo,Long>> filterByTable(Map<HRegionInfo,Long> regions, final TableName table) {
    return Iterables.filter(Objects.requireNonNull(regions).entrySet(), new Predicate<Entry<HRegionInfo,Long>>() {
      @Override
      public boolean apply(Entry<HRegionInfo,Long> input) {
        return table.equals(input.getKey().getTable());
      }
    });
  }

  /**
   * Computes the number of regions that belong to the given table.
   */
  int numRegionsForTable(Map<HRegionInfo,Long> regions, final TableName table) {
    return Iterables.size(filterByTable(regions, table));
  }

  /**
   * Transitions the given table to violation of its quota, enabling the violation policy.
   */
  private void transitionTableToViolation(TableName table, SpaceViolationPolicy violationPolicy) {
    // TODO
  }

  /**
   * Transitions the given table to observance of its quota, disabling the violation policy.
   */
  private void transitionTableToObservance(TableName table) {
    // TODO
  }

  /**
   * Extracts the {@link SpaceViolationPolicy} from the serialized {@link Quotas} protobuf.
   */
  private SpaceViolationPolicy getViolationPolicy(SpaceQuota spaceQuota) {
    // TODO
    return null;
  }

  /**
   * Computes the utilization of a namespace by summing the utilization of all tables in that namespace.
   */
  Map<String,Long> buildNamespaceSpaceUtilization(Set<TableName> tablesWithNamespaceQuotas,
      Map<HRegionInfo,Long> reportedRegionSpaceUse) {
    Map<String,Long> namespaceUsage = new HashMap<>();
    for (TableName tableWithNamespaceQuota : tablesWithNamespaceQuotas) {
      long currentSum = 0L;
      for (Entry<HRegionInfo,Long> entry : filterByTable(reportedRegionSpaceUse, tableWithNamespaceQuota)) {
        currentSum += entry.getValue();
      }
      Long previousSum = namespaceUsage.get(tableWithNamespaceQuota.getNamespaceAsString());
      if (null == previousSum) {
        previousSum = 0L;
      }
      previousSum += currentSum;
      namespaceUsage.put(tableWithNamespaceQuota.getNamespaceAsString(), previousSum);
    }
    return namespaceUsage;
  }

  /**
   * Extracts the period for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore period or the default value.
   */
  static int getPeriod(Configuration conf) {
    return conf.getInt(VIOLATION_OBSERVER_CHORE_PERIOD_KEY, VIOLATION_OBSERVER_CHORE_PERIOD_DEFAULT);
  }

  /**
   * Extracts the initial delay for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore initial delay or the default value.
   */
  static long getInitialDelay(Configuration conf) {
    return conf.getLong(VIOLATION_OBSERVER_CHORE_DELAY_KEY, VIOLATION_OBSERVER_CHORE_DELAY_DEFAULT);
  }

  /**
   * Extracts the time unit for the chore period and initial delay from the configuration. The configuration
   * value for {@link #VIOLATION_OBSERVER_CHORE_TIMEUNIT_KEY} must correspond to a {@link TimeUnit} value.
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
    private final QuotaObserverChore chore;

    public TablesWithQuotas(QuotaObserverChore chore) {
      this.chore = chore;
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
    public void filterInsufficientlyReportedTables(Map<HRegionInfo,Long> regionReports) throws IOException {
      final double percentRegionsReportedThreshold = getRegionReportPercent(chore.master.getConfiguration());
      Set<TableName> tablesToRemove = new HashSet<>();
      for (TableName table : Iterables.concat(tablesWithTableQuotas, tablesWithNamespaceQuotas)) {
        // Don't recompute a table we've already computed
        if (tablesToRemove.contains(table)) {
          continue;
        }
        final int numRegionsInTable = getNumRegions(table);
        final int reportedRegionsInQuota = chore.numRegionsForTable(regionReports, table);
        final double ratioReported = ((double) reportedRegionsInQuota) / numRegionsInTable;
        if (ratioReported < percentRegionsReportedThreshold) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Filtering " + table + " because " + reportedRegionsInQuota  + " of " +
                numRegionsInTable + " were reported.");
          }
          tablesToRemove.add(table);
        } else if (LOG.isTraceEnabled()) {
          LOG.trace("Retaining " + table + " because " + reportedRegionsInQuota  + " of " +
              numRegionsInTable + " were reported.");
        }
      }
      for (TableName tableToRemove : tablesToRemove) {
        tablesWithTableQuotas.remove(tableToRemove);
        tablesWithNamespaceQuotas.remove(tableToRemove);
      }
    }

    /**
     * Computes the number of regions in a table.
     */
    int getNumRegions(TableName table) throws IOException {
      return chore.master.getConnection().getAdmin().getTableRegions(table).size();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(32);
      sb.append(getClass().getSimpleName()).append(": tablesWithTableQuotas=").append(this.tablesWithTableQuotas);
      sb.append(", tablesWithNamespaceQuotas=").append(this.tablesWithNamespaceQuotas);
      return sb.toString();
    }
  }
}
