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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;

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

  /**
   * The current state of a table w.r.t. the policy set forth by a quota.
   */
  static enum ViolationState {
    IN_VIOLATION,
    IN_OBSERVANCE,
  }

  private final HMaster master;

  public QuotaObserverChore(HMaster master) {
    super(QuotaObserverChore.class.getSimpleName(), master, getPeriod(master.getConfiguration()),
        getInitialDelay(master.getConfiguration()), getTimeUnit(master.getConfiguration()));
    this.master = master;
  }

  @Override
  protected void chore() {
    // Get the total set of tables that have quotas defined
    Set<TableName> tablesWithQuotasDefined = fetchAllTablesWithQuotasDefined();

    // Filter out tables for which we don't have adequate regionspace reports yet.
    Set<TableName> tablesToInspect = filterInsufficientlyReportedTables(tablesWithQuotasDefined);

    // Transition each table to/from quota violation based on the current and target state.
    for (TableName table : tablesToInspect) {
      final Quotas quota = getQuotaForTable(table);
      final ViolationState currentState = getCurrentViolationState(table);
      final ViolationState targetState = getTargetViolationState(table);
      if (currentState == ViolationState.IN_VIOLATION) {
        if (targetState == ViolationState.IN_OBSERVANCE) {
          transitionTableToObservance(table);
        } else if (targetState == ViolationState.IN_VIOLATION) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(table + " remains in violation of quota.");
          }
        }
      } else if (currentState == ViolationState.IN_OBSERVANCE) {
        if (targetState == ViolationState.IN_VIOLATION) {
          transitionTableToViolation(table, getViolationPolicy(quota));
        } else if (targetState == ViolationState.IN_OBSERVANCE) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(table + " is in observance of quota.");
          }
        }
      }
    }
  }

  /**
   * Fetches the Quota for the given table.
   */
  private Quotas getQuotaForTable(TableName table) {
    // TODO
    return null;
  }

  /**
   * Fetches the current {@link ViolationState} for the given table.
   */
  private ViolationState getCurrentViolationState(TableName table) {
    // TODO
    return null;
  }

  /**
   * Computes the target {@link ViolationState} for the given table.
   */
  private ViolationState getTargetViolationState(TableName table) {
    // TODO
    return null;
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
  private SpaceViolationPolicy getViolationPolicy(Quotas quota) {
    // TODO
    return null;
  }

  /**
   * Computes the set of all tables that have quotas defined. This includes tables with quotas
   * explicitly set on them, in addition to tables that exist namespaces which have a quota
   * defined.
   */
  Set<TableName> fetchAllTablesWithQuotasDefined() {
    // TODO
    return null;
  }

  /**
   * Filters out all tables for which the Master currently doesn't have enough region space
   * reports received from RegionServers yet.
   */
  Set<TableName> filterInsufficientlyReportedTables(Set<TableName> tablesWithQuotas) {
    return null;
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
}
