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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceViolation;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.annotations.VisibleForTesting;

/**
 * A manager for filesystem space quotas in the RegionServer.
 *
 * This class is responsible for reading enacted quota violation policies from the quota
 * table and then enacting them on the given table.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionServerSpaceQuotaManager {
  private static final Log LOG = LogFactory.getLog(RegionServerSpaceQuotaManager.class);

  private final RegionServerServices rsServices;

  private SpaceQuotaViolationPolicyRefresherChore spaceQuotaRefresher;
  private ConcurrentHashMap<TableName,SpaceViolationPolicyEnforcement> enforcedPolicies;
  private SpaceViolationPolicyEnforcementFactory factory;

  public RegionServerSpaceQuotaManager(RegionServerServices rsServices) {
    this(rsServices, SpaceViolationPolicyEnforcementFactory.getInstance());
  }

  @VisibleForTesting
  RegionServerSpaceQuotaManager(
      RegionServerServices rsServices, SpaceViolationPolicyEnforcementFactory factory) {
    this.rsServices = Objects.requireNonNull(rsServices);
    this.factory = factory;
    this.enforcedPolicies = new ConcurrentHashMap<>();
  }

  public synchronized void start() throws IOException {
    if (!QuotaUtil.isQuotaEnabled(rsServices.getConfiguration())) {
      LOG.info("Quota support disabled");
      return;
    }
    if (null != spaceQuotaRefresher) {
      LOG.warn("SpaceQuotaViolationPolicyRefresherChore has already been started!");
      return;
    }
    this.spaceQuotaRefresher = new SpaceQuotaViolationPolicyRefresherChore(this);
    rsServices.getChoreService().scheduleChore(spaceQuotaRefresher);
  }

  public synchronized void stop() {
    if (null != spaceQuotaRefresher) {
      spaceQuotaRefresher.cancel();
      spaceQuotaRefresher = null;
    }
  }

  /**
   * Creates an object well-suited for the RegionServer to use in verifying active policies.
   */
  public ActivePolicyEnforcement getActiveEnforcements() {
    return new ActivePolicyEnforcement(copyActiveEnforcements());
  }

  /**
   * Converts a map of table to {@link SpaceViolationPolicyEnforcement}s into
   * {@link SpaceViolationPolicy}s.
   */
  public Map<TableName, SpaceViolationPolicy> getActivePoliciesAsMap() {
    final Map<TableName, SpaceViolationPolicyEnforcement> enforcements =
        copyActiveEnforcements();
    final Map<TableName, SpaceViolationPolicy> policies = new HashMap<>();
    for (Entry<TableName, SpaceViolationPolicyEnforcement> entry : enforcements.entrySet()) {
      final SpaceViolationPolicy policy = entry.getValue().getPolicy();
      if (null != policy) {
        policies.put(entry.getKey(), policy);
      }
    }
    return policies;
  }

  /**
   * Reads all quota violation policies which are to be enforced from the quota table.
   *
   * @return The collection of tables which are in violation of their quota and the policy which
   *    should be enforced.
   */
  public Map<TableName, SpaceViolation> getViolationsToEnforce() throws IOException {
    try (Table quotaTable = getConnection().getTable(QuotaUtil.QUOTA_TABLE_NAME);
        ResultScanner scanner = quotaTable.getScanner(QuotaTableUtil.makeQuotaViolationScan())) {
      Map<TableName,SpaceViolation> activeViolations = new HashMap<>();
      for (Result result : scanner) {
        try {
          extractViolationPolicy(result, activeViolations);
        } catch (IllegalArgumentException e) {
          final String msg = "Failed to parse result for row " + Bytes.toString(result.getRow());
          LOG.error(msg, e);
          throw new IOException(msg, e);
        }
      }
      return activeViolations;
    }
  }

  /**
   * Enforces the given violationPolicy on the given table in this RegionServer.
   */
  public void enforceViolationPolicy(TableName tableName, SpaceViolationPolicy violationPolicy) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Enabling violation policy enforcement on " + tableName
          + " with policy " + violationPolicy);
    }
    // Construct this outside of the lock
    final SpaceViolationPolicyEnforcement enforcement = getFactory().create(
        getRegionServerServices(), tableName, violationPolicy);
    // "Enables" the policy
    // TODO Should this synchronize on the actual table name instead of the map? That would allow
    // policy enable/disable on different tables to happen concurrently. As written now, only one
    // table will be allowed to transition at a time.
    synchronized (enforcedPolicies) {
      try {
        enforcement.enable();
      } catch (IOException e) {
        LOG.error("Failed to enable space violation policy for " + tableName
            + ". This table will not enter violation.", e);
        return;
      }
      enforcedPolicies.put(tableName, enforcement);
    }
  }

  /**
   * Disables enforcement on any violation policy on the given <code>tableName</code>.
   */
  public void disableViolationPolicyEnforcement(TableName tableName) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Disabling violation policy enforcement on " + tableName);
    }
    // "Disables" the policy
    // TODO Should this synchronize on the actual table name instead of the map?
    synchronized (enforcedPolicies) {
      SpaceViolationPolicyEnforcement enforcement = enforcedPolicies.remove(tableName);
      if (null != enforcement) {
        try {
          enforcement.disable();
        } catch (IOException e) {
          LOG.error("Failed to disable space violation policy for " + tableName
              + ". This table will remain in violation.", e);
          enforcedPolicies.put(tableName, enforcement);
        }
      }
    }
  }

  /**
   * Returns whether or not compactions should be disabled for the given <code>tableName</code> per
   * a space quota violation policy.
   *
   * @param tableName The table to check
   * @return True if compactions should be disabled for the table, false otherwise.
   */
  public boolean areCompactionsDisabled(TableName tableName) {
    SpaceViolationPolicyEnforcement enforcement = getEnforcement(tableName);
    if (null != enforcement) {
      return enforcement.areCompactionsDisabled();
    }
    return false;
  }

  /**
   * Returns the collection of tables which have quota violation policies enforced on
   * this RegionServer.
   */
  Map<TableName,SpaceViolationPolicyEnforcement> copyActiveEnforcements() {
    // Allows reads to happen concurrently (or while the map is being updated)
    return new HashMap<>(this.enforcedPolicies);
  }

  private SpaceViolationPolicyEnforcement getEnforcement(TableName tableName) {
    // Allows reads to happen concurrently (or while the map is being updated)
    return this.enforcedPolicies.get(Objects.requireNonNull(tableName));
  }

  /**
   * Wrapper around {@link QuotaTableUtil#extractViolationPolicy(Result, Map)} for testing.
   */
  void extractViolationPolicy(Result result, Map<TableName,SpaceViolation> activeViolations) {
    QuotaTableUtil.extractViolationPolicy(result, activeViolations);
  }

  RegionServerServices getRegionServerServices() {
    return rsServices;
  }

  Connection getConnection() {
    return rsServices.getConnection();
  }

  SpaceViolationPolicyEnforcementFactory getFactory() {
    return factory;
  }
}
