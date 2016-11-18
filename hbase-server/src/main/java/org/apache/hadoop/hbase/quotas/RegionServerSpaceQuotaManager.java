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
import java.util.Map;
import java.util.Objects;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

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

  public RegionServerSpaceQuotaManager(RegionServerServices rsServices) {
    this.rsServices = Objects.requireNonNull(rsServices);
  }

  public synchronized void start() throws IOException {
    if (!QuotaUtil.isQuotaEnabled(rsServices.getConfiguration())) {
      LOG.info("Quota support disabled");
      return;
    }

    spaceQuotaRefresher = new SpaceQuotaViolationPolicyRefresherChore(this);
  }

  public synchronized void stop() {
    if (null != spaceQuotaRefresher) {
      spaceQuotaRefresher.cancel();
      spaceQuotaRefresher = null;
    }
  }

  /**
   * Returns the collection of tables which have quota violation policies enforced.
   */
  public Map<TableName,SpaceViolationPolicy> getActivePolicyEnforcements() {
    return null;
  }

  /**
   * Reads all quota violation policies which are to be enforced from the quota table.
   * @return The collection of tables which are in violation of their quota and the policy which
   *    should be enforced.
   */
  Map<TableName, SpaceViolationPolicy> getEnforcedViolationPolicies() throws IOException {
    return null;
  }

  /**
   * Enforces the given violationPolicy on the given table in this RegionServer.
   */
  void enforceViolationPolicy(TableName tableName, SpaceViolationPolicy violationPolicy) {
    throw new RuntimeException();
  }

  /**
   * Disables enforcement on any violation policy on the given <code>tableName</code>.
   */
  void disableViolationPolicyEnforcement(TableName tableName) {
    throw new RuntimeException();
  }

  RegionServerServices getRegionServerServices() {
    return rsServices;
  }
}
