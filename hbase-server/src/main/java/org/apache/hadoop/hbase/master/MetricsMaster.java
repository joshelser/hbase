/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;

/**
 * This class is for maintaining the various master statistics
 * and publishing them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Private
public class MetricsMaster {
  private static final Log LOG = LogFactory.getLog(MetricsMaster.class);
  private MetricsMasterSource masterSource;
  private MetricsMasterProcSource masterProcSource;
  private MetricsMasterQuotaSource masterQuotaSource;

  public MetricsMaster(MetricsMasterWrapper masterWrapper) {
    masterSource = CompatibilitySingletonFactory.getInstance(MetricsMasterSourceFactory.class).create(masterWrapper);
    masterProcSource =
            CompatibilitySingletonFactory.getInstance(MetricsMasterProcSourceFactory.class).create(masterWrapper);
    masterQuotaSource =
            CompatibilitySingletonFactory.getInstance(MetricsMasterQuotaSourceFactory.class).create(masterWrapper);
  }

  // for unit-test usage
  public MetricsMasterSource getMetricsSource() {
    return masterSource;
  }

  public MetricsMasterProcSource getMetricsProcSource() {
    return masterProcSource;
  }

  public MetricsMasterQuotaSource getMetricsQuotaSource() {
    return masterQuotaSource;
  }

  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final long inc) {
    masterSource.incRequests(inc);
  }

  /**
   * Sets the number of space quotas defined.
   *
   * @see MetricsMasterQuotaSource#updateNumSpaceQuotas(long)
   */
  public void setNumSpaceQuotas(final long numSpaceQuotas) {
    masterQuotaSource.updateNumSpaceQuotas(numSpaceQuotas);
  }

  /**
   * Sets the number of table in violation of a space quota.
   *
   * @see MetricsMasterQuotaSource#updateNumTablesInSpaceQuotaViolation(long)
   */
  public void setNumTableInSpaceQuotaViolation(final long numTablesInViolation) {
    masterQuotaSource.updateNumTablesInSpaceQuotaViolation(numTablesInViolation);
  }

  /**
   * Sets the number of namespaces in violation of a space quota.
   *
   * @see MetricsMasterQuotaSource#updateNumNamespacesInSpaceQuotaViolation(long)
   */
  public void setNumNamespacesInSpaceQuotaViolation(final long numNamespacesInViolation) {
    masterQuotaSource.updateNumNamespacesInSpaceQuotaViolation(numNamespacesInViolation);
  }

  /**
   * Sets the number of region size reports the master currently has in memory.
   *
   * @see MetricsMasterQuotaSource#updateNumCurrentSpaceQuotaRegionSizeReports(long)
   */
  public void setNumRegionSizeReports(final long numRegionReports) {
    masterQuotaSource.updateNumCurrentSpaceQuotaRegionSizeReports(numRegionReports);
  }

  /**
   * Sets the execution time of a period of the QuotaObserverChore.
   *
   * @param executionTime The execution time in milliseconds.
   * @see MetricsMasterQuotaSource#incrementSpaceQuotaObserverChoreTime(long)
   */
  public void incrementQuotaObserverTime(final long executionTime) {
    masterQuotaSource.incrementSpaceQuotaObserverChoreTime(executionTime);
  }
}
