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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * A collection of exposed metrics for HBase quotas from the HBase Master.
 */
@InterfaceAudience.Private
public interface MetricsMasterQuotaSource extends BaseSource {

  String METRICS_NAME = "Quotas";
  String METRICS_CONTEXT = "master";
  String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;
  String METRICS_DESCRIPTION = "Metrics about HBase Quotas by the Master";

  /**
   * Updates the metric tracking the number of space quotas defined in the system.
   */
  void updateNumSpaceQuotas(long numSpaceQuotas);

  /**
   * Updates the metric tracking the number of tables the master has computed to be in
   * violation of their space quota.
   */
  void updateNumTablesInSpaceQuotaViolation(long numTablesInViolation);

  /**
   * Updates the metric tracking the number of namespaces the master has computed to be in
   * violation of their space quota.
   */
  void updateNumNamespacesInSpaceQuotaViolation(long numNamespacesInViolation);

  /**
   * Updates the metric tracking the number of region size reports the master is currently
   * retaining in memory.
   */
  void updateNumCurrentSpaceQuotaRegionSizeReports(long numCurrentRegionSizeReports);

  /**
   * Updates the metric tracking the amount of time taken by the {@code QuotaObserverChore}
   * which runs periodically.
   */
  void incrementSpaceQuotaObserverChoreTime(long time);
}
