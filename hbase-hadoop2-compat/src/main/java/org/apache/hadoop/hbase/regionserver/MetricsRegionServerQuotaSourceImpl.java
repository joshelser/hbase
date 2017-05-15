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
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Timer;

/**
 * Implementation of {@link MetricsRegionServerQuotaSource}.
 */
public class MetricsRegionServerQuotaSourceImpl extends BaseSourceImpl implements MetricsRegionServerQuotaSource {

  private final Counter regionSizeReportCounter;
  private final Timer regionSizeReportingChoreTimer;

  public MetricsRegionServerQuotaSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsRegionServerQuotaSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    regionSizeReportCounter = this.registry.counter(NUM_REGION_SIZE_REPORT_NAME);
    regionSizeReportingChoreTimer = registry.timer(REGION_SIZE_REPORTING_CHORE_TIME_NAME);
  }

  @Override
  public void updateNumTablesInSpaceQuotaViolation(long tablesInViolation) {
    
  }

  @Override
  public void updateNumTableSpaceQuotaSnapshots(long numSnapshots) {
    
  }

  @Override
  public void incrementSpaceQuotaFileSystemScannerChoreTime(long time) {
    
  }

  @Override
  public void incrementSpaceQuotaRefresherChoreTime(long time) {
    
  }

  @Override
  public void incrementNumRegionSizeReportsSent(long numReportsSent) {
    regionSizeReportCounter.increment(numReportsSent);
  }

  @Override
  public void incrementRegionSizeReportingChoreTime(long time) {
    regionSizeReportingChoreTimer.update(time, TimeUnit.MILLISECONDS);
  }
}
