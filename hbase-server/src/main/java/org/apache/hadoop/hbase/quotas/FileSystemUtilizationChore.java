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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;

/**
 * A chore which computes the size of each {@link HRegion} on the FileSystem hosted by the given {@link HRegionServer}.
 */
public class FileSystemUtilizationChore extends ScheduledChore {
  private static final Log LOG = LogFactory.getLog(FileSystemUtilizationChore.class);
  static final String FS_UTILIZATION_CHORE_PERIOD_KEY = "hbase.regionserver.quotas.fs.utilization.chore.period";
  static final int FS_UTILIZATION_CHORE_PERIOD_DEFAULT = 1000 * 60 * 5; // 5 minutes in millis

  static final String FS_UTILIZATION_CHORE_DELAY_KEY = "hbase.regionserver.quotas.fs.utilization.chore.delay";
  static final long FS_UTILIZATION_CHORE_DELAY_DEFAULT = 1000L * 60L; // 1 minute

  static final String FS_UTILIZATION_CHORE_TIMEUNIT_KEY = "hbase.regionserver.quotas.fs.utilization.chore.timeunit";
  static final String FS_UTILIZATION_CHORE_TIMEUNIT_DEFAULT = TimeUnit.MILLISECONDS.name();

  private final HRegionServer rs;

  public FileSystemUtilizationChore(HRegionServer rs) {
    super(FileSystemUtilizationChore.class.getSimpleName(), rs, getPeriod(rs.getConfiguration()),
        getInitialDelay(rs.getConfiguration()), getTimeUnit(rs.getConfiguration()));
    this.rs = rs;
  }

  @Override
  protected void chore() {
    // TODO Chore threads are shared -- cannot "hog" the chore thread.
    //    * Should we impose an upper bound on regions having size calculated? Maximum time spent?
    //    * How do we track "leftovers" if we bail out early? A: Keep state of onlineRegions on the Chore since its a singleton?
    final Map<HRegionInfo,Long> onlineRegionSizes = new HashMap<>();
    final List<Region> onlineRegions = rs.getOnlineRegions();
    long regionSizesCalculated = 0L;
    for (Region onlineRegion : onlineRegions) {
      final long sizeInBytes = computeSize(onlineRegion);
      onlineRegionSizes.put(onlineRegion.getRegionInfo(), sizeInBytes);
      regionSizesCalculated++;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Computed the size of " + regionSizesCalculated + " Regions.");
    }
    reportRegionSizesToMaster(onlineRegionSizes);
  }

  /**
   * Computes total FileSystem size for the given {@link Region}.
   *
   * @param r The region
   * @return The size, in bytes, of the Region.
   */
  long computeSize(Region r) {
    long regionSize = 0L;
    for (Store store : r.getStores()) {
      // StoreFile/StoreFileReaders are already instantiated with the file length cached. Can avoid extra NN ops.
      regionSize += store.getStorefilesSize();
    }
    return regionSize;
  }

  /**
   * Reports the computed region sizes to the currently active Master.
   *
   * @param onlineRegionSizes The computed region sizes to report.
   */
  void reportRegionSizesToMaster(Map<HRegionInfo,Long> onlineRegionSizes) {
    this.rs.reportRegionSizesForQuotas(onlineRegionSizes);
  }

  /**
   * Extracts the period for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore period or the default value.
   */
  static int getPeriod(Configuration conf) {
    return conf.getInt(FS_UTILIZATION_CHORE_PERIOD_KEY, FS_UTILIZATION_CHORE_PERIOD_DEFAULT);
  }

  /**
   * Extracts the initial delay for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore initial delay or the default value.
   */
  static long getInitialDelay(Configuration conf) {
    return conf.getLong(FS_UTILIZATION_CHORE_DELAY_KEY, FS_UTILIZATION_CHORE_DELAY_DEFAULT);
  }

  /**
   * Extracts the time unit for the chore period and initial delay from the configuration. The configuration
   * value for {@link #FS_UTILIZATION_CHORE_TIMEUNIT_KEY} must correspond to a {@link TimeUnit} value.
   *
   * @param conf The configuration object.
   * @return The configured time unit for the chore period and initial delay or the default value.
   */
  static TimeUnit getTimeUnit(Configuration conf) {
    return TimeUnit.valueOf(conf.get(FS_UTILIZATION_CHORE_TIMEUNIT_KEY,
        FS_UTILIZATION_CHORE_TIMEUNIT_DEFAULT));
  }
}
