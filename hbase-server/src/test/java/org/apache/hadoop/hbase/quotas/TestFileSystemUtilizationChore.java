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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test class for {@link FileSystemUtilizationChore}.
 */
@Category(SmallTests.class)
public class TestFileSystemUtilizationChore {

  @Test
  public void testNoOnlineRegions() {
    // One region with a store size of one.
    final List<Long> regionSizes = Collections.emptyList();
    final Configuration conf = getDefaultHBaseConfiguration();
    final HRegionServer rs = mockRegionServer(conf);
    final FileSystemUtilizationChore chore = new FileSystemUtilizationChore(rs);
    doAnswer(new ExpectedRegionSizeSummationAnswer(sum(regionSizes)))
        .when(rs)
        .reportRegionSizesForQuotas((Map<HRegionInfo,Long>) any(Map.class));

    final Region region = mockRegionWithSize(regionSizes);
    when(rs.getOnlineRegions()).thenReturn(Arrays.asList(region));
    chore.chore();
  }

  @Test
  public void testRegionSizes() {
    // One region with a store size of one.
    final List<Long> regionSizes = Arrays.asList(1024L);
    final Configuration conf = getDefaultHBaseConfiguration();
    final HRegionServer rs = mockRegionServer(conf);
    final FileSystemUtilizationChore chore = new FileSystemUtilizationChore(rs);
    doAnswer(new ExpectedRegionSizeSummationAnswer(sum(regionSizes)))
        .when(rs)
        .reportRegionSizesForQuotas((Map<HRegionInfo,Long>) any(Map.class));

    final Region region = mockRegionWithSize(regionSizes);
    when(rs.getOnlineRegions()).thenReturn(Arrays.asList(region));
    chore.chore();
  }

  @Test
  public void testMultipleRegionSizes() {
    final Configuration conf = getDefaultHBaseConfiguration();
    final HRegionServer rs = mockRegionServer(conf);

    // Three regions with multiple store sizes
    final List<Long> r1Sizes = Arrays.asList(1024L, 2048L);
    final long r1Sum = sum(r1Sizes);
    final List<Long> r2Sizes = Arrays.asList(1024L * 1024L);
    final long r2Sum = sum(r2Sizes);
    final List<Long> r3Sizes = Arrays.asList(10L * 1024L * 1024L);
    final long r3Sum = sum(r3Sizes);

    final FileSystemUtilizationChore chore = new FileSystemUtilizationChore(rs);
    doAnswer(new ExpectedRegionSizeSummationAnswer(sum(Arrays.asList(r1Sum, r2Sum, r3Sum))))
        .when(rs)
        .reportRegionSizesForQuotas((Map<HRegionInfo,Long>) any(Map.class));

    final Region r1 = mockRegionWithSize(r1Sizes);
    final Region r2 = mockRegionWithSize(r2Sizes);
    final Region r3 = mockRegionWithSize(r3Sizes);
    when(rs.getOnlineRegions()).thenReturn(Arrays.asList(r1, r2, r3));
    chore.chore();
  }

  @Test
  public void testDefaultConfigurationProperties() {
    final Configuration conf = getDefaultHBaseConfiguration();
    final HRegionServer rs = mockRegionServer(conf);
    final FileSystemUtilizationChore chore = new FileSystemUtilizationChore(rs);
    // Verify that the expected default values are actually represented.
    assertEquals(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_PERIOD_DEFAULT, chore.getPeriod());
    assertEquals(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_DELAY_DEFAULT, chore.getInitialDelay());
    assertEquals(TimeUnit.valueOf(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_TIMEUNIT_DEFAULT), chore.getTimeUnit());
  }

  @Test
  public void testNonDefaultConfigurationProperties() {
    final Configuration conf = getDefaultHBaseConfiguration();
    // Override the default values
    final int period = 60 * 10;
    final long delay = 30L;
    final TimeUnit timeUnit = TimeUnit.SECONDS;
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_PERIOD_KEY, period);
    conf.setLong(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_DELAY_KEY, delay);
    conf.set(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_TIMEUNIT_KEY, timeUnit.name());

    // Verify that the chore reports these non-default values
    final HRegionServer rs = mockRegionServer(conf);
    final FileSystemUtilizationChore chore = new FileSystemUtilizationChore(rs);
    assertEquals(period, chore.getPeriod());
    assertEquals(delay, chore.getInitialDelay());
    assertEquals(timeUnit, chore.getTimeUnit());
  }

  /**
   * Creates an HBase Configuration object for the default values.
   */
  private Configuration getDefaultHBaseConfiguration() {
    final Configuration conf = HBaseConfiguration.create();
    conf.addResource("hbase-default.xml");
    return conf;
  }

  /**
   * Creates an HRegionServer using the given Configuration.
   */
  private HRegionServer mockRegionServer(Configuration conf) {
    final HRegionServer rs = mock(HRegionServer.class);
    when(rs.getConfiguration()).thenReturn(conf);
    return rs;
  }

  /**
   * Sums the collection of non-null numbers.
   */
  private long sum(Collection<Long> values) {
    long sum = 0L;
    for (Long value : values) {
      assertNotNull(value);
      sum += value;
    }
    return sum;
  }

  /**
   * Creates a region with a number of Stores equal to the length of {@code storeSizes}. Each
   * {@link Store} will have a reported size corresponding to the element in {@code storeSizes}.
   *
   * @param storeSizes A list of sizes for each Store.
   * @return A list of Mocks.
   */
  private Region mockRegionWithSize(Collection<Long> storeSizes) {
    final Region r = mock(Region.class);
    final HRegionInfo info = mock(HRegionInfo.class);
    when(r.getRegionInfo()).thenReturn(info);
    List<Store> stores = new ArrayList<>();
    when(r.getStores()).thenReturn(stores);
    for (Long storeSize : storeSizes) {
      final Store s = mock(Store.class);
      stores.add(s);
      when(s.getStorefilesSize()).thenReturn(storeSize);
    }
    return r;
  }

  /**
   * An Answer implementation which verifies the sum of the Region sizes to report is as expected.
   */
  private static class ExpectedRegionSizeSummationAnswer implements Answer<Void> {
    private final long expectedSize;

    public ExpectedRegionSizeSummationAnswer(long expectedSize) {
      this.expectedSize = expectedSize;
    }

    @Override
    public Void answer(InvocationOnMock invocation) throws Throwable {
      Object[] args = invocation.getArguments();
      assertEquals(1, args.length);
      @SuppressWarnings("unchecked")
      Map<HRegionInfo,Long> regionSizes = (Map<HRegionInfo,Long>) args[0];
      long sum = 0L;
      for (Long regionSize : regionSizes.values()) {
        sum += regionSize;
      }
      assertEquals(expectedSize, sum);
      return null;
    }
  }
}
