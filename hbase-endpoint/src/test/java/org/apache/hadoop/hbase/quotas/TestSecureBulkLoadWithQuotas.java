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

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.quotas.SpaceQuotaHelperForTests;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test class for the SecureBulkLoadEndpoint.
 */
@Category(MediumTests.class)
public class TestSecureBulkLoadWithQuotas {
  private static final Log LOG = LogFactory.getLog(TestSecureBulkLoadWithQuotas.class);
  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName testName = new TestName();
  private SpaceQuotaHelperForTests helper;

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(
        CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
    conf.set(QuotaUtil.QUOTA_CONF_KEY, "true");
    // Increase the frequency of some of the chores for responsiveness of the test
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_DELAY_KEY, 1000);
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_PERIOD_KEY, 1000);
    conf.setInt(QuotaObserverChore.VIOLATION_OBSERVER_CHORE_DELAY_KEY, 1000);
    conf.setInt(QuotaObserverChore.VIOLATION_OBSERVER_CHORE_PERIOD_KEY, 1000);
    conf.setInt(SpaceQuotaRefresherChore.POLICY_REFRESHER_CHORE_DELAY_KEY, 1000);
    conf.setInt(SpaceQuotaRefresherChore.POLICY_REFRESHER_CHORE_PERIOD_KEY, 1000);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, new AtomicLong());
  }

  @Test
  public void testSecureBulkLoads() throws Exception {
    TableName tn = helper.createTableWithRegions(10);

    // Set a very small limit
    final long sizeLimit = 1L * SpaceQuotaHelperForTests.ONE_KILOBYTE;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, sizeLimit, SpaceViolationPolicy.NO_INSERTS);
    TEST_UTIL.getAdmin().setQuota(settings);

    // Wait for the RS to acknowledge this small limit
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    RegionServerSpaceQuotaManager spaceQuotaManager = rs.getRegionServerSpaceQuotaManager();
    TEST_UTIL.waitFor(60000, 3000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Map<TableName,SpaceQuotaSnapshot> snapshots = spaceQuotaManager.copyQuotaSnapshots();
        SpaceQuotaSnapshot snapshot = snapshots.get(tn);
        LOG.debug("Snapshots: " + snapshots);
        return null != snapshot && snapshot.getLimit() > 0;
      }
    });
    // Our quota limit should be reflected in the latest snapshot
    Map<TableName,SpaceQuotaSnapshot> snapshots = spaceQuotaManager.copyQuotaSnapshots();
    SpaceQuotaSnapshot snapshot = snapshots.get(tn);
    assertEquals(0L, snapshot.getUsage());
    assertEquals(sizeLimit, snapshot.getLimit());

    // Generate a file that is ~25KB
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path baseDir = new Path(fs.getHomeDirectory(), testName.getMethodName() + "_files");
    fs.mkdirs(baseDir);
    Path hfilesDir = new Path(baseDir, SpaceQuotaHelperForTests.F1);
    fs.mkdirs(hfilesDir);
    List<Pair<byte[], String>> filesToLoad = helper.createFiles(tn, 1, 500, hfilesDir);
    // Verify that they are the size we expecte
    for (Pair<byte[], String> pair : filesToLoad) {
      String file = pair.getSecond();
      FileStatus[] statuses = fs.listStatus(new Path(file));
      assertEquals(1, statuses.length);
      FileStatus status = statuses[0];
      assertTrue(
          "Expected the file, " + file + ",  length to be larger than 25KB, but was " 
              + status.getLen(),
              status.getLen() > 25 * SpaceQuotaHelperForTests.ONE_KILOBYTE);
      LOG.debug(file + " -> " + status.getLen() +"B");
    }

    // Use LoadIncrementalHFiles to load the file which should be rejected since
    // it would violate the quota.
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(TEST_UTIL.getConfiguration());
    try {
      loader.run(new String[] {new Path(
          fs.getHomeDirectory(), testName.getMethodName() + "_files").toString(), tn.toString()});
      fail("Expected the bulk load to be rejected, but it was not");
    } catch (Exception e) {
      LOG.debug("Caught expected exception", e);
      String stringifiedException = StringUtils.stringifyException(e);
      assertTrue(
          "Expected exception message to contain the SpaceLimitingException class name: "
              + stringifiedException,
          stringifiedException.contains(SpaceLimitingException.class.getName()));
    }
  }
}
