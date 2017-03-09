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

import static org.junit.Assert.assertNotNull;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.quotas.SpaceQuotaHelperForTests.SpaceQuotaSnapshotPredicate;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test class to exercise the inclusion of snapshots in space quotas
 */
@Category({LargeTests.class})
public class TestSpaceQuotasWithSnapshots {
  private static final Log LOG = LogFactory.getLog(TestSpaceQuotasWithSnapshots.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  // Global for all tests in the class
  private static final AtomicLong COUNTER = new AtomicLong(0);

  @Rule
  public TestName testName = new TestName();
  private SpaceQuotaHelperForTests helper;
  private Connection conn;
  private Admin admin;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, COUNTER);
    conn = TEST_UTIL.getConnection();
    admin = TEST_UTIL.getAdmin();
  }

  @Test
  public void testTablesInheritSnapshotSize() throws Exception {
    final long fudgeAmount = 500L * SpaceQuotaHelperForTests.ONE_KILOBYTE;
    TableName tn = helper.createTableWithRegions(1);
    LOG.info("Writing data");
    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);
    // Write some data
    final long initialSize = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    helper.writeData(tn, initialSize);

    LOG.info("Waiting until table size reflects written data");
    // Wait until that data is seen by the master
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= initialSize;
      }
    });

    // The actual size on disk after we wrote our data the first time
    final long actualInitialSize = QuotaTableUtil.getCurrentSnapshot(conn, tn).getUsage();
    LOG.info("Initial table size was " + initialSize);

    LOG.info("Snapshot the table");
    final String snapshot1 = tn.toString() + "_snapshot1";
    admin.snapshot(snapshot1, tn);

    // Write the same data again, then flush+compact. This should make sure that
    // the snapshot is referencing files that the table no longer references.
    LOG.info("Write more data");
    helper.writeData(tn, initialSize);
    LOG.info("Flush the table");
    admin.flush(tn);
    LOG.info("Synchronously compacting the table");
    TEST_UTIL.compact(tn, true);

    final long upperBound = initialSize + fudgeAmount;
    final long lowerBound = initialSize - fudgeAmount;

    // Store the actual size after writing more data and then compacting it down to one file
    final AtomicReference<Long> lastSeenSize = new AtomicReference<>();

    LOG.info("Waiting for the region reports to reflect the correct size, between ("
        + lowerBound + ", " + upperBound + ")");
    TEST_UTIL.waitFor(30 * 1000, 500, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Map<TableName,Long> sizes = QuotaTableUtil.getMasterReportedTableSizes(conn);
        LOG.debug("Master observed table sizes from region size reports: " + sizes);
        Long size = sizes.get(tn);
        if (null == size) {
          return false;
        }
        lastSeenSize.set(size);
        return size < upperBound && size > lowerBound;
      }
    });

    assertNotNull("Did not expect to see a null size", lastSeenSize.get());
    LOG.info("Last seen size: " + lastSeenSize.get());

    // Make sure the QuotaObserverChore has time to reflect the new region size reports
    // (we saw above). The usage of the table should *not* decrease when we check it below,
    // though, because the snapshot on our table will cause the table to "retain" the size.
    TEST_UTIL.waitFor(10 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override
      public boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return lastSeenSize.get() >= snapshot.getUsage();
      }
    });

    // The final usage should be the sum of the initial size (referenced by the snapshot) and the
    // new size we just wrote above.
    long expectedFinalSize = actualInitialSize + lastSeenSize.get();
    LOG.info("Expecting table usage to be " + expectedFinalSize);
    // The size of the table (WRT quotas) should now be approximately double what it was previously
    TEST_UTIL.waitFor(30 * 1000, 1000, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return expectedFinalSize == snapshot.getUsage();
      }
    });
  }

  @Test
  public void testNamespacesInheritSnapshotSize() throws Exception {
    final long fudgeAmount = 500L * SpaceQuotaHelperForTests.ONE_KILOBYTE;
    String ns = helper.createNamespace().getName();
    TableName tn = helper.createTableWithRegions(ns, 1);
    LOG.info("Writing data");
    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitNamespaceSpace(
        ns, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);

    // Write some data
    final long initialSize = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    helper.writeData(tn, initialSize);

    LOG.info("Waiting until namespace size reflects written data");
    // Wait until that data is seen by the master
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, ns) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= initialSize;
      }
    });

    // The actual size on disk after we wrote our data the first time
    final long actualInitialSize = QuotaTableUtil.getCurrentSnapshot(conn, ns).getUsage();
    LOG.info("Initial table size was " + initialSize);

    LOG.info("Snapshot the table");
    final String snapshot1 = tn.getQualifierAsString() + "_snapshot1";
    admin.snapshot(snapshot1, tn);

    // Write the same data again, then flush+compact. This should make sure that
    // the snapshot is referencing files that the table no longer references.
    LOG.info("Write more data");
    helper.writeData(tn, initialSize);
    LOG.info("Flush the table");
    admin.flush(tn);
    LOG.info("Synchronously compacting the table");
    TEST_UTIL.compact(tn, true);

    final long upperBound = initialSize + fudgeAmount;
    final long lowerBound = initialSize - fudgeAmount;

    // Store the actual size after writing more data and then compacting it down to one file
    final AtomicReference<Long> lastSeenSize = new AtomicReference<>();

    LOG.info("Waiting for the region reports to reflect the correct size, between ("
        + lowerBound + ", " + upperBound + ")");
    TEST_UTIL.waitFor(30 * 1000, 500, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Map<TableName,Long> sizes = QuotaTableUtil.getMasterReportedTableSizes(conn);
        LOG.debug("Master observed table sizes from region size reports: " + sizes);
        Long size = sizes.get(tn);
        if (null == size) {
          return false;
        }
        lastSeenSize.set(size);
        return size < upperBound && size > lowerBound;
      }
    });

    assertNotNull("Did not expect to see a null size", lastSeenSize.get());
    LOG.info("Last seen size: " + lastSeenSize.get());

    // Make sure the QuotaObserverChore has time to reflect the new region size reports
    // (we saw above). The usage of the table should *not* decrease when we check it below,
    // though, because the snapshot on our table will cause the table to "retain" the size.
    TEST_UTIL.waitFor(10 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, ns) {
      @Override
      public boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return lastSeenSize.get() >= snapshot.getUsage();
      }
    });

    // The final usage should be the sum of the initial size (referenced by the snapshot) and the
    // new size we just wrote above.
    long expectedFinalSize = actualInitialSize + lastSeenSize.get();
    LOG.info("Expecting namespace usage to be " + expectedFinalSize);
    // The size of the table (WRT quotas) should now be approximately double what it was previously
    TEST_UTIL.waitFor(30 * 1000, 1000, new SpaceQuotaSnapshotPredicate(conn, ns) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return expectedFinalSize == snapshot.getUsage();
      }
    });
  }
}
