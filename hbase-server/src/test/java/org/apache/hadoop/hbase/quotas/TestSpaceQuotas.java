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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * End-to-end test class for filesystem space quotas.
 */
@Category(LargeTests.class)
public class TestSpaceQuotas {
  private static final Log LOG = LogFactory.getLog(TestSpaceQuotas.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final AtomicLong COUNTER = new AtomicLong(0);
  private static final int NUM_RETRIES = 10;

  @Rule
  public TestName testName = new TestName();
  private SpaceQuotaHelperForTests helper;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Increase the frequency of some of the chores for responsiveness of the test
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_DELAY_KEY, 1000);
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_PERIOD_KEY, 1000);
    conf.setInt(QuotaObserverChore.VIOLATION_OBSERVER_CHORE_DELAY_KEY, 1000);
    conf.setInt(QuotaObserverChore.VIOLATION_OBSERVER_CHORE_PERIOD_KEY, 1000);
    conf.setInt(SpaceQuotaViolationPolicyRefresherChore.POLICY_REFRESHER_CHORE_DELAY_KEY, 1000);
    conf.setInt(SpaceQuotaViolationPolicyRefresherChore.POLICY_REFRESHER_CHORE_PERIOD_KEY, 1000);
    conf.setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    final Connection conn = TEST_UTIL.getConnection();
    // Wait for the quota table to be created
    if (!conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME)) {
      do {
        LOG.debug("Quota table does not yet exist");
        Thread.sleep(1000);
      } while (!conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME));
    } else {
      // Or, clean up any quotas from previous test runs.
      QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration());
      for (QuotaSettings quotaSettings : scanner) {
        final String namespace = quotaSettings.getNamespace();
        final TableName tableName = quotaSettings.getTableName();
        if (null != namespace) {
          LOG.debug("Deleting quota for namespace: " + namespace);
          QuotaUtil.deleteNamespaceQuota(conn, namespace);
        } else {
          assert null != tableName;
          LOG.debug("Deleting quota for table: "+ tableName);
          QuotaUtil.deleteTableQuota(conn, tableName);
        }
      }
    }
    helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, COUNTER);
  }

  @Test
  public void testNoInsertsWithPut() throws Exception {
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_INSERTS, p);
  }

  @Test
  public void testNoInsertsWithAppend() throws Exception {
    Append a = new Append(Bytes.toBytes("to_reject"));
    a.add(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_INSERTS, a);
  }

  @Test
  public void testNoInsertsWithIncrement() throws Exception {
    Increment i = new Increment(Bytes.toBytes("to_reject"));
    i.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("count"), 0);
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_INSERTS, i);
  }

  @Test
  public void testDeletesAfterNoInserts() throws Exception {
    final TableName tn = writeUntilViolation(SpaceViolationPolicy.NO_INSERTS);
    // Try a couple of times to verify that the quota never gets enforced, same as we
    // do when we're trying to catch the failure.
    Delete d = new Delete(Bytes.toBytes("should_not_be_rejected"));
    for (int i = 0; i < NUM_RETRIES; i++) {
      try (Table t = TEST_UTIL.getConnection().getTable(tn)) {
        t.delete(d);
      }
    }
  }

  @Test
  public void testNoWritesWithPut() throws Exception {
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, p);
  }

  @Test
  public void testNoWritesWithAppend() throws Exception {
    Append a = new Append(Bytes.toBytes("to_reject"));
    a.add(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, a);
  }

  @Test
  public void testNoWritesWithIncrement() throws Exception {
    Increment i = new Increment(Bytes.toBytes("to_reject"));
    i.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("count"), 0);
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, i);
  }

  @Test
  public void testNoWritesWithDelete() throws Exception {
    Delete d = new Delete(Bytes.toBytes("to_reject"));
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, d);
  }

  private TableName writeUntilViolation(SpaceViolationPolicy policyToViolate) throws Exception {
    TableName tn = helper.createTableWithRegions(10);

    final long sizeLimit = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(tn, sizeLimit, policyToViolate);
    TEST_UTIL.getAdmin().setQuota(settings);

    // Write more data than should be allowed and flush it to disk
    helper.writeData(tn, 3L * SpaceQuotaHelperForTests.ONE_MEGABYTE);

    // This should be sufficient time for the chores to run and see the change.
    Thread.sleep(5000);

    return tn;
  }

  private void writeUntilViolationAndVerifyViolation(SpaceViolationPolicy policyToViolate, Mutation m) throws Exception {
    final TableName tn = writeUntilViolation(policyToViolate);

    // But let's try a few times to get the exception before failing
    boolean sawError = false;
    for (int i = 0; i < NUM_RETRIES && !sawError; i++) {
      try (Table table = TEST_UTIL.getConnection().getTable(tn)) {
        if (m instanceof Put) {
          table.put((Put) m);
        } else if (m instanceof Delete) {
          table.delete((Delete) m);
        } else if (m instanceof Append) {
          table.append((Append) m);
        } else if (m instanceof Increment) {
          table.increment((Increment) m);
        } else {
          fail("Failed to apply " + m.getClass().getSimpleName() + " to the table. Programming error");
        }
        LOG.info("Did not reject the " + m.getClass().getSimpleName() + ", will sleep and retry");
        Thread.sleep(2000);
      } catch (Exception e) {
        String msg = StringUtils.stringifyException(e);
        assertTrue("Expected exception message to contain the word '" + policyToViolate.name() + "', but was " + msg,
            msg.contains(policyToViolate.name()));
        sawError = true;
      }
    }
    if (!sawError) {
      try (Table quotaTable = TEST_UTIL.getConnection().getTable(QuotaUtil.QUOTA_TABLE_NAME)) {
        ResultScanner scanner = quotaTable.getScanner(new Scan());
        Result result = null;
        LOG.info("Dumping contents of hbase:quota table");
        while ((result = scanner.next()) != null) {
          LOG.info(Bytes.toString(result.getRow()) + " => " + result.toString());
        }
        scanner.close();
      }
    }
    assertTrue("Expected to see an exception writing data to a table exceeding its quota", sawError);
  }
}
