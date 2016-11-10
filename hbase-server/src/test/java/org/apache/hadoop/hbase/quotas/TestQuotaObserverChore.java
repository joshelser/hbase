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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test class for {@link QuotaObserverChore}.
 */
@Category(MediumTests.class)
public class TestQuotaObserverChore {
  private static final Log LOG = LogFactory.getLog(TestQuotaObserverChore.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final AtomicLong COUNTER = new AtomicLong(0);

  @Rule
  public TestName testName = new TestName();

  private HMaster master;
  private QuotaObserverChore chore;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
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

    master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    chore = new QuotaObserverChore(master);
  }

  @Test
  public void testGetAllTablesWithQuotas() throws Exception {
    final Set<TableName> tablesWithQuotas = createTablesWithSpaceQuotas();

    Set<TableName> actualTableNames = chore.fetchAllTablesWithQuotasDefined();
    assertEquals(tablesWithQuotas, actualTableNames);
  }

  @Test
  public void testRpcQuotaTablesAreFiltered() throws Exception {
    final Set<TableName> tablesWithQuotas = createTablesWithSpaceQuotas();

    TableName rpcQuotaTable = createTable();
    TEST_UTIL.getAdmin().setQuota(QuotaSettingsFactory
      .throttleTable(rpcQuotaTable, ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));

    // The `rpcQuotaTable` should not be included in this Set
    Set<TableName> actualTableNames = chore.fetchAllTablesWithQuotasDefined();
    assertEquals(tablesWithQuotas, actualTableNames);
  }

  @Test
  public void testFilterRegions() throws Exception {
    Map<TableName,Integer> mockReportedRegions = new HashMap<>();
    QuotaObserverChore mockedChore = new QuotaObserverChore(master){
      @Override
      int getNumReportedRegionsForQuota(TableName table, Map<HRegionInfo, Long> snapshot) {
        Integer i = mockReportedRegions.get(table);
        if (null == i) {
          return 0;
        }
        return i;
      }
    };

    TableName tn1 = createTableWithRegions(20);
    TableName tn2 = createTableWithRegions(20);
    TableName tn3 = createTableWithRegions(20);

    mockReportedRegions.put(tn1, 10); // 50%
    mockReportedRegions.put(tn2, 19); // 95%
    mockReportedRegions.put(tn3, 20); // 100%

    Set<TableName> filteredTables = mockedChore.filterInsufficientlyReportedTables(mockReportedRegions.keySet());
    assertEquals(new HashSet<>(Arrays.asList(tn2, tn3)), filteredTables);
  }

  @Test
  public void testFilterRegionsByTable() throws Exception {
    TableName tn1 = TableName.valueOf("foo");
    TableName tn2 = TableName.valueOf("bar");
    TableName tn3 = TableName.valueOf("ns", "foo");
    Map<HRegionInfo, Long> regions = new HashMap<>();
    assertEquals(0, chore.getNumReportedRegionsForQuota(tn1, regions));
    for (int i = 0; i < 5; i++) {
      regions.put(new HRegionInfo(tn1, Bytes.toBytes(i), Bytes.toBytes(i+1)), 0L);
    }
    for (int i = 0; i < 3; i++) {
      regions.put(new HRegionInfo(tn2, Bytes.toBytes(i), Bytes.toBytes(i+1)), 0L);
    }
    for (int i = 0; i < 10; i++) {
      regions.put(new HRegionInfo(tn3, Bytes.toBytes(i), Bytes.toBytes(i+1)), 0L);
    }
    assertEquals(18, regions.size());
    assertEquals(5, chore.getNumReportedRegionsForQuota(tn1, regions));
    assertEquals(3, chore.getNumReportedRegionsForQuota(tn2, regions));
    assertEquals(10, chore.getNumReportedRegionsForQuota(tn3, regions));
  }

  Set<TableName> createTablesWithSpaceQuotas() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final Set<TableName> tablesWithQuotas = new HashSet<>();

    final TableName tn1 = createTable();
    tablesWithQuotas.add(tn1);
    final TableName tn2 = createTable();
    tablesWithQuotas.add(tn2);

    NamespaceDescriptor nd = NamespaceDescriptor.create("ns" + COUNTER.getAndIncrement()).build();
    admin.createNamespace(nd);
    final TableName tn3 = createTableInNamespace(nd);
    final TableName tn4 = createTableInNamespace(nd);
    final TableName tn5 = createTableInNamespace(nd);
    tablesWithQuotas.add(tn3);
    tablesWithQuotas.add(tn4);
    tablesWithQuotas.add(tn5);

    final long sizeLimit1 = 1024L * 1024L * 1024L * 1024L * 5L; // 5TB
    final SpaceViolationPolicy violationPolicy1 = SpaceViolationPolicy.NO_WRITES;
    admin.setQuota(QuotaSettingsFactory.limitTableSpace(tn1, sizeLimit1, violationPolicy1));

    final long sizeLimit2 = 1024L * 1024L * 1024L * 200L; // 200GB
    final SpaceViolationPolicy violationPolicy2 = SpaceViolationPolicy.NO_WRITES_COMPACTIONS;
    admin.setQuota(QuotaSettingsFactory.limitTableSpace(tn2, sizeLimit2, violationPolicy2));

    final long sizeLimit3 = 1024L * 1024L * 1024L * 1024L * 100L; // 100TB
    final SpaceViolationPolicy violationPolicy3 = SpaceViolationPolicy.NO_INSERTS;
    admin.setQuota(QuotaSettingsFactory.limitNamespaceSpace(nd.getName(), sizeLimit3, violationPolicy3));

    final long sizeLimit4 = 1024L * 1024L * 1024L * 5L; // 5GB
    final SpaceViolationPolicy violationPolicy4 = SpaceViolationPolicy.NO_INSERTS;
    admin.setQuota(QuotaSettingsFactory.limitTableSpace(tn5, sizeLimit4, violationPolicy4));

    return tablesWithQuotas;
  }

  TableName createTable() throws Exception {
    return createTableWithRegions(1);
  }

  TableName createTableWithRegions(int numRegions) throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf(testName.getMethodName() + COUNTER.getAndIncrement());

    // Delete the old table
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }

    // Create the table
    HTableDescriptor tableDesc = new HTableDescriptor(tn);
    tableDesc.addFamily(new HColumnDescriptor("f1"));
    if (numRegions == 1) {
      admin.createTable(tableDesc);
    } else {
      admin.createTable(tableDesc, Bytes.toBytes("a"), Bytes.toBytes("z"), numRegions);
    }
    return tn;
  }

  TableName createTableInNamespace(NamespaceDescriptor nd) throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf(nd.getName(),
        testName.getMethodName() + COUNTER.getAndIncrement());

    // Delete the old table
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }

    // Create the table
    HTableDescriptor tableDesc = new HTableDescriptor(tn);
    tableDesc.addFamily(new HColumnDescriptor("f1"));
    
    admin.createTable(tableDesc);
    return tn;
  }
  
}
