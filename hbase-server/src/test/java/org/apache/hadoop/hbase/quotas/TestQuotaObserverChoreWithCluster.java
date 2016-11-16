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
import java.util.Map.Entry;
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
import org.apache.hadoop.hbase.quotas.QuotaObserverChore.TablesWithQuotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Test class for {@link QuotaObserverChore}.
 */
@Category(MediumTests.class)
public class TestQuotaObserverChoreWithCluster {
  private static final Log LOG = LogFactory.getLog(TestQuotaObserverChoreWithCluster.class);
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
    final Multimap<TableName, QuotaSettings> quotas = createTablesWithSpaceQuotas();
    Set<TableName> tablesWithQuotas = new HashSet<>();
    Set<TableName> namespaceTablesWithQuotas = new HashSet<>();
    // Partition the tables with quotas by table and ns quota
    partitionTablesByQuotaTarget(quotas, tablesWithQuotas, namespaceTablesWithQuotas);

    TablesWithQuotas tables = chore.fetchAllTablesWithQuotasDefined();
    assertEquals("Found tables: " + tables, tablesWithQuotas, tables.getTableQuotaTables());
    assertEquals("Found tables: " + tables, namespaceTablesWithQuotas, tables.getNamespaceQuotaTables());
  }

  @Test
  public void testRpcQuotaTablesAreFiltered() throws Exception {
    final Multimap<TableName, QuotaSettings> quotas = createTablesWithSpaceQuotas();
    Set<TableName> tablesWithQuotas = new HashSet<>();
    Set<TableName> namespaceTablesWithQuotas = new HashSet<>();
    // Partition the tables with quotas by table and ns quota
    partitionTablesByQuotaTarget(quotas, tablesWithQuotas, namespaceTablesWithQuotas);

    TableName rpcQuotaTable = createTable();
    TEST_UTIL.getAdmin().setQuota(QuotaSettingsFactory
      .throttleTable(rpcQuotaTable, ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));

    // The `rpcQuotaTable` should not be included in this Set
    TablesWithQuotas tables = chore.fetchAllTablesWithQuotasDefined();
    assertEquals("Found tables: " + tables, tablesWithQuotas, tables.getTableQuotaTables());
    assertEquals("Found tables: " + tables, namespaceTablesWithQuotas, tables.getNamespaceQuotaTables());
  }

  @Test
  public void testFilterRegions() throws Exception {
    Map<TableName,Integer> mockReportedRegions = new HashMap<>();
    // Can't mock because of primitive int as a return type -- Mockito
    // can only handle an Integer.
    QuotaObserverChore mockedChore = new QuotaObserverChore(master) {
      @Override
      int numRegionsForTable(Map<HRegionInfo,Long> snapshot, TableName table) {
        Integer i = mockReportedRegions.get(table);
        if (null == i) {
          return 0;
        }
        return i;
      }
    };
    TablesWithQuotas tables = new TablesWithQuotas(mockedChore);

    // Create the tables
    TableName tn1 = createTableWithRegions(20);
    TableName tn2 = createTableWithRegions(20);
    TableName tn3 = createTableWithRegions(20);

    // Add them to the Tables with Quotas object
    tables.addTableQuotaTable(tn1);
    tables.addTableQuotaTable(tn2);
    tables.addTableQuotaTable(tn3);

    // Mock the number of regions reported
    mockReportedRegions.put(tn1, 10); // 50%
    mockReportedRegions.put(tn2, 19); // 95%
    mockReportedRegions.put(tn3, 20); // 100%

    // Argument is un-used
    tables.filterInsufficientlyReportedTables(null);
    // The default of 95% reported should prevent tn1 from appearing
    assertEquals(new HashSet<>(Arrays.asList(tn2, tn3)), tables.getTableQuotaTables());
  }

  @Test
  public void testFetchSpaceQuota() throws Exception {
    Multimap<TableName,QuotaSettings> tables = createTablesWithSpaceQuotas();
    // All tables that were created should have a quota defined.
    for (Entry<TableName,QuotaSettings> entry : tables.entries()) {
      final TableName table = entry.getKey();
      final QuotaSettings qs = entry.getValue();

      assertTrue("QuotaSettings was an instance of " + qs.getClass(),
          qs instanceof SpaceLimitSettings);

      SpaceQuota spaceQuota = null;
      if (null != qs.getTableName()) {
        spaceQuota = chore.getSpaceQuotaForTable(table);
        assertNotNull("Could not find table space quota for " + table, spaceQuota);
      } else if (null != qs.getNamespace()) {
        spaceQuota = chore.getSpaceQuotaForNamespace(table.getNamespaceAsString());
        assertNotNull("Could not find namespace space quota for " + table.getNamespaceAsString(), spaceQuota);
      } else {
        fail("Expected table or namespace space quota");
      }

      final SpaceLimitSettings sls = (SpaceLimitSettings) qs;
      assertEquals(sls.getProto().getQuota(), spaceQuota);
    }

    TableName tableWithoutQuota = createTable();
    assertNull(chore.getSpaceQuotaForTable(tableWithoutQuota));
  }

  //
  // Helpers
  //

  Multimap<TableName, QuotaSettings> createTablesWithSpaceQuotas() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final Multimap<TableName, QuotaSettings> tablesWithQuotas = HashMultimap.create();

    final TableName tn1 = createTable();
    final TableName tn2 = createTable();

    NamespaceDescriptor nd = NamespaceDescriptor.create("ns" + COUNTER.getAndIncrement()).build();
    admin.createNamespace(nd);
    final TableName tn3 = createTableInNamespace(nd);
    final TableName tn4 = createTableInNamespace(nd);
    final TableName tn5 = createTableInNamespace(nd);

    final long sizeLimit1 = 1024L * 1024L * 1024L * 1024L * 5L; // 5TB
    final SpaceViolationPolicy violationPolicy1 = SpaceViolationPolicy.NO_WRITES;
    QuotaSettings qs1 = QuotaSettingsFactory.limitTableSpace(tn1, sizeLimit1, violationPolicy1);
    tablesWithQuotas.put(tn1, qs1);
    admin.setQuota(qs1);

    final long sizeLimit2 = 1024L * 1024L * 1024L * 200L; // 200GB
    final SpaceViolationPolicy violationPolicy2 = SpaceViolationPolicy.NO_WRITES_COMPACTIONS;
    QuotaSettings qs2 = QuotaSettingsFactory.limitTableSpace(tn2, sizeLimit2, violationPolicy2);
    tablesWithQuotas.put(tn2, qs2);
    admin.setQuota(qs2);

    final long sizeLimit3 = 1024L * 1024L * 1024L * 1024L * 100L; // 100TB
    final SpaceViolationPolicy violationPolicy3 = SpaceViolationPolicy.NO_INSERTS;
    QuotaSettings qs3 = QuotaSettingsFactory.limitNamespaceSpace(nd.getName(), sizeLimit3, violationPolicy3);
    tablesWithQuotas.put(tn3, qs3);
    tablesWithQuotas.put(tn4, qs3);
    tablesWithQuotas.put(tn5, qs3);
    admin.setQuota(qs3);

    final long sizeLimit4 = 1024L * 1024L * 1024L * 5L; // 5GB
    final SpaceViolationPolicy violationPolicy4 = SpaceViolationPolicy.NO_INSERTS;
    QuotaSettings qs4 = QuotaSettingsFactory.limitTableSpace(tn5, sizeLimit4, violationPolicy4);
    // Override the ns quota for tn5, import edge-case to catch table quota taking
    // precedence over ns quota.
    tablesWithQuotas.put(tn5, qs4);
    admin.setQuota(qs4);

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

  void partitionTablesByQuotaTarget(Multimap<TableName,QuotaSettings> quotas,
      Set<TableName> tablesWithTableQuota, Set<TableName> tablesWithNamespaceQuota) {
    // Partition the tables with quotas by table and ns quota
    for (Entry<TableName, QuotaSettings> entry : quotas.entries()) {
      SpaceLimitSettings settings = (SpaceLimitSettings) entry.getValue();
      TableName tn = entry.getKey();
      if (null != settings.getTableName()) {
        tablesWithTableQuota.add(tn);
      }
      if (null != settings.getNamespace()) {
        tablesWithNamespaceQuota.add(tn);
      }

      if (null == settings.getTableName() && null == settings.getNamespace()) {
        fail("Unexpected table name with null tableName and namespace: " + tn);
      }
    }
  }
}
