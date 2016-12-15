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

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.quotas.policies.DisableTableViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoInsertsViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoWritesCompactionsViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoWritesViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoopViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test class for {@link RegionServerSpaceQuotaManager}.
 */
public class TestRegionServerSpaceQuotaManager {

  private RegionServerSpaceQuotaManager quotaManager;
  private Connection conn;
  private Table quotaTable;
  private ResultScanner scanner;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() throws Exception {
    quotaManager = mock(RegionServerSpaceQuotaManager.class);
    conn = mock(Connection.class);
    quotaTable = mock(Table.class);
    scanner = mock(ResultScanner.class);
    // Call the real getActivePolicyEnforcements()
    when(quotaManager.getViolationPoliciesToEnforce()).thenCallRealMethod();
    // Mock out creating a scanner
    when(quotaManager.getConnection()).thenReturn(conn);
    when(conn.getTable(QuotaUtil.QUOTA_TABLE_NAME)).thenReturn(quotaTable);
    when(quotaTable.getScanner(any(Scan.class))).thenReturn(scanner);
    // Mock out the static method call with some indirection
    doAnswer(new Answer<Void>(){
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Result result = invocation.getArgumentAt(0, Result.class);
        Map<TableName,SpaceViolationPolicy> policies = invocation.getArgumentAt(1, Map.class);
        QuotaTableUtil.extractViolationPolicy(result, policies);
        return null;
      }
    }).when(quotaManager).extractViolationPolicy(any(Result.class), any(Map.class));
  }

  @Test
  public void testMissingAllColumns() {
    List<Result> results = new ArrayList<>();
    results.add(Result.create(Collections.emptyList()));
    when(scanner.iterator()).thenReturn(results.iterator());
    try {
      quotaManager.getViolationPoliciesToEnforce();
      fail("Expected an IOException, but did not receive one.");
    } catch (IOException e) {
      // Expected an error because we had no cells in the row.
      // This should only happen due to programmer error.
    }
  }

  @Test
  public void testMissingDesiredColumn() {
    List<Result> results = new ArrayList<>();
    // Give a column that isn't the one we want
    Cell c = new KeyValue(toBytes("t:inviolation"), toBytes("q"), toBytes("s"), new byte[0]);
    results.add(Result.create(Collections.singletonList(c)));
    when(scanner.iterator()).thenReturn(results.iterator());
    try {
      quotaManager.getViolationPoliciesToEnforce();
      fail("Expected an IOException, but did not receive one.");
    } catch (IOException e) {
      // Expected an error because we were missing the column we expected in this row.
      // This should only happen due to programmer error.
    }
  }

  @Test
  public void testParsingError() {
    List<Result> results = new ArrayList<>();
    Cell c = new KeyValue(toBytes("t:inviolation"), toBytes("u"), toBytes("v"), new byte[0]);
    results.add(Result.create(Collections.singletonList(c)));
    when(scanner.iterator()).thenReturn(results.iterator());
    try {
      quotaManager.getViolationPoliciesToEnforce();
      fail("Expected an IOException, but did not receive one.");
    } catch (IOException e) {
      // We provided a garbage serialized protobuf message (empty byte array), this should
      // in turn throw an IOException
    }
  }

  @Test
  public void testSpacePoliciesFromEnforcements() {
    final Map<TableName, SpaceViolationPolicyEnforcement> enforcements = new HashMap<>();
    final Map<TableName, SpaceViolationPolicy> expectedPolicies = new HashMap<>();
    when(quotaManager.copyActiveEnforcements()).thenReturn(enforcements);
    when(quotaManager.getActivePoliciesAsMap()).thenCallRealMethod();

    enforcements.put(TableName.valueOf("no_inserts"), new NoInsertsViolationPolicyEnforcement());
    expectedPolicies.put(TableName.valueOf("no_inserts"), SpaceViolationPolicy.NO_INSERTS);

    enforcements.put(TableName.valueOf("no_writes"), new NoWritesViolationPolicyEnforcement());
    expectedPolicies.put(TableName.valueOf("no_writes"), SpaceViolationPolicy.NO_WRITES);

    enforcements.put(TableName.valueOf("no_writes_compactions"),
        new NoWritesCompactionsViolationPolicyEnforcement());
    expectedPolicies.put(TableName.valueOf("no_writes_compactions"),
        SpaceViolationPolicy.NO_WRITES_COMPACTIONS);

    enforcements.put(TableName.valueOf("disable"), new DisableTableViolationPolicyEnforcement());
    expectedPolicies.put(TableName.valueOf("disable"), SpaceViolationPolicy.DISABLE);

    enforcements.put(TableName.valueOf("no_policy"), NoopViolationPolicyEnforcement.getInstance());

    Map<TableName, SpaceViolationPolicy> actualPolicies = quotaManager.getActivePoliciesAsMap();
    assertEquals(expectedPolicies, actualPolicies);
  }

  @Test
  public void testExceptionOnPolicyEnforcementEnable() throws Exception {
    final TableName tableName = TableName.valueOf("foo");
    final SpaceViolationPolicy policy = SpaceViolationPolicy.DISABLE;
    RegionServerServices rss = mock(RegionServerServices.class);
    SpaceViolationPolicyEnforcementFactory factory = mock(
        SpaceViolationPolicyEnforcementFactory.class);
    SpaceViolationPolicyEnforcement enforcement = mock(SpaceViolationPolicyEnforcement.class);
    RegionServerSpaceQuotaManager realManager = new RegionServerSpaceQuotaManager(rss, factory);

    when(factory.create(rss, tableName, policy)).thenReturn(enforcement);
    doThrow(new IOException("Failed for test!")).when(enforcement).enable();

    realManager.enforceViolationPolicy(tableName, policy);
    Map<TableName, SpaceViolationPolicyEnforcement> enforcements =
        realManager.copyActiveEnforcements();
    assertTrue("Expected active enforcements to be empty, but were " + enforcements,
        enforcements.isEmpty());
  }

  @Test
  public void testExceptionOnPolicyEnforcementDisable() throws Exception {
    final TableName tableName = TableName.valueOf("foo");
    final SpaceViolationPolicy policy = SpaceViolationPolicy.DISABLE;
    RegionServerServices rss = mock(RegionServerServices.class);
    SpaceViolationPolicyEnforcementFactory factory = mock(
        SpaceViolationPolicyEnforcementFactory.class);
    SpaceViolationPolicyEnforcement enforcement = mock(SpaceViolationPolicyEnforcement.class);
    RegionServerSpaceQuotaManager realManager = new RegionServerSpaceQuotaManager(rss, factory);

    when(factory.create(rss, tableName, policy)).thenReturn(enforcement);
    doNothing().when(enforcement).enable();
    doThrow(new IOException("Failed for test!")).when(enforcement).disable();

    // Enabling should work
    realManager.enforceViolationPolicy(tableName, policy);
    Map<TableName, SpaceViolationPolicyEnforcement> enforcements =
        realManager.copyActiveEnforcements();
    assertEquals(1, enforcements.size());

    // If the disable fails, we should still treat it as "active"
    realManager.disableViolationPolicyEnforcement(tableName);
    enforcements = realManager.copyActiveEnforcements();
    assertEquals(1, enforcements.size());
  }
}
