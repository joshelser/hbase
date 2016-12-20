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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.policies.DisableTableViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoInsertsViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoWritesCompactionsViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoWritesViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoopViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link SpaceQuotaViolationPolicyRefresherChore}.
 */
public class TestSpaceQuotaViolationPolicyRefresherChore {

  private RegionServerSpaceQuotaManager manager;
  private RegionServerServices rss;
  private SpaceQuotaViolationPolicyRefresherChore chore;
  private Configuration conf;

  @Before
  public void setup() {
    conf = HBaseConfiguration.create();
    rss = mock(RegionServerServices.class);
    manager = mock(RegionServerSpaceQuotaManager.class);
    when(manager.getRegionServerServices()).thenReturn(rss);
    when(rss.getConfiguration()).thenReturn(conf);
    chore = new SpaceQuotaViolationPolicyRefresherChore(manager);
  }

  @Test
  public void testPoliciesAreEnforced() throws IOException {
    // Create a number of policies that should be enforced (usage > limit)
    final Map<TableName,SpaceQuotaSnapshot> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(TableName.valueOf("table1"), new SpaceQuotaSnapshot(SpaceViolationPolicy.DISABLE, 1024L, 512L));
    policiesToEnforce.put(TableName.valueOf("table2"), new SpaceQuotaSnapshot(SpaceViolationPolicy.NO_INSERTS, 2048L, 512L));
    policiesToEnforce.put(TableName.valueOf("table3"), new SpaceQuotaSnapshot(SpaceViolationPolicy.NO_WRITES, 4096L, 512L));
    policiesToEnforce.put(TableName.valueOf("table4"), new SpaceQuotaSnapshot(SpaceViolationPolicy.NO_WRITES_COMPACTIONS, 8192L, 512L));

    // No active enforcements
    when(manager.copyActiveEnforcements()).thenReturn(Collections.emptyMap());
    // Policies to enforce
    when(manager.getViolationsToEnforce()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceQuotaSnapshot> entry : policiesToEnforce.entrySet()) {
      // Ensure we enforce the policy
      verify(manager).enforceViolationPolicy(entry.getKey(), entry.getValue());
      // Don't disable any policies
      verify(manager, never()).disableViolationPolicyEnforcement(entry.getKey());
    }
  }

  @Test
  public void testOldPoliciesAreRemoved() throws IOException {
    final Map<TableName,SpaceQuotaSnapshot> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(TableName.valueOf("table1"), new SpaceQuotaSnapshot(SpaceViolationPolicy.DISABLE, 1024L, 512L));
    policiesToEnforce.put(TableName.valueOf("table2"), new SpaceQuotaSnapshot(SpaceViolationPolicy.NO_INSERTS, 2048L, 512L));
    final Map<TableName,SpaceQuotaSnapshot> previousPolicies = new HashMap<>();
    previousPolicies.put(TableName.valueOf("table3"), new SpaceQuotaSnapshot(SpaceViolationPolicy.NO_WRITES, 4096L, 512L));
    previousPolicies.put(TableName.valueOf("table4"), new SpaceQuotaSnapshot(SpaceViolationPolicy.NO_WRITES, 8192L, 512L));

    // No active enforcements
    when(manager.getActivePoliciesAsMap()).thenReturn(previousPolicies);
    // Policies to enforce
    when(manager.getViolationsToEnforce()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceQuotaSnapshot> entry : policiesToEnforce.entrySet()) {
      verify(manager).enforceViolationPolicy(entry.getKey(), entry.getValue());
    }

    for (Entry<TableName,SpaceQuotaSnapshot> entry : previousPolicies.entrySet()) {
      verify(manager).disableViolationPolicyEnforcement(entry.getKey());
    }
  }

  @Test
  public void testNewPolicyOverridesOld() throws IOException {
    final Map<TableName,SpaceQuotaSnapshot> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(TableName.valueOf("table1"), new SpaceQuotaSnapshot(SpaceViolationPolicy.DISABLE, 1024L, 512L));
    policiesToEnforce.put(TableName.valueOf("table2"), new SpaceQuotaSnapshot(SpaceViolationPolicy.NO_WRITES, 2048L, 512L));
    policiesToEnforce.put(TableName.valueOf("table3"), new SpaceQuotaSnapshot(SpaceViolationPolicy.NO_INSERTS, 4096L, 512L));

    final Map<TableName,SpaceQuotaSnapshot> previousPolicies = new HashMap<>();
    previousPolicies.put(TableName.valueOf("table1"), new SpaceQuotaSnapshot(SpaceViolationPolicy.NO_WRITES, 8192L, 512L));

    // No active enforcements
    when(manager.getActivePoliciesAsMap()).thenReturn(previousPolicies);
    // Policies to enforce
    when(manager.getViolationsToEnforce()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceQuotaSnapshot> entry : policiesToEnforce.entrySet()) {
      verify(manager).enforceViolationPolicy(entry.getKey(), entry.getValue());
    }
    verify(manager, never()).disableViolationPolicyEnforcement(TableName.valueOf("table1"));
  }

  @Test
  public void testFilterNullPoliciesFromEnforcements() {
    final Map<TableName, SpaceViolationPolicyEnforcement> enforcements = new HashMap<>();
    final Map<TableName, SpaceQuotaSnapshot> expectedPolicies = new HashMap<>();
    when(manager.copyActiveEnforcements()).thenReturn(enforcements);
    when(manager.getActivePoliciesAsMap()).thenCallRealMethod();

    NoInsertsViolationPolicyEnforcement noInsertsPolicy =
        new NoInsertsViolationPolicyEnforcement();
    SpaceQuotaSnapshot noInsertsSnapshot = new SpaceQuotaSnapshot(
        SpaceViolationPolicy.NO_INSERTS, 1024L, 512L);
    noInsertsPolicy.initialize(rss, TableName.valueOf("no_inserts"), noInsertsSnapshot);
    enforcements.put(noInsertsPolicy.getTableName(), noInsertsPolicy);
    expectedPolicies.put(noInsertsPolicy.getTableName(), noInsertsSnapshot);

    NoWritesViolationPolicyEnforcement noWritesPolicy = new NoWritesViolationPolicyEnforcement();
    SpaceQuotaSnapshot noWritesSnapshot = new SpaceQuotaSnapshot(
        SpaceViolationPolicy.NO_WRITES, 2048L, 512L);
    noWritesPolicy.initialize(rss, TableName.valueOf("no_writes"), noWritesSnapshot);
    enforcements.put(noWritesPolicy.getTableName(), noWritesPolicy);
    expectedPolicies.put(noWritesPolicy.getTableName(), noWritesSnapshot);

    NoWritesCompactionsViolationPolicyEnforcement noWritesCompactionsPolicy =
        new NoWritesCompactionsViolationPolicyEnforcement();
    SpaceQuotaSnapshot noWritesCompactionsSnapshot = new SpaceQuotaSnapshot(
        SpaceViolationPolicy.NO_WRITES_COMPACTIONS, 4096L, 512L);
    noWritesCompactionsPolicy.initialize(
        rss, TableName.valueOf("no_writes_compactions"), noWritesCompactionsSnapshot);
    enforcements.put(noWritesCompactionsPolicy.getTableName(), noWritesCompactionsPolicy);
    expectedPolicies.put(noWritesCompactionsPolicy.getTableName(),
        noWritesCompactionsSnapshot);

    DisableTableViolationPolicyEnforcement disablePolicy =
        new DisableTableViolationPolicyEnforcement();
    SpaceQuotaSnapshot disableSnapshot = new SpaceQuotaSnapshot(
        SpaceViolationPolicy.DISABLE, 8192L, 512L);
    disablePolicy.initialize(rss, TableName.valueOf("disable"), disableSnapshot);
    enforcements.put(disablePolicy.getTableName(), disablePolicy);
    expectedPolicies.put(disablePolicy.getTableName(), disableSnapshot);

    // This is excluded because the policy enforcement has no quota snapshot
    enforcements.put(TableName.valueOf("no_policy"), NoopViolationPolicyEnforcement.getInstance());

    Map<TableName, SpaceQuotaSnapshot> actualPolicies = manager.getActivePoliciesAsMap();
    assertEquals(expectedPolicies, actualPolicies);
  }
}
