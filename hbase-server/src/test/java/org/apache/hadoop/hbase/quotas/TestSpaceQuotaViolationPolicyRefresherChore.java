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
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceViolation;
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
    final Map<TableName,SpaceViolation> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(TableName.valueOf("table1"), 
        SpaceViolation.newBuilder().setPolicy(
            ProtobufUtil.toProtoViolationPolicy(SpaceViolationPolicy.DISABLE))
        .setUsage(1024L)
        .setLimit(512L)
        .build());
    policiesToEnforce.put(TableName.valueOf("table2"), 
        SpaceViolation.newBuilder().setPolicy(
            ProtobufUtil.toProtoViolationPolicy(SpaceViolationPolicy.NO_INSERTS))
        .setUsage(1024L)
        .setLimit(512L)
        .build());
    policiesToEnforce.put(TableName.valueOf("table3"),
        SpaceViolation.newBuilder().setPolicy(
            ProtobufUtil.toProtoViolationPolicy(SpaceViolationPolicy.NO_WRITES))
        .setUsage(1024L)
        .setLimit(512L)
        .build());
    policiesToEnforce.put(TableName.valueOf("table4"),
        SpaceViolation.newBuilder().setPolicy(
            ProtobufUtil.toProtoViolationPolicy(SpaceViolationPolicy.NO_WRITES_COMPACTIONS))
        .setUsage(1024L)
        .setLimit(512L)
        .build());

    // No active enforcements
    when(manager.copyActiveEnforcements()).thenReturn(Collections.emptyMap());
    // Policies to enforce
    when(manager.getViolationsToEnforce()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceViolation> entry : policiesToEnforce.entrySet()) {
      // Ensure we enforce the policy
      verify(manager).enforceViolationPolicy(entry.getKey(), ProtobufUtil.toViolationPolicy(entry.getValue().getPolicy()));
      // Don't disable any policies
      verify(manager, never()).disableViolationPolicyEnforcement(entry.getKey());
    }
  }

  @Test
  public void testOldPoliciesAreRemoved() throws IOException {
    final Map<TableName,SpaceViolation> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(TableName.valueOf("table1"),
        SpaceViolation.newBuilder().setPolicy(
            ProtobufUtil.toProtoViolationPolicy(SpaceViolationPolicy.DISABLE))
        .setUsage(1024L)
        .setLimit(512L)
        .build());
    policiesToEnforce.put(TableName.valueOf("table2"),
        SpaceViolation.newBuilder().setPolicy(
            ProtobufUtil.toProtoViolationPolicy(SpaceViolationPolicy.NO_INSERTS))
        .setUsage(1024L)
        .setLimit(512L)
        .build());
    final Map<TableName,SpaceViolationPolicy> previousPolicies = new HashMap<>();
    previousPolicies.put(TableName.valueOf("table3"), SpaceViolationPolicy.NO_WRITES);
    previousPolicies.put(TableName.valueOf("table4"), SpaceViolationPolicy.NO_WRITES);

    // No active enforcements
    when(manager.getActivePoliciesAsMap()).thenReturn(previousPolicies);
    // Policies to enforce
    when(manager.getViolationsToEnforce()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceViolation> entry : policiesToEnforce.entrySet()) {
      verify(manager).enforceViolationPolicy(entry.getKey(), ProtobufUtil.toViolationPolicy(entry.getValue().getPolicy()));
    }

    for (Entry<TableName,SpaceViolationPolicy> entry : previousPolicies.entrySet()) {
      verify(manager).disableViolationPolicyEnforcement(entry.getKey());
    }
  }

  @Test
  public void testNewPolicyOverridesOld() throws IOException {
    final Map<TableName,SpaceViolation> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(TableName.valueOf("table1"),
        SpaceViolation.newBuilder().setPolicy(
            ProtobufUtil.toProtoViolationPolicy(SpaceViolationPolicy.DISABLE))
            .setUsage(1024L)
            .setLimit(512L)
            .build());
    policiesToEnforce.put(TableName.valueOf("table2"),
        SpaceViolation.newBuilder().setPolicy(
            ProtobufUtil.toProtoViolationPolicy(SpaceViolationPolicy.NO_WRITES))
        .setUsage(1024L)
        .setLimit(512L)
        .build());
    policiesToEnforce.put(TableName.valueOf("table3"),
        SpaceViolation.newBuilder().setPolicy(
            ProtobufUtil.toProtoViolationPolicy(SpaceViolationPolicy.NO_INSERTS))
        .setUsage(1024L)
        .setLimit(512L)
        .build());

    final Map<TableName,SpaceViolationPolicy> previousPolicies = new HashMap<>();
    previousPolicies.put(TableName.valueOf("table1"), SpaceViolationPolicy.NO_WRITES);

    // No active enforcements
    when(manager.getActivePoliciesAsMap()).thenReturn(previousPolicies);
    // Policies to enforce
    when(manager.getViolationsToEnforce()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceViolation> entry : policiesToEnforce.entrySet()) {
      verify(manager).enforceViolationPolicy(entry.getKey(), ProtobufUtil.toViolationPolicy(entry.getValue().getPolicy()));
    }
    verify(manager, never()).disableViolationPolicyEnforcement(TableName.valueOf("table1"));
  }

  @Test
  public void testFilterNullPoliciesFromEnforcements() {
    final Map<TableName, SpaceViolationPolicyEnforcement> enforcements = new HashMap<>();
    final Map<TableName, SpaceViolationPolicy> expectedPolicies = new HashMap<>();
    when(manager.copyActiveEnforcements()).thenReturn(enforcements);
    when(manager.getActivePoliciesAsMap()).thenCallRealMethod();

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

    Map<TableName, SpaceViolationPolicy> actualPolicies = manager.getActivePoliciesAsMap();
    assertEquals(expectedPolicies, actualPolicies);
  }
}
