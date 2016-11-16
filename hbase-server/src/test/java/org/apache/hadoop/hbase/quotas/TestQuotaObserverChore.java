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
import static org.mockito.Mockito.*;
import static com.google.common.collect.Iterables.size;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore.ViolationState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceViolationPolicy;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Non-HBase cluster unit tests for {@link QuotaObserverChore}.
 */
@Category(SmallTests.class)
public class TestQuotaObserverChore {
  private static final long ONE_MEGABYTE = 1024L * 1024L;
  private QuotaObserverChore chore;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws Exception {
    chore = mock(QuotaObserverChore.class);
    // Set up some rules to call the real method on the mock.
    when(chore.getTargetViolationState(any(TableName.class), any(SpaceQuota.class),
        any(Map.class))).thenCallRealMethod();
    when(chore.filterByTable(any(Map.class), any(TableName.class))).thenCallRealMethod();
    when(chore.numRegionsForTable(any(Map.class), any(TableName.class))).thenCallRealMethod();
    when(chore.getSpaceQuotaForTable(any(TableName.class))).thenCallRealMethod();
    when(chore.getSpaceQuotaForNamespace(any(String.class))).thenCallRealMethod();
    when(chore.buildNamespaceSpaceUtilization(any(Set.class), any(Map.class)))
        .thenCallRealMethod();
  }

  @Test
  public void testFilterRegionsByTable() throws Exception {
    TableName tn1 = TableName.valueOf("foo");
    TableName tn2 = TableName.valueOf("bar");
    TableName tn3 = TableName.valueOf("ns", "foo");
    Map<HRegionInfo, Long> regions = new HashMap<>();
    assertEquals(0, size(chore.filterByTable(regions, tn1)));
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
    assertEquals(5, size(chore.filterByTable(regions, tn1)));
    assertEquals(3, size(chore.filterByTable(regions, tn2)));
    assertEquals(10, size(chore.filterByTable(regions, tn3)));
  }

  @Test
  public void testTargetViolationState() {
    TableName tn1 = TableName.valueOf("violation1");
    TableName tn2 = TableName.valueOf("observance1");
    TableName tn3 = TableName.valueOf("observance2");
    SpaceQuota quota = SpaceQuota.newBuilder()
        .setSoftLimit(1024L * 1024L)
        .setViolationPolicy(SpaceViolationPolicy.DISABLE)
        .build();

    Map<HRegionInfo,Long> regionReports = new HashMap<>();
    // Create some junk data to filter. Makes sure it's so large that it would
    // immediately violate the quota.
    for (int i = 0; i < 3; i++) {
      regionReports.put(new HRegionInfo(tn2, Bytes.toBytes(i), Bytes.toBytes(i + 1)),
          5L * ONE_MEGABYTE);
      regionReports.put(new HRegionInfo(tn3, Bytes.toBytes(i), Bytes.toBytes(i + 1)),
          5L * ONE_MEGABYTE);
    }

    regionReports.put(new HRegionInfo(tn1, Bytes.toBytes(0), Bytes.toBytes(1)), 1024L * 512L);
    regionReports.put(new HRegionInfo(tn1, Bytes.toBytes(1), Bytes.toBytes(2)), 1024L * 256L);

    // Below the quota
    assertEquals(ViolationState.IN_OBSERVANCE,
        chore.getTargetViolationState(tn1, quota, regionReports));

    regionReports.put(new HRegionInfo(tn1, Bytes.toBytes(2), Bytes.toBytes(3)), 1024L * 256L);

    // Equal to the quota is still in observance
    assertEquals(ViolationState.IN_OBSERVANCE,
        chore.getTargetViolationState(tn1, quota, regionReports));

    regionReports.put(new HRegionInfo(tn1, Bytes.toBytes(3), Bytes.toBytes(4)), 1024L);

    // Exceeds the quota, should be in violation
    assertEquals(ViolationState.IN_VIOLATION,
        chore.getTargetViolationState(tn1, quota, regionReports));
  }

  @Test
  public void testNumRegionsForTable() {
    TableName tn1 = TableName.valueOf("t1");
    TableName tn2 = TableName.valueOf("t2");
    TableName tn3 = TableName.valueOf("t3");

    final int numTable1Regions = 10;
    final int numTable2Regions = 15;
    final int numTable3Regions = 8;
    Map<HRegionInfo,Long> regionReports = new HashMap<>();
    for (int i = 0; i < numTable1Regions; i++) {
      regionReports.put(new HRegionInfo(tn1, Bytes.toBytes(i), Bytes.toBytes(i + 1)), 0L);
    }

    for (int i = 0; i < numTable2Regions; i++) {
      regionReports.put(new HRegionInfo(tn2, Bytes.toBytes(i), Bytes.toBytes(i + 1)), 0L);
    }

    for (int i = 0; i < numTable3Regions; i++) {
      regionReports.put(new HRegionInfo(tn3, Bytes.toBytes(i), Bytes.toBytes(i + 1)), 0L);
    }

    assertEquals(numTable1Regions, chore.numRegionsForTable(regionReports, tn1));
    assertEquals(numTable2Regions, chore.numRegionsForTable(regionReports, tn2));
    assertEquals(numTable3Regions, chore.numRegionsForTable(regionReports, tn3));
  }

  @Test
  public void testQuotaInheritance() throws Exception {
    final String ns1 = "ns1";
    final TableName t1 = TableName.valueOf(ns1, "t1");
    final AtomicReference<Quotas> tableQuota = new AtomicReference<>(null);
    Answer<Quotas> tableQuotaAnswer = new Answer<Quotas>() {
      @Override
      public Quotas answer(InvocationOnMock invocation) throws Throwable {
        assertEquals(1, invocation.getArguments().length);
        TableName tn = invocation.getArgumentAt(0, TableName.class);
        assertEquals(t1, tn);
        return tableQuota.get();
      }
    };
    final AtomicReference<Quotas> namespaceQuota = new AtomicReference<>(null);
    Answer<Quotas> namespaceQuotaAnswer = new Answer<Quotas>() {
      @Override
      public Quotas answer(InvocationOnMock invocation) throws Throwable {
        assertEquals(1, invocation.getArguments().length);
        String namespace = invocation.getArgumentAt(0, String.class);
        assertEquals(ns1, namespace);
        return namespaceQuota.get();
      }
    };

    when(chore.getQuotaForTable(any(TableName.class))).thenAnswer(tableQuotaAnswer);
    when(chore.getQuotaForNamespace(any(String.class))).thenAnswer(namespaceQuotaAnswer);

    final Quotas disableQuota = Quotas.newBuilder().setSpace(
            SpaceQuota.newBuilder()
                .setSoftLimit(1024L)
                .setViolationPolicy(SpaceViolationPolicy.DISABLE)
                .build())
        .build();
    final Quotas noWritesQuota = Quotas.newBuilder().setSpace(
            SpaceQuota.newBuilder()
                .setSoftLimit(2048L)
                .setViolationPolicy(SpaceViolationPolicy.NO_WRITES)
                .build())
        .build();

    tableQuota.set(null);
    namespaceQuota.set(null);

    // No table or ns quota
    assertNull(chore.getSpaceQuotaForTable(t1));
    assertNull(chore.getSpaceQuotaForNamespace(ns1));

    tableQuota.set(disableQuota);
    namespaceQuota.set(null);

    // Table quota only
    assertEquals(disableQuota.getSpace(), chore.getSpaceQuotaForTable(t1));
    assertNull(chore.getSpaceQuotaForNamespace(ns1));

    tableQuota.set(null);
    namespaceQuota.set(noWritesQuota);

    // No table quota falls back to NS quota
    assertEquals(noWritesQuota.getSpace(), chore.getSpaceQuotaForTable(t1));
    assertEquals(noWritesQuota.getSpace(), chore.getSpaceQuotaForNamespace(ns1));

    tableQuota.set(noWritesQuota);
    namespaceQuota.set(disableQuota);

    // Table quota takes priority over namespace, but both are present
    assertEquals(noWritesQuota.getSpace(), chore.getSpaceQuotaForTable(t1));
    assertEquals(disableQuota.getSpace(), chore.getSpaceQuotaForNamespace(ns1));
  }

  @Test
  public void testNamespaceUtilizationMap() {
    String namespace1 = "ns1";
    String namespace2 = "ns2";
    String namespace3 = "ns3";
    TableName tn1 = TableName.valueOf("table1");
    TableName tn2 = TableName.valueOf("table2");
    Map<String,Long> expectedNamespaceSizes = new HashMap<>();
    // Some regions for the "default" namespace.
    Map<HRegionInfo,Long> reportedRegionSpaceUse = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      reportedRegionSpaceUse.put(new HRegionInfo(tn1, Bytes.toBytes(i), Bytes.toBytes(i + 1)),
          ONE_MEGABYTE);
      setOrAdd(expectedNamespaceSizes, tn1.getNamespaceAsString(), ONE_MEGABYTE);
      reportedRegionSpaceUse.put(new HRegionInfo(tn2, Bytes.toBytes(i), Bytes.toBytes(i + 1)),
          ONE_MEGABYTE);
      setOrAdd(expectedNamespaceSizes, tn1.getNamespaceAsString(), ONE_MEGABYTE);
    }
    Set<TableName> namespaceTables = new HashSet<>();
    for (String ns : Arrays.asList(namespace1, namespace2, namespace3)) {
      for (int i = 0; i < 3; i++) {
        TableName tn = TableName.valueOf(ns, "table" + i);
        namespaceTables.add(tn);
        long sizeUse = ONE_MEGABYTE * (i + 1);
        reportedRegionSpaceUse.put(new HRegionInfo(tn, Bytes.toBytes(i), Bytes.toBytes(i + 1)),
            sizeUse);
        setOrAdd(expectedNamespaceSizes, tn.getNamespaceAsString(), sizeUse);
      }
    }

    // Avoid the default namespace
    Map<String,Long> actualUse = chore.buildNamespaceSpaceUtilization(namespaceTables,
        reportedRegionSpaceUse);
    Map<String,Long> explicitNamespacesOnly = new HashMap<>(expectedNamespaceSizes);
    explicitNamespacesOnly.remove(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
    assertEquals(explicitNamespacesOnly, actualUse);

    // Should be able to get all of the namespaces
    namespaceTables.add(tn1);
    namespaceTables.add(tn2);
    Map<String,Long> allNamespaces = chore.buildNamespaceSpaceUtilization(namespaceTables,
        reportedRegionSpaceUse);
    assertEquals(expectedNamespaceSizes, allNamespaces);

    // No results when we provide no tables with namespace quotas
    assertEquals(0, chore.buildNamespaceSpaceUtilization(Collections.emptySet(),
        reportedRegionSpaceUse).size());
    // All results are zero
    Map<String,Long> allNamespacesAreZero = chore.buildNamespaceSpaceUtilization(namespaceTables,
        Collections.emptyMap());
    assertEquals(4, allNamespacesAreZero.size());
    for (Entry<String,Long> entry : allNamespacesAreZero.entrySet()) {
      assertEquals("Unexpected value for " + entry.getKey(), Long.valueOf(0L), entry.getValue());
    }

    // Remove all but namespace1 and namespace3
    namespaceTables.remove(tn1);
    namespaceTables.remove(tn2);
    Iterator<TableName> iter = namespaceTables.iterator();
    while (iter.hasNext()) {
      TableName tn = iter.next();
      if (namespace2.equals(tn.getNamespaceAsString())) {
        iter.remove();
      }
    }
    Map<String,Long> reducedNamespaces = chore.buildNamespaceSpaceUtilization(namespaceTables,
        reportedRegionSpaceUse);
    Map<String,Long> expectedReducedNamespaces = new HashMap<>(expectedNamespaceSizes);
    expectedReducedNamespaces.remove(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
    expectedReducedNamespaces.remove(namespace2);
    assertEquals(expectedReducedNamespaces, reducedNamespaces);
  }

  private void setOrAdd(Map<String,Long> namespaceSizes, String namespace, long newSize) {
    Long currentSize = namespaceSizes.get(namespace);
    if (null == currentSize) {
      currentSize = 0L;
    }
    namespaceSizes.put(namespace, currentSize + newSize);
  }
}