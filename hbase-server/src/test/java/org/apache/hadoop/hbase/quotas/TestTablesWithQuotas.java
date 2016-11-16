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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore.TablesWithQuotas;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;

/**
 * Non-HBase cluster unit tests for {@link TablesWithQuotas}.
 */
@Category(SmallTests.class)
public class TestTablesWithQuotas {
  private QuotaObserverChore chore;

  @Before
  public void setup() throws Exception {
    chore = mock(QuotaObserverChore.class);
  }
  
  @Test
  public void testFilterRegionSpaceReportsByTableName() throws Exception {
    when(chore.filterByTable(any(Map.class), any(TableName.class))).thenCallRealMethod();

    final Map<HRegionInfo, Long> regions = new HashMap<>();
    // Create regions for tables 0-9
    for (int i = 0; i < 10; i++) {
      regions.put(new HRegionInfo(TableName.valueOf(Integer.toString(i)),
          Bytes.toBytes(i), Bytes.toBytes(i+1)), 0L);
    }
    for (int i = 0; i < 10; i++) {
      final TableName tableName = TableName.valueOf(Integer.toString(i));
      Entry<HRegionInfo,Long> entry = Iterables.getOnlyElement(
          chore.filterByTable(regions, tableName));
      assertEquals(tableName, entry.getKey().getTable());
    }
  }

  @Test
  public void testImmutableGetters() {
    Set<TableName> tablesWithTableQuotas = new HashSet<>();
    Set<TableName> tablesWithNamespaceQuotas = new HashSet<>();
    final TablesWithQuotas tables = new TablesWithQuotas(chore);
    for (int i = 0; i < 5; i++) {
      TableName tn = TableName.valueOf("tn" + i);
      tablesWithTableQuotas.add(tn);
      tables.addTableQuotaTable(tn);
    }
    for (int i = 0; i < 3; i++) {
      TableName tn = TableName.valueOf("tn_ns" + i);
      tablesWithNamespaceQuotas.add(tn);
      tables.addNamespaceQuotaTable(tn);
    }
    Set<TableName> actualTableQuotaTables = tables.getTableQuotaTables();
    Set<TableName> actualNamespaceQuotaTables = tables.getNamespaceQuotaTables();
    assertEquals(tablesWithTableQuotas, actualTableQuotaTables);
    assertEquals(tablesWithNamespaceQuotas, actualNamespaceQuotaTables);
    try {
      actualTableQuotaTables.add(null);
      fail("Should not be able to add an element");
    } catch (UnsupportedOperationException e) {
      // pass
    }
    try {
      actualNamespaceQuotaTables.add(null);
      fail("Should not be able to add an element");
    } catch (UnsupportedOperationException e) {
      // pass
    }
  }

  @Test
  public void testRegionsForTable() {
    
  }
}
