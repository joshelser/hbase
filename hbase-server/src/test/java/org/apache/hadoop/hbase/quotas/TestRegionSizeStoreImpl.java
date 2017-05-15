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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestRegionSizeStoreImpl {

  private static final HRegionInfo INFOA = new HRegionInfo(
      TableName.valueOf("TEST"), Bytes.toBytes("a"), Bytes.toBytes("b"));
  private static final HRegionInfo INFOB = new HRegionInfo(
      TableName.valueOf("TEST"), Bytes.toBytes("b"), Bytes.toBytes("c"));

  @Test
  public void testSizeUpdates() {
    RegionSizeStore store = new RegionSizeStoreImpl();
    assertTrue(store.isEmpty());
    assertEquals(0, store.size());

    store.put(INFOA, 1024L);

    assertFalse(store.isEmpty());
    assertEquals(1, store.size());
    assertEquals(1024L, store.getRegionSize(INFOA).getSize());

    store.put(INFOA, 2048L);
    assertEquals(1, store.size());
    assertEquals(2048L, store.getRegionSize(INFOA).getSize());

    store.incrementRegionSize(INFOA, 512L);
    assertEquals(1, store.size());
    assertEquals(2048L + 512L, store.getRegionSize(INFOA).getSize());

    store.remove(INFOA);
    assertTrue(store.isEmpty());
    assertEquals(0, store.size());

    store.put(INFOA, 64L);
    store.put(INFOB, 128L);

    assertEquals(2, store.size());
    Map<HRegionInfo,RegionSize> records = new HashMap<>();
    for (Entry<HRegionInfo,RegionSize> entry : store) {
      records.put(entry.getKey(), entry.getValue());
    }

    assertEquals(64L, records.remove(INFOA).getSize());
    assertEquals(128L, records.remove(INFOB).getSize());
    assertTrue(records.isEmpty());
  }
}
