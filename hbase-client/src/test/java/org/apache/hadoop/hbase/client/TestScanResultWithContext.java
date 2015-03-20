/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for ScanResultWithContext
 */
@Category(SmallTests.class)
public class TestScanResultWithContext {

  @Test
  public void testGetters() {
    KeyValue kv1 = new KeyValue("row1".getBytes(), "cf".getBytes(), "cq".getBytes(), 1, Type.Maximum);
    Result[] results = new Result[] {Result.create(new Cell[] {kv1})};

    ScanResultWithContext ctx1 = new ScanResultWithContext(results);

    assertTrue(results == ctx1.getResults());

    ScanResultWithContext ctx2 = new ScanResultWithContext(results, false);
    ScanResultWithContext ctx3 = new ScanResultWithContext(results, true);

    assertTrue(ctx2.getHasMoreResultsContext());
    assertFalse(ctx2.getServerHasMoreResults());

    assertTrue(ctx3.getHasMoreResultsContext());
    assertTrue(ctx3.getServerHasMoreResults());
  }

}
