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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test the ClientScanner.
 */
@Category(SmallTests.class)
public class TestClientScanner {

  Scan scan;
  ExecutorService pool;
  Configuration conf;

  ClusterConnection clusterConn;
  RpcRetryingCallerFactory rpcFactory;
  RpcControllerFactory controllerFactory;

  @Before
  @SuppressWarnings("deprecation")
  public void setup() throws IOException {
    clusterConn = Mockito.mock(ClusterConnection.class);
    rpcFactory = Mockito.mock(RpcRetryingCallerFactory.class);
    controllerFactory = Mockito.mock(RpcControllerFactory.class);
    pool = Executors.newSingleThreadExecutor();
    scan = new Scan();
    conf = new Configuration();
    Mockito.when(clusterConn.getConfiguration()).thenReturn(conf);
  }

  @After
  public void teardown() {
    if (null != pool) {
      pool.shutdownNow();
    }
  }

  private static class MockClientScanner extends ClientScanner {

    private boolean rpcFinished = false;
    private boolean rpcFinishedFired = false;

    public MockClientScanner(final Configuration conf, final Scan scan, final TableName tableName,
        ClusterConnection connection, RpcRetryingCallerFactory rpcFactory,
        RpcControllerFactory controllerFactory, ExecutorService pool, int primaryOperationTimeout)
        throws IOException {
      super(conf, scan, tableName, connection, rpcFactory, controllerFactory, pool,
          primaryOperationTimeout);
    }

    @Override
    protected boolean nextScanner(int nbRows, final boolean done) throws IOException {
      if (!rpcFinished) {
        return super.nextScanner(nbRows, done);
      }

      // Enforce that we don't short-circuit more than once
      if (rpcFinishedFired) {
        throw new RuntimeException("Expected nextScanner to only be called once after " +
            " short-circuit was triggered.");
      }
      rpcFinishedFired = true;
      return false;
    }

    public void setRpcFinished(boolean rpcFinished) {
      this.rpcFinished = rpcFinished;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNoResultsHint() throws IOException {
    Result[] results = new Result[1];
    KeyValue kv1 = new KeyValue("row".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum);
    results[0] = Result.create(new Cell[] {kv1});
    final ScanResultWithContext resultWithContext1 = new ScanResultWithContext(results);

    RpcRetryingCaller<ScanResultWithContext> caller = Mockito.mock(RpcRetryingCaller.class);

    Mockito.when(rpcFactory.<ScanResultWithContext> newCaller()).thenReturn(caller);
    Mockito.when(caller.callWithoutRetries(Mockito.any(RetryingCallable.class),
      Mockito.anyInt())).thenAnswer(new Answer<ScanResultWithContext>() {
        private int count = 0;
        @Override
        public ScanResultWithContext answer(InvocationOnMock invocation) throws Throwable {
          switch (count) {
            case 0: // initialize
            case 2: // close
              count++;
              return null;
            case 1:
              count++;
              return resultWithContext1;
            default:
              throw new RuntimeException("Expected only 2 invocations");
          }
        }
    });

    // Set a much larger cache and buffer size than we'll provide
    scan.setCaching(100);
    scan.setMaxResultSize(1000*1000);

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf("table"),
        clusterConn, rpcFactory, controllerFactory, pool, Integer.MAX_VALUE)) {

      scanner.setRpcFinished(true);

      InOrder inOrder = Mockito.inOrder(caller);

      scanner.loadCache();

      // One more call due to initializeScannerInConstruction()
      inOrder.verify(caller, Mockito.times(2)).callWithoutRetries(
          Mockito.any(RetryingCallable.class), Mockito.anyInt());

      assertEquals(1, scanner.cache.size());
      Result r = scanner.cache.poll();
      assertNotNull(r);
      CellScanner cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv1, cs.current());
      assertFalse(cs.advance());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSizeLimit() throws IOException {
    Result[] results = new Result[1];
    KeyValue kv1 = new KeyValue("row".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum);
    results[0] = Result.create(new Cell[] {kv1});
    final ScanResultWithContext resultWithContext1 = new ScanResultWithContext(results, false);

    RpcRetryingCaller<ScanResultWithContext> caller = Mockito.mock(RpcRetryingCaller.class);

    Mockito.when(rpcFactory.<ScanResultWithContext> newCaller()).thenReturn(caller);
    Mockito.when(caller.callWithoutRetries(Mockito.any(RetryingCallable.class),
      Mockito.anyInt())).thenAnswer(new Answer<ScanResultWithContext>() {
        private int count = 0;
        @Override
        public ScanResultWithContext answer(InvocationOnMock invocation) throws Throwable {
          switch (count) {
            case 0: // initialize
            case 2: // close
              count++;
              return null;
            case 1:
              count++;
              return resultWithContext1;
            default:
              throw new RuntimeException("Expected only 2 invocations");
          }
        }
    });

    Mockito.when(rpcFactory.<ScanResultWithContext> newCaller()).thenReturn(caller);

    // Set a much larger cache
    scan.setCaching(100);
    // The single key-value will exit the loop
    scan.setMaxResultSize(1);

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf("table"),
        clusterConn, rpcFactory, controllerFactory, pool, Integer.MAX_VALUE)) {

      // Due to initializeScannerInConstruction()
      Mockito.verify(caller).callWithoutRetries(Mockito.any(RetryingCallable.class),
          Mockito.anyInt());

      InOrder inOrder = Mockito.inOrder(caller);

      scanner.loadCache();

      inOrder.verify(caller, Mockito.times(2)).callWithoutRetries(
          Mockito.any(RetryingCallable.class), Mockito.anyInt());

      assertEquals(1, scanner.cache.size());
      Result r = scanner.cache.poll();
      assertNotNull(r);
      CellScanner cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv1, cs.current());
      assertFalse(cs.advance());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCacheLimit() throws IOException {
    KeyValue kv1 = new KeyValue("row1".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum), kv2 = new KeyValue("row2".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum), kv3 = new KeyValue("row3".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum);
    Result[] results = new Result[] {Result.create(new Cell[] {kv1}),
        Result.create(new Cell[] {kv2}), Result.create(new Cell[] {kv3})};
    final ScanResultWithContext resultWithContext1 = new ScanResultWithContext(results, false);

    RpcRetryingCaller<ScanResultWithContext> caller = Mockito.mock(RpcRetryingCaller.class);

    Mockito.when(rpcFactory.<ScanResultWithContext> newCaller()).thenReturn(caller);
    Mockito.when(caller.callWithoutRetries(Mockito.any(RetryingCallable.class),
      Mockito.anyInt())).thenAnswer(new Answer<ScanResultWithContext>() {
        private int count = 0;
        @Override
        public ScanResultWithContext answer(InvocationOnMock invocation) throws Throwable {
          switch (count) {
            case 0: // initialize
            case 2: // close
              count++;
              return null;
            case 1:
              count++;
              return resultWithContext1;
            default:
              throw new RuntimeException("Expected only 2 invocations");
          }
        }
    });

    Mockito.when(rpcFactory.<ScanResultWithContext> newCaller()).thenReturn(caller);

    // Set a small cache
    scan.setCaching(1);
    // Set a very large size
    scan.setMaxResultSize(1000*1000);

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf("table"),
        clusterConn, rpcFactory, controllerFactory, pool, Integer.MAX_VALUE)) {

      // Due to initializeScannerInConstruction()
      Mockito.verify(caller).callWithoutRetries(Mockito.any(RetryingCallable.class),
          Mockito.anyInt());

      InOrder inOrder = Mockito.inOrder(caller);

      scanner.loadCache();

      // Ensures that possiblyNextScanner isn't called at the end which would trigger
      // another call to callWithoutRetries
      inOrder.verify(caller, Mockito.times(2)).callWithoutRetries(
          Mockito.any(RetryingCallable.class), Mockito.anyInt());

      assertEquals(3, scanner.cache.size());
      Result r = scanner.cache.poll();
      assertNotNull(r);
      CellScanner cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv1, cs.current());
      assertFalse(cs.advance());

      r = scanner.cache.poll();
      assertNotNull(r);
      cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv2, cs.current());
      assertFalse(cs.advance());

      r = scanner.cache.poll();
      assertNotNull(r);
      cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv3, cs.current());
      assertFalse(cs.advance());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNoMoreResults() throws IOException {
    Result[] results = new Result[1];
    KeyValue kv1 = new KeyValue("row".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum);
    results[0] = Result.create(new Cell[] {kv1});
    final ScanResultWithContext resultWithContext1 = new ScanResultWithContext(results, false);

    RpcRetryingCaller<ScanResultWithContext> caller = Mockito.mock(RpcRetryingCaller.class);

    Mockito.when(rpcFactory.<ScanResultWithContext> newCaller()).thenReturn(caller);
    Mockito.when(caller.callWithoutRetries(Mockito.any(RetryingCallable.class),
      Mockito.anyInt())).thenAnswer(new Answer<ScanResultWithContext>() {
        private int count = 0;
        @Override
        public ScanResultWithContext answer(InvocationOnMock invocation) throws Throwable {
          switch (count) {
            case 0: // initialize
            case 2: // close
              count++;
              return null;
            case 1:
              count++;
              return resultWithContext1;
            default:
              throw new RuntimeException("Expected only 2 invocations");
          }
        }
    });

    Mockito.when(rpcFactory.<ScanResultWithContext> newCaller()).thenReturn(caller);

    // Set a much larger cache and buffer size than we'll provide
    scan.setCaching(100);
    scan.setMaxResultSize(1000*1000);

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf("table"),
        clusterConn, rpcFactory, controllerFactory, pool, Integer.MAX_VALUE)) {

      // Due to initializeScannerInConstruction()
      Mockito.verify(caller).callWithoutRetries(Mockito.any(RetryingCallable.class),
          Mockito.anyInt());

      scanner.setRpcFinished(true);

      InOrder inOrder = Mockito.inOrder(caller);

      scanner.loadCache();

      inOrder.verify(caller, Mockito.times(2)).callWithoutRetries(
          Mockito.any(RetryingCallable.class), Mockito.anyInt());

      assertEquals(1, scanner.cache.size());
      Result r = scanner.cache.poll();
      assertNotNull(r);
      CellScanner cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv1, cs.current());
      assertFalse(cs.advance());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMoreResults() throws IOException {
    Result[] results = new Result[1];
    KeyValue kv1 = new KeyValue("row".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum);
    results[0] = Result.create(new Cell[] {kv1});
    final ScanResultWithContext resultWithContext1 = new ScanResultWithContext(results, true);

    results = new Result[1];
    KeyValue kv2 = new KeyValue("row2".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum);
    results[0] = Result.create(new Cell[] {kv2});

    // The server reports back nothing WRT more results
    final ScanResultWithContext resultWithContext2 = new ScanResultWithContext(results, false);

    RpcRetryingCaller<ScanResultWithContext> caller = Mockito.mock(RpcRetryingCaller.class);

    Mockito.when(rpcFactory.<ScanResultWithContext> newCaller()).thenReturn(caller);
    Mockito.when(caller.callWithoutRetries(Mockito.any(RetryingCallable.class),
        Mockito.anyInt())).thenAnswer(new Answer<ScanResultWithContext>() {
          private int count = 0;
          @Override
          public ScanResultWithContext answer(InvocationOnMock invocation) throws Throwable {
            switch (count) {
              case 0: // initialize
              case 3: // close
                count++;
                return null;
              case 1:
                count++;
                return resultWithContext1;
              case 2:
                count++;
                return resultWithContext2;
              default:
                throw new RuntimeException("Expected only 2 invocations");
            }
          }
      });

    // Set a much larger cache and buffer size than we'll provide
    scan.setCaching(100);
    scan.setMaxResultSize(1000*1000);

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf("table"),
        clusterConn, rpcFactory, controllerFactory, pool, Integer.MAX_VALUE)) {

      // Due to initializeScannerInConstruction()
      Mockito.verify(caller).callWithoutRetries(Mockito.any(RetryingCallable.class),
          Mockito.anyInt());

      InOrder inOrder = Mockito.inOrder(caller);

      scanner.loadCache();

      inOrder.verify(caller, Mockito.times(2)).callWithoutRetries(
          Mockito.any(RetryingCallable.class), Mockito.anyInt());

      assertEquals(1, scanner.cache.size());
      Result r = scanner.cache.poll();
      assertNotNull(r);
      CellScanner cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv1, cs.current());
      assertFalse(cs.advance());

      scanner.setRpcFinished(true);

      inOrder = Mockito.inOrder(caller);

      scanner.loadCache();

      inOrder.verify(caller, Mockito.times(3)).callWithoutRetries(
          Mockito.any(RetryingCallable.class), Mockito.anyInt());

      r = scanner.cache.poll();
      assertNotNull(r);
      cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv2, cs.current());
      assertFalse(cs.advance());
    }
  }
}
