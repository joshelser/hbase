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

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClientSmallScanner.SmallScannerCallable;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

/**
 * Construct a {@link SmallScannerCallable} wrapped in a {@link ScannerCallableWithReplicas}
 */
@InterfaceAudience.Private
public class SmallScannerCallableFactory implements RetryingCallableFactory {

  @Override
  public ScannerCallableWithReplicas getCallable(ClusterConnection connection, TableName table,
      Scan scan, ScanMetrics scanMetrics, byte[] localStartKey, int cacheNum,
      RpcControllerFactory controllerFactory, ExecutorService pool, int primaryOperationTimeout,
      int retries, int scannerTimeout, Configuration conf, RpcRetryingCaller<Result[]> caller) {
    scan.setStartRow(localStartKey);
    SmallScannerCallable s = new SmallScannerCallable(
      connection, table, scan, scanMetrics, controllerFactory, cacheNum, 0);
    ScannerCallableWithReplicas scannerCallableWithReplicas =
        new ScannerCallableWithReplicas(table, connection,
            s, pool, primaryOperationTimeout, scan, retries,
            scannerTimeout, cacheNum, conf, caller);
    return scannerCallableWithReplicas;
  }

}
