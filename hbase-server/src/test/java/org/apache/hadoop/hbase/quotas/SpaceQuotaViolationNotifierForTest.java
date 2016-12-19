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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

/**
 * A SpaceQuotaViolationNotifier implementation for verifying testing.
 */
public class SpaceQuotaViolationNotifierForTest implements SpaceQuotaViolationNotifier {

  private final Map<TableName,SpaceQuotaSnapshot> tablesInViolation = new HashMap<>();

  @Override
  public void initialize(Connection conn) {}

  @Override
  public void transitionTable(TableName tableName, SpaceQuotaSnapshot snapshot) {
    tablesInViolation.put(tableName, snapshot);
  }

  public Map<TableName,SpaceQuotaSnapshot> snapshotTablesInViolation() {
    return new HashMap<>(this.tablesInViolation);
  }

  public void clearTableViolations() {
    this.tablesInViolation.clear();
  }
}
