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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.DisableTableViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoInsertsViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoWritesCompactionsViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoWritesViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoopViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * A factory class for instantiating {@link SpaceViolationPolicyEnforcement} instances.
 */
public class SpaceViolationPolicyEnforcementFactory {

  private static final SpaceViolationPolicyEnforcementFactory INSTANCE =
      new SpaceViolationPolicyEnforcementFactory();

  private SpaceViolationPolicyEnforcementFactory() {}

  public static SpaceViolationPolicyEnforcementFactory getInstance() {
    return INSTANCE;
  }

  public SpaceViolationPolicyEnforcement create(
      RegionServerServices rss, TableName tableName, SpaceViolationPolicy policy) {
    SpaceViolationPolicyEnforcement enforcement;
    switch (policy) {
      case NONE:
        enforcement = NoopViolationPolicyEnforcement.getInstance();
        break;
      case DISABLE:
        enforcement = new DisableTableViolationPolicyEnforcement();
        break;
      case NO_WRITES_COMPACTIONS:
        enforcement = new NoWritesCompactionsViolationPolicyEnforcement();
        break;
      case NO_WRITES:
        enforcement = new NoWritesViolationPolicyEnforcement();
        break;
      case NO_INSERTS:
        enforcement = new NoInsertsViolationPolicyEnforcement();
        break;
      default:
        throw new IllegalArgumentException("Unhandled SpaceViolationPolicy: " + policy);
    }
    enforcement.initialize(rss, tableName);
    return enforcement;
  }
}
