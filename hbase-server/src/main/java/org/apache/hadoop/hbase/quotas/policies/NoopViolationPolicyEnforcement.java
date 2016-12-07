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
package org.apache.hadoop.hbase.quotas.policies;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.quotas.SpaceLimitingException;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * A {@link SpaceViolationPolicyEnforcement} instance which does nothing. Useful for tables
 * which have no violation policy.
 */
public class NoopViolationPolicyEnforcement implements SpaceViolationPolicyEnforcement {
  private static final NoopViolationPolicyEnforcement INSTANCE = new NoopViolationPolicyEnforcement();

  private NoopViolationPolicyEnforcement() {}

  public static NoopViolationPolicyEnforcement getInstance() {
    return INSTANCE;
  }

  @Override
  public void initialize(RegionServerServices rss, TableName tableName) {}

  @Override
  public void enable() {}

  @Override
  public void disable() {}

  @Override
  public void check(Mutation m) throws SpaceLimitingException {}

  @Override
  public SpaceViolationPolicy getPolicy() {
    return null;
  }

  @Override
  public boolean areCompactionsDisabled() {
    return false;
  }
}
