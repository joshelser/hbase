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

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * RegionServer implementation of {@link SpaceViolationPolicy}.
 *
 * Implementations must have a public, no-args constructor.
 */
public interface SpaceViolationPolicyEnforcement {

  /**
   * Initializes this policy instance.
   */
  void initialize(RegionServerServices rss, TableName tableName);

  /**
   * Enables this policy. Not all policies have enable actions.
   */
  void enable() throws IOException;

  /**
   * Disables this policy. Not all policies have disable actions.
   */
  void disable() throws IOException;

  /**
   * Checks the given {@link Mutation} against <code>this</code> policy. If the
   * {@link Mutation} violates the policy, this policy should throw a
   * {@link SpaceLimitingException}.
   *
   * @throws SpaceLimitingException When the given mutation violates this policy.
   */
  void check(Mutation m) throws SpaceLimitingException;

  /**
   * Returns the {@link SpaceViolationPolicy} this enforcement is for.
   */
  SpaceViolationPolicy getPolicy();

  /**
   * Returns whether or not compactions on this table should be disabled for this policy.
   */
  boolean areCompactionsDisabled();
}
