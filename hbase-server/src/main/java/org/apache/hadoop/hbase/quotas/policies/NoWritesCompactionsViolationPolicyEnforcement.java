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

import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;

/**
 * A {@link SpaceViolationPolicyEnforcement} implementation which disables all updates and
 * compactions. The enforcement counterpart to {@link SpaceViolationPolicy#NO_WRITES_COMPACTIONS}.
 */
public class NoWritesCompactionsViolationPolicyEnforcement extends NoWritesViolationPolicyEnforcement {

  @Override
  public void enable() {
    //TODO
    throw new RuntimeException("TODO How do we disable compactions?");
  }

  @Override
  public void disable() {
    //TODO
    throw new RuntimeException("TODO How do we enable compactions?");
  }

  @Override
  public SpaceViolationPolicy getPolicy() {
    return SpaceViolationPolicy.NO_WRITES_COMPACTIONS;
  }
}
