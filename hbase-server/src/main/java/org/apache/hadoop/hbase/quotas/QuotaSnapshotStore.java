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
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

/**
 * A common interface for computing quota observance/violation for tables or namespaces. 
 */
public interface QuotaSnapshotStore<T> {

  /**
   * The current state of a table with respect to the policy set forth by a quota.
   */
  public enum ViolationState {
    IN_VIOLATION,
    IN_OBSERVANCE,
  }

  /**
   * Singleton to represent a table without a quota defined. It is never in violation.
   */
  public static final SpaceQuotaSnapshot NO_QUOTA = new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), -1, -1);

  /**
   * Fetch the Quota for the given table. May be null.
   */
  SpaceQuota getSpaceQuota(T subject) throws IOException;

  /**
   * Returns the current {@link ViolationState} for the given <code>subject</code>.
   */
  SpaceQuotaSnapshot getCurrentState(T subject);

  /**
   * Computes the target {@link ViolationState} for the given <code>subject</code>.
   */
  SpaceQuotaSnapshot getTargetState(T subject, SpaceQuota spaceQuota);

  /**
   * Filters the provided <code>regions</code>, returning those which match the given
   * <code>subject</code>.
   *
   * @param subject The filter criteria.
   */
  Iterable<Entry<HRegionInfo,Long>> filterBySubject(T subject);

  /**
   * Sets the current {@link ViolationState} for the <code>subject</code>.
   */
  void setCurrentState(T subject, SpaceQuotaSnapshot state);
}
