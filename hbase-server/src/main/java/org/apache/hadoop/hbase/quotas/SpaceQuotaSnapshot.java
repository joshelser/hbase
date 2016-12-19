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

import java.util.Objects;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;

/**
 * A point-in-time view of a space quota on a table.
 */

public class SpaceQuotaSnapshot {
  private final SpaceViolationPolicy policy;
  private final long usage;
  private final long limit;

  public SpaceQuotaSnapshot(SpaceViolationPolicy policy, long usage, long limit) {
    this.policy = Objects.requireNonNull(policy);
    this.usage = usage;
    this.limit = limit;
  }

  /**
   * Returns the state of violation for the quota.
   */
  public SpaceViolationPolicy getPolicy() {
    return policy;
  }

  /**
   * Returns the current usage, in bytes, of the target (e.g. table, namespace).
   */
  public long getUsage() {
    return usage;
  }

  /**
   * Returns the limit, in bytes, of the target (e.g. table, namespace).
   */
  public long getLimit() {
    return limit;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(policy.hashCode())
        .append(usage)
        .append(limit)
        .toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SpaceQuotaSnapshot) {
      SpaceQuotaSnapshot other = (SpaceQuotaSnapshot) o;
      return policy == other.policy && usage == other.usage && limit == other.limit;
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append("ViolationSnapshot: policy=").append(policy).append(", use=");
    sb.append(usage).append("bytes/").append(limit).append("bytes");
    return sb.toString();
  }

  // ProtobufUtil is in hbase-client, and this doesn't need to be public.
  public static SpaceQuotaSnapshot toSpaceQuotaSnapshot(QuotaProtos.SpaceViolation proto) {
    return new SpaceQuotaSnapshot(ProtobufUtil.toViolationPolicy(proto.getPolicy()),
        proto.getUsage(), proto.getLimit());
  }

  public static QuotaProtos.SpaceViolation toProtoSpaceViolation(SpaceQuotaSnapshot snapshot) {
    return QuotaProtos.SpaceViolation.newBuilder().setPolicy(ProtobufUtil.toProtoViolationPolicy(snapshot.getPolicy()))
        .setUsage(snapshot.getUsage()).setLimit(snapshot.getLimit()).build();
  }
}
