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
import java.util.Objects;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory.QuotaGlobalsSettingsBypass;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest.Builder;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * An object which captures all quotas types (throttle, space) for a subject (user, table, ns).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class GlobalQuotaSettings extends QuotaSettings {
  private final QuotaProtos.Throttle throttleProto;
  private final boolean bypassGlobals;
  private final QuotaProtos.SpaceQuota spaceProto;

  protected GlobalQuotaSettings(String username, TableName tableName, String namespace, QuotaProtos.Quotas quotas) {
    this(username, tableName, namespace, quotas.getThrottle(), quotas.getBypassGlobals(), quotas.getSpace());
  }

  protected GlobalQuotaSettings(String userName, TableName tableName, String namespace, QuotaProtos.Throttle throttleProto, boolean bypassGlobals, QuotaProtos.SpaceQuota spaceProto) {
    super(userName, tableName, namespace);
    this.throttleProto = throttleProto;
    this.bypassGlobals = bypassGlobals;
    this.spaceProto = spaceProto;
  }

  @Override
  public QuotaType getQuotaType() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void setupSetQuotaRequest(Builder builder) {
    // ThrottleSettings should be used instead for setting a throttle quota.
    throw new UnsupportedOperationException(
        "This class should not be used to generate a SetQuotaRequest.");
  }

  protected QuotaProtos.Throttle getThrottleProto() {
    return this.throttleProto;
  }

  protected boolean getGlobalBypass() {
    return this.bypassGlobals;
  }

  protected QuotaProtos.SpaceQuota getSpaceProto() {
    return this.spaceProto;
  }

  /**
   * Constructs a new {@link Quotas} message from {@code this}.
   */
  protected Quotas toQuotas() {
    return QuotaProtos.Quotas.newBuilder()
        .setThrottle(getThrottleProto())
        .setBypassGlobals(getGlobalBypass())
        .setSpace(getSpaceProto())
        .build();
  }

  @Override
  protected GlobalQuotaSettings merge(QuotaSettings other) throws IOException {
    // Propagate the Throttle 
    QuotaProtos.Throttle.Builder throttleBuilder = throttleProto.toBuilder();
    if (other instanceof ThrottleSettings) {
      ThrottleSettings otherThrottle = (ThrottleSettings) other;

      if (otherThrottle.proto.hasType()) {
        QuotaProtos.ThrottleRequest otherProto = otherThrottle.proto;
        if (otherProto.hasTimedQuota()) {
          if (otherProto.hasTimedQuota()) {
            validateTimedQuota(otherProto.getTimedQuota());
          }
  
          switch (otherProto.getType()) {
            case REQUEST_NUMBER:
              if (otherProto.hasTimedQuota()) {
                throttleBuilder.setReqNum(otherProto.getTimedQuota());
              } else {
                throttleBuilder.clearReqNum();
              }
              break;
            case REQUEST_SIZE:
              if (otherProto.hasTimedQuota()) {
                throttleBuilder.setReqSize(otherProto.getTimedQuota());
              } else {
                throttleBuilder.clearReqSize();
              }
              break;
            case WRITE_NUMBER:
              if (otherProto.hasTimedQuota()) {
                throttleBuilder.setWriteNum(otherProto.getTimedQuota());
              } else {
                throttleBuilder.clearWriteNum();
              }
              break;
            case WRITE_SIZE:
              if (otherProto.hasTimedQuota()) {
                throttleBuilder.setWriteSize(otherProto.getTimedQuota());
              } else {
                throttleBuilder.clearWriteSize();
              }
              break;
            case READ_NUMBER:
              if (otherProto.hasTimedQuota()) {
                throttleBuilder.setReadNum(otherProto.getTimedQuota());
              } else {
                throttleBuilder.clearReqNum();
              }
              break;
            case READ_SIZE:
              if (otherProto.hasTimedQuota()) {
                throttleBuilder.setReadSize(otherProto.getTimedQuota());
              } else {
                throttleBuilder.clearReadSize();
              }
              break;
          }
        }
      }
    }

    // Propagate the space quota portion
    QuotaProtos.SpaceQuota.Builder spaceBuilder = spaceProto.toBuilder();
    if (other instanceof SpaceLimitSettings) {
      SpaceLimitSettings settingsToMerge = (SpaceLimitSettings) other;
      QuotaProtos.SpaceLimitRequest spaceRequest = settingsToMerge.getProto();

      // The message contained the expect SpaceQuota object
      if (spaceRequest.hasQuota()) {
        SpaceQuota quotaToMerge = spaceRequest.getQuota();
        // Validate that the two settings are for the same target.
        // SpaceQuotas either apply to a table or a namespace (no user spacequota).
        if (!Objects.equals(getTableName(), settingsToMerge.getTableName())
            && !Objects.equals(getNamespace(), settingsToMerge.getNamespace())) {
          throw new IllegalArgumentException(
              "Cannot merge " + settingsToMerge + " into " + this);
        }

        if (quotaToMerge.getRemove()) {
          // Update the builder to propagate the removal
          spaceBuilder.setRemove(true).clearSoftLimit().clearViolationPolicy();
        } else {
          // Add the new settings to the existing settings
          spaceBuilder.mergeFrom(quotaToMerge);
        }
      }
    }

    boolean bypassGlobals = this.bypassGlobals;
    if (other instanceof QuotaGlobalsSettingsBypass) {
      bypassGlobals = ((QuotaGlobalsSettingsBypass) other).getBypass();
    }

    return new GlobalQuotaSettings(
        getUserName(), getTableName(), getNamespace(), throttleBuilder.build(), bypassGlobals,
        spaceBuilder.build());
  }

  private void validateTimedQuota(final TimedQuota timedQuota) throws IOException {
    if (timedQuota.getSoftLimit() < 1) {
      throw new DoNotRetryIOException(new UnsupportedOperationException(
          "The throttle limit must be greater then 0, got " + timedQuota.getSoftLimit()));
    }
  }
}
