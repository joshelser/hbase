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


import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * An Exception that is thrown when a space quota is in violation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SpaceLimitingException extends QuotaExceededException {
  private static final long serialVersionUID = 2319438922387583600L;

  private final SpaceViolationPolicy policy;

  public SpaceLimitingException(SpaceViolationPolicy policy, String msg) {
    super(msg);
    this.policy = policy;
  }

  /**
   * Returns the violation policy in effect.
   *
   * @return The violation policy in effect.
   */
  public SpaceViolationPolicy getViolationPolicy() {
    return this.policy;
  }
}
