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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * An Exception that is thrown when a space quota is in violation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SpaceLimitingException extends QuotaExceededException {
  private static final long serialVersionUID = 2319438922387583600L;
  private static final Log LOG = LogFactory.getLog(SpaceLimitingException.class);
  private static final String MESSAGE_PREFIX = SpaceLimitingException.class.getName() + ": ";

  private final SpaceViolationPolicy policy;

  public SpaceLimitingException(String msg) {
    super(parseMessage(msg));

    // Hack around ResponseConverter expecting to invoke a single-arg String constructor
    // on this class
    if (null != msg) {
      for (SpaceViolationPolicy definedPolicy : SpaceViolationPolicy.values()) {
        if (msg.indexOf(definedPolicy.name()) != -1) {
          policy = definedPolicy;
          return;
        }
      }
    }
    policy = null;
  }

  public SpaceLimitingException(SpaceViolationPolicy policy, String msg) {
    super(msg);
    this.policy = policy;
  }

  public SpaceLimitingException(SpaceViolationPolicy policy, String msg, Throwable e) {
    super(msg, e);
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

  private static String parseMessage(String originalMessage) {
    // Serialization of the exception places a duplicate class name. Try to strip that off if it
    // exists. Best effort... Looks something like:
    // "org.apache.hadoop.hbase.quotas.SpaceLimitingException: NO_INSERTS A Put is disallowed due
    // to a space quota."
    if (null != originalMessage && originalMessage.startsWith(MESSAGE_PREFIX)) {
      // If it starts with the class name, rip off the policy too.
      try {
        int index = originalMessage.indexOf(' ', MESSAGE_PREFIX.length());
        return originalMessage.substring(index + 1);
      } catch (Exception e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Failed to trim exception message", e);
        }
      }
    }
    return originalMessage;
  }

  @Override
  public String getMessage() {
    return (null == policy ? "(unknown policy)" : policy.name()) + " " + super.getMessage();
  }
}
