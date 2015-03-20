/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import java.util.Arrays;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Encapsulates the results from a Scan, optionally with additionally
 * information from the RegionServer.
 */
@InterfaceAudience.Private
public class ScanResultWithContext {

  /**
   * Results from the server
   */
  private final Result[] results;
  /**
   * Was more information on the presence of more results
   * on the server returned?
   */
  private final boolean hasMoreResultsContext;
  /**
   * Do more results exist on the server. Only valid if
   * {@link #hasAddtlResultsContext} is true.
   */
  private final Boolean hasMoreResults;

  public ScanResultWithContext(Result[] results) {
    this.results = results;
    this.hasMoreResultsContext = false;
    this.hasMoreResults = null;
  }

  public ScanResultWithContext(Result[] results, boolean hasMoreResults) {
    this.results = results;
    this.hasMoreResultsContext = true;
    this.hasMoreResults = hasMoreResults;
  }

  public Result[] getResults() {
    return results;
  }

  public boolean getHasMoreResultsContext() {
    return hasMoreResultsContext;
  }

  public boolean getServerHasMoreResults() {
    if (!hasMoreResultsContext) {
      throw new IllegalStateException("Context doesn't contain server response whether " +
          "the server contains more results");
    }
    assert null != hasMoreResults;
    return hasMoreResults;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append("ScanResultWithContext[results=").append(Arrays.toString(results));
    sb.append(", hasMoreResultsContext=").append(hasMoreResultsContext);
    if (hasMoreResultsContext) {
      sb.append(", hasMoreResults=").append(hasMoreResults);
    }
    sb.append("]");
    return sb.toString();
  }

  /* Result doesn't have hashCode and equals implemented, so it's pointless
   * to try to implement these here.
   *
  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder(17,31);
    hcb.append(results).append(hasMoreResultsContext);
    if (hasMoreResultsContext) {
      hcb.append(hasMoreResults);
    }
    return hcb.toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ScanResultWithContext) {
      ScanResultWithContext other = (ScanResultWithContext) o;

      if (!Arrays.equals(results, other.results)) {
        return false;
      }

      if (hasMoreResultsContext != other.hasMoreResultsContext) {
        return false;
      }

      if (null == hasMoreResults) {
        // One is null, the other isn't
        if (null != other.hasMoreResults) {
          return false;
        }
        // Both are null
        return true;
      } else {
        // One is null, the other isn't
        if (null == other.hasMoreResults) {
          return false;
        }

        // Check boolean equality
        return hasMoreResults.equals(other.hasMoreResults);
      }
    }
    return false;
  }*/
}
