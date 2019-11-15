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
package org.apache.hadoop.hbase.security.provider;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accessor for all SaslAuthenticationProvider instances.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public class SaslClientAuthenticationProviders {
  private static final Logger LOG = LoggerFactory.getLogger(SaslClientAuthenticationProviders.class);

  public static final String SELECTOR_KEY = "hbase.client.sasl.provider.class";

  private static final AtomicReference<SaslClientAuthenticationProviders> providersRef =
      new AtomicReference<>();

  private final Configuration conf;
  private final HashSet<SaslClientAuthenticationProvider> providers;
  private final ProviderSelector selector;

  private SaslClientAuthenticationProviders(
      Configuration conf, HashSet<SaslClientAuthenticationProvider> providers,
      ProviderSelector selector) {
    this.conf = conf;
    this.providers = providers;
    this.selector = selector;
  }

  /**
   * Returns a singleton instance of {@link SaslClientAuthenticationProviders}.
   */
  public static synchronized SaslClientAuthenticationProviders getInstance(Configuration conf) {
    SaslClientAuthenticationProviders providers = providersRef.get();
    if (providers == null) {
      providers = instantiate(conf);
      providersRef.set(providers);
    }

    return providers;
  }

  /**
   * Removes the cached singleton instance of {@link SaslClientAuthenticationProviders}. 
   */
  public static synchronized void reset() {
    providersRef.set(null);
  }

  static SaslClientAuthenticationProviders instantiate(Configuration conf) {
    ServiceLoader<SaslClientAuthenticationProvider> loader =
        ServiceLoader.load(SaslClientAuthenticationProvider.class);
    HashSet<SaslClientAuthenticationProvider> providers = new HashSet<>();
    for (SaslClientAuthenticationProvider provider : loader) {
      if (providers.contains(provider)) {
        throw new RuntimeException("Already registered authentication provider with " +
            provider.getAuthenticationName() + ":" + provider.getAuthenticationCode()); 
      }
      providers.add(provider);
    }

    Class<? extends ProviderSelector> clz = conf.getClass(
        SELECTOR_KEY, DefaultProviderSelector.class, ProviderSelector.class);
    ProviderSelector selector = null;
    try {
      selector = clz.newInstance();
      selector.configure(conf, providers);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Failed to instantiate " + clz +
          " as the ProviderSelector defined by " + SELECTOR_KEY, e);
    }

    return new SaslClientAuthenticationProviders(conf, providers, selector);
  }
 
  public Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> selectProvider(
      Text clusterId, UserGroupInformation clientUser) {
    return selector.selectProvider(clusterId, clientUser);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("providers=[");
    boolean first = true;
    for (SaslClientAuthenticationProvider provider : providers) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(provider.getClass());
    }
    return sb.append("], selector=").append(selector.getClass()).toString();
  }
}
