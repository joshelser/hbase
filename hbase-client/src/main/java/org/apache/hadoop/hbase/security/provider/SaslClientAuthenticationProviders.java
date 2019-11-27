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
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
  public static final String EXTRA_PROVIDERS_KEY = "hbase.client.sasl.provider.extras";

  private static final AtomicReference<SaslClientAuthenticationProviders> providersRef =
      new AtomicReference<>();

  private final HashSet<SaslClientAuthenticationProvider> providers;
  private final ProviderSelector selector;

  private SaslClientAuthenticationProviders(HashSet<SaslClientAuthenticationProvider> providers,
      ProviderSelector selector) {
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

  /**
   * Adds the given {@code provider} to the set, only if an equivalent provider does not
   * already exist in the set.
   */
  static void addProviderIfNotExists(SaslClientAuthenticationProvider provider,
      HashSet<SaslClientAuthenticationProvider> providers) {
    if (providers.contains(provider)) {
    throw new RuntimeException("Already registered authentication provider with " +
        provider.getAuthenticationName() + ":" + provider.getAuthenticationCode()); 
    }
    providers.add(provider);
  }

  /**
   * Instantiates the ProviderSelector implementation from the provided configuration.
   */
  static ProviderSelector instantiateSelector(Configuration conf,
      HashSet<SaslClientAuthenticationProvider> providers) {
    Class<? extends ProviderSelector> clz = conf.getClass(
        SELECTOR_KEY, DefaultProviderSelector.class, ProviderSelector.class);
    try {
      ProviderSelector selector = clz.newInstance();
      selector.configure(conf, providers);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Loaded ProviderSelector {}", selector.getClass());
      }
      return selector;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Failed to instantiate " + clz +
          " as the ProviderSelector defined by " + SELECTOR_KEY, e);
    }
  }

  /**
   * Extracts and instantiates authentication providers from the configuration.
   */
  static void addExplicitProviders(Configuration conf,
      HashSet<SaslClientAuthenticationProvider> providers) {
    for(String implName : conf.getStringCollection(EXTRA_PROVIDERS_KEY)) {
      Class<?> clz;
      // Load the class from the config
      try {
        clz = Class.forName(implName);
      } catch (ClassNotFoundException e) {
        LOG.warn("Failed to load SaslClientAuthenticationProvider {}", implName, e);
        continue;
      }

      // Make sure it's the right type
      if (!SaslClientAuthenticationProvider.class.isAssignableFrom(clz)) {
        LOG.warn("Ignoring SaslClientAuthenticationProvider {} because it is not an instance of"
            + " SaslClientAuthenticationProvider", clz);
        continue;
      }

      // Instantiate it
      SaslClientAuthenticationProvider provider;
      try {
        provider = (SaslClientAuthenticationProvider) clz.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        LOG.warn("Failed to instantiate SaslClientAuthenticationProvider {}", clz, e);
        continue;
      }

      // Add it to our set, only if it doesn't conflict with something else we've
      // already registered.
      addProviderIfNotExists(provider, providers);
    }
  }

  /**
   * Instantiates all client authentication providers and returns an instance of
   * {@link SaslClientAuthenticationProviders}.
   */
  static SaslClientAuthenticationProviders instantiate(Configuration conf) {
    ServiceLoader<SaslClientAuthenticationProvider> loader =
        ServiceLoader.load(SaslClientAuthenticationProvider.class);
    HashSet<SaslClientAuthenticationProvider> providers = new HashSet<>();
    for (SaslClientAuthenticationProvider provider : loader) {
      addProviderIfNotExists(provider, providers);
    }

    addExplicitProviders(conf, providers);

    ProviderSelector selector = instantiateSelector(conf, providers);

    if (LOG.isTraceEnabled()) {
      String loadedProviders = providers.stream()
          .map((provider) -> provider.getClass().getName())
          .collect(Collectors.joining(", "));
      LOG.trace("Found SaslClientAuthenticationProviders {}", loadedProviders);
    }
    return new SaslClientAuthenticationProviders(providers, selector);
  }

  public SaslClientAuthenticationProvider getSimpleProvider() {
    Optional<SaslClientAuthenticationProvider> optional = providers.stream()
        .filter((p) -> p instanceof SimpleSaslClientAuthenticationProvider)
        .findFirst();
    return optional.get();
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
