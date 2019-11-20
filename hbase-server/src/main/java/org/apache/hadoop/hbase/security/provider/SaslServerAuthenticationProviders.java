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

import java.util.HashMap;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class SaslServerAuthenticationProviders {
  private static final Logger LOG = LoggerFactory.getLogger(SaslClientAuthenticationProviders.class);

  private static final AtomicReference<SaslServerAuthenticationProviders> holder = new AtomicReference<>();

  private final HashMap<Byte, SaslServerAuthenticationProvider> providers;
  private SaslServerAuthenticationProviders(HashMap<Byte, SaslServerAuthenticationProvider> providers) {
    this.providers = providers;
  }

  /**
   * Returns a singleton instance of {@link SaslClientAuthenticationProviders}.
   */
  public static SaslServerAuthenticationProviders getInstance() {
    SaslServerAuthenticationProviders providers = holder.get();
    if (null == providers) {
      synchronized (holder) {
        // Someone else beat us here
        providers = holder.get();
        if (null != providers) {
          return providers;
        }

        providers = createProviders();
        holder.set(providers);
      }
    }
    return providers;
  }

  /**
   * Removes the cached singleton instance of {@link SaslClientAuthenticationProviders}. 
   */
  public static void resetLoadedProviders() {
    synchronized (holder) {
      holder.set(null);
    }
  }

  static SaslServerAuthenticationProviders createProviders() {
    ServiceLoader<SaslServerAuthenticationProvider> loader =
        ServiceLoader.load(SaslServerAuthenticationProvider.class);
    HashMap<Byte,SaslServerAuthenticationProvider> providers = new HashMap<>();
    for (SaslServerAuthenticationProvider provider : loader) {
      final byte newProviderAuthCode = provider.getAuthenticationCode();
      final SaslServerAuthenticationProvider alreadyRegisteredProvider = providers.get(newProviderAuthCode);
      if (alreadyRegisteredProvider != null) {
        throw new RuntimeException("Trying to load SaslServerAuthenticationProvider " + provider.getClass()
            + alreadyRegisteredProvider.getClass() + " is already registered with the same auth code");
      }
      providers.put(newProviderAuthCode, provider);
    }
    return new SaslServerAuthenticationProviders(providers);
  }

  /**
   * Selects the appropriate SaslServerAuthenticationProvider from those available. If there is no
   * matching provider for the given {@code authByte}, this method will return null.
   */
  public SaslServerAuthenticationProvider selectProvider(byte authByte) {
    return providers.get(Byte.valueOf(authByte));
  }
}
