/*
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

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultProviderSelector {

  DefaultProviderSelector selector;
  @Before
  public void setup() {
    selector = new DefaultProviderSelector();
  }

  @Test(expected = IllegalStateException.class)
  public void testExceptionOnMissingProviders() {
    selector.configure(new Configuration(false), Collections.emptySet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullConfiguration() {
    selector.configure(null, Collections.emptySet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullProviderMap() {
    selector.configure(new Configuration(false), null);
  }

  @Test(expected = IllegalStateException.class)
  public void testDuplicateProviders() {
    Set<SaslClientAuthenticationProvider> providers = new HashSet<>();
    providers.add(new SimpleSaslClientAuthenticationProvider());
    providers.add(new SimpleSaslClientAuthenticationProvider());
    selector.configure(new Configuration(false), providers);
  }

  @Test
  public void testExpectedProviders() {
    HashSet<SaslClientAuthenticationProvider> providers = new HashSet<>(Arrays.asList(
        new SimpleSaslClientAuthenticationProvider(), new GssSaslClientAuthenticationProvider(),
        new DigestSaslClientAuthenticationProvider()));

    selector.configure(new Configuration(false), providers);

    assertNotNull("Simple provider was null", selector.simpleAuth);
    assertNotNull("Kerberos provider was null", selector.krbAuth);
    assertNotNull("Digest provider was null", selector.digestAuth);
  }
}
