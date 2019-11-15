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

import java.io.IOException;
import java.util.Map;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public class GssSaslClientAuthenticationProvider extends AbstractSaslClientAuthenticationProvider {
  private static final AuthMethod AUTH_METHOD = AuthMethod.KERBEROS;

  private String serverPrincipal;
  private Token<? extends TokenIdentifier> token;
  private boolean fallbackAllowed;
  private Map<String, String> saslProps;

  @Override
  public void configure(String serverPrincipal, Token<? extends TokenIdentifier> token, boolean fallbackAllowed, Map<String, String> saslProps) {
    this.serverPrincipal = serverPrincipal;
    this.token = token;
    this.fallbackAllowed = fallbackAllowed;
    this.saslProps = saslProps;
  }

  @Override
  public SaslClient createClient() throws IOException {
    String[] names = SaslUtil.splitKerberosName(serverPrincipal);
    if (names.length != 3) {
      throw new IOException("Kerberos principal '" + serverPrincipal + "' does not have the expected format");
    }
    return Sasl.createSaslClient(new String[] { AUTH_METHOD.getMechanismName() }, null, names[0], names[1], saslProps,
        null);
  }

  @Override
  public String getAuthenticationName() {
    return AUTH_METHOD.name();
  }

  @Override
  public byte getAuthenticationCode() {
    return AUTH_METHOD.code;
  }

  @Override
  public String getSaslMechanism() {
    return AUTH_METHOD.getMechanismName();
  }

  @Override
  public AuthenticationMethod getAuthMethod() {
    return AUTH_METHOD.authenticationMethod;
  }

  @Override
  public AuthMethod getHBaseAuthMethod() {
    return AUTH_METHOD;
  }

  public String getServerPrincipal() {
    return serverPrincipal;
  }

  public Token<? extends TokenIdentifier> getToken() {
    return token;
  }

  public boolean isFallbackAllowed() {
    return fallbackAllowed;
  }

  public Map<String, String> getSaslProps() {
    return saslProps;
  }

  public void setServerPrincipal(String serverPrincipal) {
    this.serverPrincipal = serverPrincipal;
  }

  public void setToken(Token<? extends TokenIdentifier> token) {
    this.token = token;
  }

  public void setFallbackAllowed(boolean fallbackAllowed) {
    this.fallbackAllowed = fallbackAllowed;
  }

  public void setSaslProps(Map<String, String> saslProps) {
    this.saslProps = saslProps;
  }
}
