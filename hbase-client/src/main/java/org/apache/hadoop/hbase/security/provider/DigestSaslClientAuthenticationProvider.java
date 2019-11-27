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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public class DigestSaslClientAuthenticationProvider extends AbstractSaslClientAuthenticationProvider {

  public static final String MECHANISM = "DIGEST-MD5";
  private static final AuthMethod AUTH_METHOD = AuthMethod.DIGEST;

  public static String getMechanism() {
    return MECHANISM;
  }

  @Override
  public SaslClient createClient(Configuration conf, String serverPrincipal,
      Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
      Map<String, String> saslProps) throws IOException {
    return Sasl.createSaslClient(new String[] { MECHANISM }, null, null,
        SaslUtil.SASL_DEFAULT_REALM, saslProps, new DigestSaslClientCallbackHandler(token));
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

  public static class DigestSaslClientCallbackHandler implements CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DigestSaslClientCallbackHandler.class);
    private final String userName;
    private final char[] userPassword;

    public DigestSaslClientCallbackHandler(Token<? extends TokenIdentifier> token) {
      this.userName = SaslUtil.encodeIdentifier(token.getIdentifier());
      this.userPassword = SaslUtil.encodePassword(token.getPassword());
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL client callback: setting username: " + userName);
        }
        nc.setName(userName);
      }
      if (pc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL client callback: setting userPassword");
        }
        pc.setPassword(userPassword);
      }
      if (rc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL client callback: setting realm: " + rc.getDefaultText());
        }
        rc.setText(rc.getDefaultText());
      }
    }
  }

  @Override
  public UserInformation getUserInfo(UserGroupInformation user) {
    return null;
  }

  @Override
  public boolean isKerberos() {
    return false;
  }
}
