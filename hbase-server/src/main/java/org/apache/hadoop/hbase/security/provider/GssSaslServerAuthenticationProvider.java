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
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public class GssSaslServerAuthenticationProvider extends GssSaslClientAuthenticationProvider implements SaslServerAuthenticationProvider {
  private static final Logger LOG = LoggerFactory.getLogger(GssSaslServerAuthenticationProvider.class);

  @Override
  public SaslServer createServer(SecretManager<TokenIdentifier> secretManager,
      Map<String, String> saslProps) throws IOException {
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    String fullName = current.getUserName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Server's Kerberos principal name is " + fullName);
    }
    String[] names = SaslUtil.splitKerberosName(fullName);
    if (names.length != 3) {
      throw new AccessDeniedException(
          "Kerberos principal does NOT contain an instance (hostname): " + fullName);
    }
    try {
      return current.doAs(new PrivilegedExceptionAction<SaslServer>() {
        @Override
        public SaslServer run() throws SaslException {
          return Sasl.createSaslServer(getSaslMechanism(), names[0],
            names[1], saslProps, new SaslGssCallbackHandler());
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Failed to construct GSS SASL server");
    }
  }

  /** CallbackHandler for SASL GSSAPI Kerberos mechanism */
  private static class SaslGssCallbackHandler implements CallbackHandler {

    /** {@inheritDoc} */
    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL GSSAPI Callback");
        }
      }
      if (ac != null) {
        String authid = ac.getAuthenticationID();
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "SASL server GSSAPI callback: setting " + "canonicalized client ID: " + authzid);
          }
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }
}
