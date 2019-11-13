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

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SaslUtil;
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

  private String serverPrincipal;
  private Token<? extends TokenIdentifier> token;
  private boolean fallbackAllowed;
  private Map<String, String> saslProps;

  @Override
  public void configure(String serverPrincipal, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
      Map<String, String> saslProps) {
    this.serverPrincipal = serverPrincipal;
    this.token = token;
    this.fallbackAllowed = fallbackAllowed;
    this.saslProps = saslProps;
  }

  @Override
  public SaslClient createClient() throws IOException {
    return Sasl.createSaslClient(new String[] { MECHANISM }, null, null, SaslUtil.SASL_DEFAULT_REALM, saslProps,
        new DigestSaslClientCallbackHandler(token));
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

  static class DigestSaslClientCallbackHandler implements CallbackHandler {
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
}
