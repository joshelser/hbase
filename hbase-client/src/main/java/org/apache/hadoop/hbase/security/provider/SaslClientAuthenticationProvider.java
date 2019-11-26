package org.apache.hadoop.hbase.security.provider;

import java.io.IOException;
import java.util.Map;

import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Encapsulation of client-side logic to authenticate to HBase via some means over SASL.
 * Implementations should not directly implement this interface, but instead extend
 * {@link AbstractSaslClientAuthenticationProvider}.
 *
 * Implementations of this interface must make an implementation of {link {@link #hashCode()}
 * which returns the same value across multiple instances of the provider implementation.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public interface SaslClientAuthenticationProvider {

  /**
   * Creates the SASL client instance for this auth'n method.
   */
  SaslClient createClient(Configuration conf, String serverPrincipal,
      Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
      Map<String, String> saslProps) throws IOException;

  /**
   * Returns the unique name to identify this authentication method among other HBase auth methods.
   */
  String getAuthenticationName();

  /**
   * Returns the unique value to identify this authentication method among other HBase auth methods.
   */
  byte getAuthenticationCode();

  /**
   * Returns the SASL mechanism used by this authentication method. 
   */
  String getSaslMechanism();

  /**
   * Returns the Hadoop {@link AuthenticationMethod} for this method.
   */
  AuthenticationMethod getAuthMethod();

  /**
   * Returns the name of the type used by the TokenIdentifier.
   */
  Text getTokenKind();

  /**
   * Stopgap. REMOVE_ME
   */
  AuthMethod getHBaseAuthMethod();
}
