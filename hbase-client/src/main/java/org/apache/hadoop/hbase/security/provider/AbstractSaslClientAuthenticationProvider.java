package org.apache.hadoop.hbase.security.provider;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Base implementation of {@link SaslClientAuthenticationProvider}. All implementations should extend
 * this class instead of directly implementing the interface.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public abstract class AbstractSaslClientAuthenticationProvider implements SaslClientAuthenticationProvider {
  public static final Text AUTH_TOKEN_TYPE = new Text("HBASE_AUTH_TOKEN");


  @Override
  public final Text getTokenKind() {
    // All HBase authentication tokens are "HBASE_AUTH_TOKEN"'s. We differentiate between them
    // via the code().
    return AUTH_TOKEN_TYPE;
  }

  /**
   * Provides a hash code to identify this AuthenticationProvider among others. These two fields must be
   * unique to ensure that authentication methods are clearly separated.
   */
  @Override
  public final int hashCode() {
    return new HashCodeBuilder()
        .append(getAuthenticationName())
        .append(getAuthenticationCode())
        .toHashCode();
  }

  @Override
  public UserGroupInformation unwrapUgi(UserGroupInformation ugi) {
    // Unwrap the UGI with the real user when we're using Kerberos auth
    if (isKerberos() && ugi != null && ugi.getRealUser() != null) {
      return ugi.getRealUser();
    }

    // Otherwise, use the UGI we were given
    return ugi;
  }
}
