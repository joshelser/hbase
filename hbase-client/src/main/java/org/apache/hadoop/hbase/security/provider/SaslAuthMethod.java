package org.apache.hadoop.hbase.security.provider;

import java.util.Objects;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Describes the way in which some {@link SaslClientAuthenticationProvider} authenticates over SASL.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public class SaslAuthMethod {

  private final String name;
  private final byte code;
  private final String saslMech;
  private final AuthenticationMethod method;
  
  public SaslAuthMethod(String name, byte code, String saslMech, AuthenticationMethod method) {
    this.name = name;
    this.code = code;
    this.saslMech = saslMech;
    this.method = method;
  }
 
  public String getName() {
    return name;
  }

  public byte getCode() {
    return code;
  }

  public String getSaslMechanism() {
    return saslMech;
  }

  public AuthenticationMethod getAuthMethod() {
    return method;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SaslAuthMethod)) {
      return false;
    }
    SaslAuthMethod other = (SaslAuthMethod) o;
    return Objects.equals(name, other.name) &&
        code == other.code &&
        Objects.equals(saslMech, other.saslMech) &&
        Objects.equals(method, other.method);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(name)
        .append(code)
        .append(saslMech)
        .append(method)
        .toHashCode();
  }
}
