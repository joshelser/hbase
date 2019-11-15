package org.apache.hadoop.hbase.security.provider;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultProviderSelector implements ProviderSelector {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultProviderSelector.class);

  Configuration conf;
  SimpleSaslClientAuthenticationProvider simpleAuth = null;
  GssSaslClientAuthenticationProvider krbAuth = null;
  DigestSaslClientAuthenticationProvider digestAuth = null;
  Set<SaslClientAuthenticationProvider> providers;

  @Override
  public void configure(Configuration conf, Set<SaslClientAuthenticationProvider> providers) {
    this.conf = conf;
    this.providers = providers;
    for (SaslClientAuthenticationProvider provider : providers) {
      if (provider instanceof SimpleSaslClientAuthenticationProvider) {
        if (simpleAuth != null) {
          LOG.warn("Ignoring duplicate SimpleSaslClientAuthenticationProvider: previous={}, ignored={}",
              simpleAuth.getClass(), provider.getClass());
        } else {
          simpleAuth = (SimpleSaslClientAuthenticationProvider) provider;
        }
      } else if (provider instanceof GssSaslClientAuthenticationProvider) {
        if (krbAuth != null) {
          LOG.warn("Ignoring duplicate GssSaslClientAuthenticationProvider: previous={}, ignored={}",
              krbAuth.getClass(), provider.getClass());
        } else {
          krbAuth = (GssSaslClientAuthenticationProvider) provider;
        }
      } else if (provider instanceof DigestSaslClientAuthenticationProvider) {
        if (digestAuth != null) {
          LOG.warn("Ignoring duplicate DigestSaslClientAuthenticationProvider: previous={}, ignored={}",
              digestAuth.getClass(), provider.getClass());
        } else {
          digestAuth = (DigestSaslClientAuthenticationProvider) provider;
        }
      } else {
        LOG.warn("Ignoring unknown SaslClientAuthenticationProvider: {}", provider.getClass());
      }
    }
  }

  @Override
  public Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> selectProvider(Text clusterId, UserGroupInformation ugi) {
    if (clusterId == null) {
      throw new NullPointerException("Null clusterId was given");
    }
    // Superfluous: we dont' do SIMPLE auth over SASL, but we should to simplify.
    if (!User.isHBaseSecurityEnabled(conf)) {
      return new Pair<>(simpleAuth, null);
    }
    if (ugi.hasKerberosCredentials()) {
      return new Pair<>(krbAuth, null);
    }
    // Must be digest auth, look for a token.
    for (Token<? extends TokenIdentifier> token : ugi.getTokens()) {
      // We need to check for two things:
      //   1. This token is for the HBase cluster we want to talk to
      //   2. We have suppporting client implementation to handle the token (the "kind" of token)
      if (clusterId.equals(token.getService()) && digestAuth.getTokenKind().equals(token.getKind())) {
        return new Pair<>(digestAuth, token);
      }
    }
    LOG.warn("No matching SASL authentication provider and supporting token found from providers {} to HBase cluster {}", providers, clusterId);
    return null;
  }

}
