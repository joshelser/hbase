package org.apache.hadoop.hbase.security.provider;

import java.util.Collection;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accessor for all SaslAuthenticationProvider instances.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public class SaslClientAuthenticationProviders {
  private static final Logger LOG = LoggerFactory.getLogger(SaslClientAuthenticationProviders.class);

  private static final AtomicReference<SaslClientAuthenticationProviders> holder = new AtomicReference<>();

  private final HashSet<SaslClientAuthenticationProvider> providers;
  private SaslClientAuthenticationProviders(HashSet<SaslClientAuthenticationProvider> providers) {
    this.providers = providers;
  }

  /**
   * Returns a singleton instance of {@link SaslClientAuthenticationProviders}.
   */
  public static SaslClientAuthenticationProviders getInstance() {
    SaslClientAuthenticationProviders providers = holder.get();
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

  static SaslClientAuthenticationProviders createProviders() {
    ServiceLoader<SaslClientAuthenticationProvider> loader =
        ServiceLoader.load(SaslClientAuthenticationProvider.class);
    HashSet<SaslClientAuthenticationProvider> providers = new HashSet<>();
    for (SaslClientAuthenticationProvider provider : loader) {
      if (providers.contains(provider)) {
        throw new RuntimeException("Already registered authentication provider with " + provider.getAuthenticationName() + ":" + provider.getAuthenticationCode()); 
      }
      providers.add(provider);
    }
    return null;
  }

  /**
   * Selects the first matching provider for HBase client authentication from all provided tokens.  
   */
  public Pair<Token<? extends TokenIdentifier>, SaslClientAuthenticationProvider> selectProvider(
      Text clusterId, Collection<Token<? extends TokenIdentifier>> tokens) {
    if (clusterId == null) {
      throw new NullPointerException("Null clusterId was given");
    }
    // TODO this mimics the current logic from AuthenticationTokenSelector, but it's not the
    // best. We should really have some weighting to be able to judge cryptographic strength
    // or how recent those tokens are.
    for (SaslClientAuthenticationProvider provider : providers) {
      for (Token<? extends TokenIdentifier> token : tokens) {
        // We need to check for two things:
        //   1. This token is for the HBase cluster we want to talk to
        //   2. We have suppporting client implementation to handle the token (the "kind" of token)
        if (clusterId.equals(token.getService()) && provider.getTokenKind().equals(token.getKind())) {
          LOG.trace("Returning token={} used by provider={}", token, provider);
          return new Pair<>(token, provider);
        }
      }
    }
    LOG.warn("No matching SASL authentication provider found from providers {} to HBase cluster {}", providers, clusterId);
    return null;
  }
}
