package org.infinispan.server.endpoint.subsystem;

import java.util.Map;

import org.infinispan.server.core.security.AuthorizingCallbackHandler;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.endpoint.subsystem.security.ElytronCallbackHandlers;
import org.infinispan.server.endpoint.subsystem.security.JBossSaslCallbackHandlers;
import org.jboss.as.domain.management.AuthMechanism;
import org.jboss.as.domain.management.RealmConfigurationConstants;
import org.jboss.as.domain.management.SecurityRealm;

/**
 * EndpointServerAuthenticationProvider.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class EndpointServerAuthenticationProvider implements ServerAuthenticationProvider {
   private static final String SASL_OPT_REALM_PROPERTY = "com.sun.security.sasl.digest.realm";
   private static final String JBOSS_SASL_OPT_PRE_DIGESTED_PROPERTY = "org.jboss.sasl.digest.pre_digested";
   private static final String ELYTRON_SASL_OPT_PRE_DIGESTED_PROPERTY = "org.wildfly.security.sasl.digest.pre_digested";

   private static final String DIGEST_MD5 = "DIGEST-MD5";
   private static final String EXTERNAL = "EXTERNAL";
   private static final String GSSAPI = "GSSAPI";
   private static final String PLAIN = "PLAIN";


   private final SecurityRealm realm;
   private final boolean elytron;

   EndpointServerAuthenticationProvider(SecurityRealm realm, boolean elytron) {
      this.realm = realm;
      this.elytron = elytron;
   }

   @Override
   public AuthorizingCallbackHandler getCallbackHandler(String mechanismName, Map<String, String> mechanismProperties) {
      if (GSSAPI.equals(mechanismName)) {
         // The EAP SecurityRealm doesn't actually support a GSSAPI mech yet so let's handle this ourselves
         return elytron ?
               new ElytronCallbackHandlers.GSSAPIEndpointAuthorizingCallbackHandler(realm) :
               new JBossSaslCallbackHandlers.GSSAPIEndpointAuthorizingCallbackHandler(realm);
      } else if (PLAIN.equals(mechanismName)) {
         return getAuthCallbackHandler(AuthMechanism.PLAIN);
      } else if (DIGEST_MD5.equals(mechanismName)) {
         Map<String, String> mechConfig = realm.getMechanismConfig(AuthMechanism.DIGEST);
         boolean plainTextDigest = true;
         if (mechConfig.containsKey(RealmConfigurationConstants.DIGEST_PLAIN_TEXT)) {
            plainTextDigest = Boolean.parseBoolean(mechConfig.get(RealmConfigurationConstants.DIGEST_PLAIN_TEXT));
         }
         String realmStr = mechanismProperties.get(SASL_OPT_REALM_PROPERTY);
         if (elytron) {
            mechanismProperties.put(ELYTRON_SASL_OPT_PRE_DIGESTED_PROPERTY, Boolean.toString(plainTextDigest));
            String[] realmList = realmStr == null ? new String[]{realm.getName()} : realmStr.split(" ");
            return new ElytronCallbackHandlers.RealmAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.DIGEST), realmList);
         }

         if (realmStr == null) {
            mechanismProperties.put(SASL_OPT_REALM_PROPERTY, realm.getName());
         }
         if (!plainTextDigest) {
            mechanismProperties.put(JBOSS_SASL_OPT_PRE_DIGESTED_PROPERTY, "true");
         }
         return getAuthCallbackHandler(AuthMechanism.DIGEST);
      } else if (EXTERNAL.equals(mechanismName)) {
         return getAuthCallbackHandler(AuthMechanism.CLIENT_CERT);
      } else {
         throw new IllegalArgumentException("Unsupported mech " + mechanismName);
      }
   }

   private AuthorizingCallbackHandler getAuthCallbackHandler(AuthMechanism mechanism) {
      org.jboss.as.domain.management.AuthorizingCallbackHandler delegate = realm.getAuthorizingCallbackHandler(mechanism);
      return elytron ?
            new ElytronCallbackHandlers.RealmAuthorizingCallbackHandler(delegate) :
            new JBossSaslCallbackHandlers.RealmAuthorizingCallbackHandler(delegate);
   }
}
