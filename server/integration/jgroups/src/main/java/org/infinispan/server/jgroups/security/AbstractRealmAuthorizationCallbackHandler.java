package org.infinispan.server.jgroups.security;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

import org.infinispan.server.jgroups.logging.JGroupsLogger;
import org.jboss.as.core.security.RealmUser;
import org.jboss.as.core.security.SubjectUserInfo;
import org.jboss.as.domain.management.AuthMechanism;
import org.jboss.as.domain.management.AuthorizingCallbackHandler;
import org.jboss.as.domain.management.RealmConfigurationConstants;
import org.jboss.as.domain.management.SecurityRealm;

/**
 * An abstract class which provides common methods for JbossSasl and Elyton compatible CallbackHandler implementations
 *
 * @author Tristan Tarrant
 * @author Ryan Emerson
 * @since 8.5
 */
abstract class AbstractRealmAuthorizationCallbackHandler implements CallbackHandler {
   static final String SASL_OPT_REALM_PROPERTY = "com.sun.security.sasl.digest.realm";

   static final String DIGEST_MD5 = "DIGEST-MD5";
   static final String EXTERNAL = "EXTERNAL";
   static final String GSSAPI = "GSSAPI";
   static final String PLAIN = "PLAIN";

   protected final SecurityRealm realm;
   private final String mechanismName;
   private final String clusterRole;
   private final String preDigestProperty;

   AbstractRealmAuthorizationCallbackHandler(SecurityRealm realm, String mechanismName, String clusterRole,
                                             Map<String, String> mechanismProperties, String preDigestProperty) {
      this.mechanismName = mechanismName;
      this.realm = realm;
      this.clusterRole = clusterRole;
      this.preDigestProperty = preDigestProperty;
      tunePropsForMech(mechanismProperties);
   }

   private void tunePropsForMech(Map<String, String> mechanismProperties) {
      if (DIGEST_MD5.equals(mechanismName)) {
         if (!mechanismProperties.containsKey(SASL_OPT_REALM_PROPERTY)) {
            mechanismProperties.put(SASL_OPT_REALM_PROPERTY, realm.getName());
         }
         Map<String, String> mechConfig = realm.getMechanismConfig(AuthMechanism.DIGEST);
         boolean plainTextDigest = true;
         if (mechConfig.containsKey(RealmConfigurationConstants.DIGEST_PLAIN_TEXT)) {
            plainTextDigest = Boolean.parseBoolean(mechConfig.get(RealmConfigurationConstants.DIGEST_PLAIN_TEXT));
         }
         if (!plainTextDigest) {
            mechanismProperties.put(preDigestProperty, "true");
         }
      }
   }

   AuthorizingCallbackHandler getMechCallbackHandler() {
      if (PLAIN.equals(mechanismName)) {
         return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN));
      } else if (DIGEST_MD5.equals(mechanismName)) {
         return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.DIGEST));
      } else if (GSSAPI.equals(mechanismName)) {
         return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN));
      } else if (EXTERNAL.equals(mechanismName)) {
         return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.CLIENT_CERT));
      } else {
         throw new IllegalArgumentException("Unsupported mech " + mechanismName);
      }
   }

   SubjectUserInfo validateSubjectRole(SubjectUserInfo subjectUserInfo) {
      for (Principal principal : subjectUserInfo.getPrincipals()) {
         if (clusterRole.equals(principal.getName())) {
            return subjectUserInfo;
         }
      }
      throw JGroupsLogger.ROOT_LOGGER.unauthorizedNodeJoin(subjectUserInfo.getUserName());
   }

   class DelegatingRoleAwareAuthorizingCallbackHandler implements AuthorizingCallbackHandler {
      private final AuthorizingCallbackHandler delegate;

      DelegatingRoleAwareAuthorizingCallbackHandler(AuthorizingCallbackHandler acbh) {
         this.delegate = acbh;
      }

      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
         AuthorizeCallback acb = findCallbackHandler(AuthorizeCallback.class, callbacks);
         if (acb != null) {
            String authenticationId = acb.getAuthenticationID();
            String authorizationId = acb.getAuthorizationID();
            acb.setAuthorized(authenticationId.equals(authorizationId));
            int realmSep = authorizationId.indexOf('@');
            RealmUser realmUser = realmSep < 0 ? new RealmUser(authorizationId) : new RealmUser(authorizationId.substring(realmSep + 1), authorizationId.substring(0, realmSep));
            List<Principal> principals = new ArrayList<>();
            principals.add(realmUser);
            createSubjectUserInfo(principals);
         } else {
            delegate.handle(callbacks);
         }
      }

      @Override
      public SubjectUserInfo createSubjectUserInfo(Collection<Principal> principals) throws IOException {
         // The call to the delegate will supplement the user with additional role information
         SubjectUserInfo subjectUserInfo = delegate.createSubjectUserInfo(principals);
         return validateSubjectRole(subjectUserInfo);
      }
   }

   private static <T extends Callback> T findCallbackHandler(Class<T> klass, Callback[] callbacks) {
      for (Callback callback : callbacks) {
         if (klass.isInstance(callback))
            return (T) callback;
      }
      return null;
   }
}
