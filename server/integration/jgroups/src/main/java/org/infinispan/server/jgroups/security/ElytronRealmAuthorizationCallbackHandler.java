package org.infinispan.server.jgroups.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.jboss.as.domain.management.AuthorizingCallbackHandler;
import org.jboss.as.domain.management.SecurityRealm;
import org.wildfly.security.auth.callback.AvailableRealmsCallback;

/**
 * RealmAuthorizationCallbackHandler. A {@link CallbackHandler} for JGroups which piggybacks on the
 * realm-provided {@link AuthorizingCallbackHandler}s and provides additional role validation
 *
 * @author Ryan Emerson
 * @since 8.5
 */
public class ElytronRealmAuthorizationCallbackHandler extends AbstractRealmAuthorizationCallbackHandler {

   private static final String SASL_OPT_PRE_DIGESTED_PROPERTY = "org.wildfly.security.sasl.digest.pre_digested";

   private String[] realmList;

   public ElytronRealmAuthorizationCallbackHandler(SecurityRealm realm, String mechanismName, String clusterRole, Map<String, String> mechanismProperties) {
      super(realm, mechanismName, clusterRole, mechanismProperties, SASL_OPT_PRE_DIGESTED_PROPERTY);

      if (DIGEST_MD5.equals(mechanismName)) {
         String realmStr = mechanismProperties.get(SASL_OPT_REALM_PROPERTY);
         realmList = realmStr == null ? new String[] {realm.getName()} : realmStr.split(" ");
      }
   }

   @Override
   public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      // We have to provide the available realms via this callback
      // Ideally we would utilise org.wildfly.security.sasl.util.AvailableRealmsSaslServerFactory, however as we can't
      // pass the SaslServerFactory impl to JGroups we must do it here instead.
      ArrayList<Callback> list = new ArrayList<>(Arrays.asList(callbacks));
      Iterator<Callback> it = list.iterator();
      while (it.hasNext()) {
         Callback callback = it.next();
         if (callback instanceof AvailableRealmsCallback) {
            ((AvailableRealmsCallback) callback).setRealmNames(realmList);
            it.remove();
         }
      }

      // If the only callback was AvailableRealmsCallback, we must not pass it to the AuthorizingCallbackHandler
      if (!list.isEmpty()) {
         AuthorizingCallbackHandler cbh = getMechCallbackHandler();
         cbh.handle(callbacks);
      }
   }
}
