package org.infinispan.server.endpoint.subsystem.security;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

import org.infinispan.server.core.security.AuthorizingCallbackHandler;
import org.infinispan.server.core.security.SubjectUserInfo;
import org.infinispan.server.endpoint.subsystem.RealmSubjectUserInfo;
import org.jboss.as.core.security.RealmUser;
import org.jboss.as.domain.management.AuthMechanism;
import org.jboss.as.domain.management.SecurityRealm;

/**
 * JBossSaslCallbackHandlers required for when JDG is ran on EAP 7.0.x and below
 *
 * @author Ryan Emerson
 * @since 8.5
 */
public class JBossSaslCallbackHandlers {
   public static class GSSAPIEndpointAuthorizingCallbackHandler implements AuthorizingCallbackHandler {
      private final org.jboss.as.domain.management.AuthorizingCallbackHandler delegate;
      private RealmUser realmUser;

      public GSSAPIEndpointAuthorizingCallbackHandler(SecurityRealm realm) {
         this.delegate = realm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN);
      }

      @Override
      public void handle(Callback[] callbacks) {
         AuthorizeCallback acb = (AuthorizeCallback) callbacks[0];
         String authenticationId = acb.getAuthenticationID();
         String authorizationId = acb.getAuthorizationID();
         acb.setAuthorized(authenticationId.equals(authorizationId));
         int realmSep = authorizationId.indexOf('@');
         realmUser = realmSep <= 0 ? new RealmUser(authorizationId) : new RealmUser(authorizationId.substring(realmSep + 1), authorizationId.substring(0, realmSep));
      }

      @Override
      public SubjectUserInfo getSubjectUserInfo(Collection<Principal> principals) {
         // The call to the delegate will supplement the realm user with additional role information
         Collection<Principal> realmPrincipals = new ArrayList<>();
         realmPrincipals.add(realmUser);
         try {
            org.jboss.as.core.security.SubjectUserInfo userInfo = delegate.createSubjectUserInfo(realmPrincipals);
            userInfo.getPrincipals().addAll(principals);
            return new RealmSubjectUserInfo(userInfo);
         } catch (IOException e) {
            throw ROOT_LOGGER.cannotRetrieveAuthorizationInformation(e, realmUser.toString());
         }
      }
   }

   public static class RealmAuthorizingCallbackHandler implements AuthorizingCallbackHandler {

      private final org.jboss.as.domain.management.AuthorizingCallbackHandler delegate;

      public RealmAuthorizingCallbackHandler(org.jboss.as.domain.management.AuthorizingCallbackHandler authorizingCallbackHandler) {
         this.delegate = authorizingCallbackHandler;
      }

      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
         delegate.handle(callbacks);
      }

      @Override
      public SubjectUserInfo getSubjectUserInfo(Collection<Principal> principals) {
         try {
            org.jboss.as.core.security.SubjectUserInfo realmUserInfo = delegate.createSubjectUserInfo(principals);
            return new RealmSubjectUserInfo(realmUserInfo.getUserName(), realmUserInfo.getSubject());
         } catch (IOException e) {
            // Handle this
            return null;
         }
      }
   }
}
