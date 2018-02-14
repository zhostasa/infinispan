package org.infinispan.server.jgroups.security;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.RealmCallback;

import org.jboss.sasl.callback.DigestHashCallback;

/**
 * JbossSaslClientCallbackHandler.
 *
 * @author Tristan Tarrant
 */
public class JbossSaslClientCallbackHandler implements CallbackHandler {
   private final String realm;
   private final String name;
   private final String credential;

   public JbossSaslClientCallbackHandler(String realm, String name, String credential) {
      this.realm = realm;
      this.name = name;
      this.credential = credential;
   }

   @Override
   public void handle(Callback[] callbacks) {
      for (Callback callback : callbacks) {
         if (callback instanceof PasswordCallback) {
            ((PasswordCallback) callback).setPassword(credential.toCharArray());
         } else if (callback instanceof NameCallback) {
            ((NameCallback) callback).setName(name);
         } else if (callback instanceof RealmCallback) {
            ((RealmCallback) callback).setText(realm);
         } else if (callback instanceof DigestHashCallback) {
            ((DigestHashCallback) callback).setHexHash(credential);
         }
      }
   }
}
