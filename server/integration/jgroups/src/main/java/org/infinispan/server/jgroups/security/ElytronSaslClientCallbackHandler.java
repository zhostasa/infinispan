package org.infinispan.server.jgroups.security;

import static org.wildfly.security.password.interfaces.ClearPassword.ALGORITHM_CLEAR;
import static org.wildfly.security.password.interfaces.DigestPassword.ALGORITHM_DIGEST_MD5;

import java.nio.charset.StandardCharsets;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.RealmCallback;

import org.wildfly.security.auth.callback.CredentialCallback;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.password.Password;
import org.wildfly.security.password.interfaces.ClearPassword;
import org.wildfly.security.password.interfaces.DigestPassword;
import org.wildfly.security.util.ByteIterator;

/**
 * ElytronSaslClientCallbackHandler required for when JDG is ran on EAP 7.1.x and above
 *
 * @author Ryan Emerson
 * @since 8.5
 */
public class ElytronSaslClientCallbackHandler implements CallbackHandler {
   private final String realm;
   private final String name;
   private final String credential;

   public ElytronSaslClientCallbackHandler(String realm, String name, String credential) {
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
         } else if (callback instanceof CredentialCallback) {
            CredentialCallback cb = (CredentialCallback) callback;
            Password password;
            switch (cb.getAlgorithm()) {
               case ALGORITHM_CLEAR:
                  password = ClearPassword.createRaw(ALGORITHM_CLEAR, credential.toCharArray());
                  break;
               case ALGORITHM_DIGEST_MD5:
                  byte[] decodedDigest = ByteIterator.ofBytes(credential.getBytes(StandardCharsets.UTF_8)).hexDecode().drain();
                  password = DigestPassword.createRaw(ALGORITHM_DIGEST_MD5, name, realm, decodedDigest);
                  break;
               default:
                  continue;
            }
            cb.setCredential(cb.getCredentialType().cast(new PasswordCredential(password)));
         }
      }
   }
}
