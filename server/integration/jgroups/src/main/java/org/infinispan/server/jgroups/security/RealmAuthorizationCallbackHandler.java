package org.infinispan.server.jgroups.security;

import java.io.IOException;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.jboss.as.domain.management.AuthorizingCallbackHandler;
import org.jboss.as.domain.management.SecurityRealm;

/**
 * RealmAuthorizationCallbackHandler. A {@link CallbackHandler} for JGroups which piggybacks on the
 * realm-provided {@link AuthorizingCallbackHandler}s and provides additional role validation
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class RealmAuthorizationCallbackHandler extends AbstractRealmAuthorizationCallbackHandler {

    private static final String SASL_OPT_PRE_DIGESTED_PROPERTY = "org.jboss.sasl.digest.pre_digested";

    public RealmAuthorizationCallbackHandler(SecurityRealm realm, String mechanismName, String clusterRole,
                                             Map<String, String> mechanismProperties) {
        super(realm, mechanismName, clusterRole, mechanismProperties, SASL_OPT_PRE_DIGESTED_PROPERTY);
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        AuthorizingCallbackHandler cbh = getMechCallbackHandler();
        cbh.handle(callbacks);
    }
}
