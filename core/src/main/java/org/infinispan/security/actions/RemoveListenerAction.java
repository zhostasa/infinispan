package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.notifications.Listenable;

/**
 * RemoveListenerAction
 *
 * @author vjuranek
 * @since 8.3
 */
public class RemoveListenerAction implements PrivilegedAction<Void> {

    private final Listenable listenable;
    private final Object listener;

    public RemoveListenerAction(Listenable listenable, Object listener) {
        this.listenable = listenable;
        this.listener = listener;
    }

    @Override
    public Void run() {
        listenable.removeListener(listener);
        return null;
    }

}
