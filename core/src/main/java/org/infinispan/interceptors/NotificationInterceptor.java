package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * The interceptor in charge of firing off notifications to cache listeners
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class NotificationInterceptor extends CommandInterceptor {
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }
}
