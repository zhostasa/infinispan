package org.infinispan.interceptors.base;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Acts as a base for all RPC calls
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public abstract class BaseRpcInterceptor extends CommandInterceptor {
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }
}
