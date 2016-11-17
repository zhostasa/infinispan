package org.infinispan.interceptors;


import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class InvocationContextInterceptor extends CommandInterceptor {
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }
}
