package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * This interceptor populates the {@link org.infinispan.transaction.xa.DldGlobalTransaction} with
 * appropriate information needed in order to accomplish deadlock detection. It MUST process populate data before the
 * replication takes place, so it will do all the tasks before calling {@link org.infinispan.interceptors.base.CommandInterceptor#invokeNextInterceptor(org.infinispan.context.InvocationContext,
 * org.infinispan.commands.VisitableCommand)}.
 * <p/>
 * Note: for local caches, deadlock detection dos NOT work for aggregate operations (clear, putAll).
 *
 * @author Mircea.Markus@jboss.com
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class DeadlockDetectingInterceptor extends CommandInterceptor {
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }
}
