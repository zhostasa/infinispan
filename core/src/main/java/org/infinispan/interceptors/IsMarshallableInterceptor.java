package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * Interceptor to verify whether parameters passed into cache are marshallables
 * or not.
 *
 * <p>This is handy when marshalling happens in a separate
 * thread and marshalling failures might be swallowed.
 * Currently, this only happens when we have an asynchronous store.</p>
 *
 * @author Galder Zamarreño
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class IsMarshallableInterceptor extends CommandInterceptor {
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }
}
