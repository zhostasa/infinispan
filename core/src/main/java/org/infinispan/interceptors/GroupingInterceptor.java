package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * A {@link org.infinispan.interceptors.base.CommandInterceptor} implementation that keeps track of the keys
 * added/removed during the processing of a {@link org.infinispan.commands.remote.GetKeysInGroupCommand}
 *
 * @author Pedro Ruivo
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class GroupingInterceptor extends CommandInterceptor {
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }
}
