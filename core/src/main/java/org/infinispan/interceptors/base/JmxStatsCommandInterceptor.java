package org.infinispan.interceptors.base;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.jmx.JmxStatisticsExposer;

/**
 * Base class for all the interceptors exposing management statistics.
 *
 * @author Mircea.Markus@jboss.com
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public abstract class JmxStatsCommandInterceptor extends CommandInterceptor implements JmxStatisticsExposer {
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean getStatisticsEnabled() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void resetStatistics() {
      throw new UnsupportedOperationException();
   }
}
