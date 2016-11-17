package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.MBean;

/**
 * Interceptor in charge with handling transaction related operations, e.g enlisting cache as an transaction
 * participant, propagating remotely initiated changes.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.transaction.xa.TransactionXaAdapter
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
@MBean(objectName = "Transactions", description = "Component that manages the cache's participation in JTA transactions.")
public class TxInterceptor<K, V> extends CommandInterceptor implements JmxStatisticsExposer {
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
