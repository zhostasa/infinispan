package org.infinispan.interceptors;

import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.MBean;


/**
 * This interceptor acts as a replacement to the replication interceptor when the CacheImpl is configured with
 * ClusteredSyncMode as INVALIDATE.
 * <p/>
 * The idea is that rather than replicating changes to all caches in a cluster when write methods are called, simply
 * broadcast an {@link InvalidateCommand} on the remote caches containing all keys modified.  This allows the remote
 * cache to look up the value in a shared cache loader which would have been updated with the changes.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
@MBean(objectName = "Invalidation", description = "Component responsible for invalidating entries on remote caches when entries are written to locally.")
public class InvalidationInterceptor extends BaseRpcInterceptor implements JmxStatisticsExposer {
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
