package org.infinispan.persistence;

import org.infinispan.persistence.spi.AdvancedCacheLoader;

/**
 * @author Mircea Markus
 * @private
 */
@Deprecated
public class TaskContextImpl implements AdvancedCacheLoader.TaskContext {

   private volatile boolean stopped;

   @Override
   public void stop() {
      stopped = true;
   }

   public boolean isStopped() {
      return stopped;
   }
}
