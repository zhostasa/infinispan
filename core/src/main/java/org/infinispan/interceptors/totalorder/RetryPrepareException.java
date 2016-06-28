package org.infinispan.interceptors.totalorder;

import org.infinispan.commons.CacheException;

/**
 * Indicates the state transfer is running and the prepare should be retried.
 *
 * @author Pedro Ruivo
 * @deprecated Since 8.3, will be removed.
 */
@Deprecated
public class RetryPrepareException extends CacheException {

   public RetryPrepareException() {
      super();
   }

}
