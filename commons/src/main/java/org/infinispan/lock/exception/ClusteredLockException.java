package org.infinispan.lock.exception;

/**
 * Exception used to handle errors on clustered locks
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 8.5
 */
public class ClusteredLockException extends RuntimeException {

   public ClusteredLockException(String message) {
      super(message);
   }

   public ClusteredLockException(Throwable t) {
      super(t);
   }
}
