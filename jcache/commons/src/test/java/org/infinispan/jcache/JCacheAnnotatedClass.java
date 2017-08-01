package org.infinispan.jcache;

import java.util.concurrent.atomic.AtomicInteger;

import javax.cache.annotation.CacheDefaults;
import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheRemove;
import javax.cache.annotation.CacheRemoveAll;
import javax.cache.annotation.CacheResult;

/**
 * Simple class using basic jsr107 annotations.
 *
 * @author Matej Cimbora
 */
@CacheDefaults(cacheName = "annotation", cacheKeyGenerator = JCacheCustomKeyGenerator.class)
public class JCacheAnnotatedClass {

   private AtomicInteger resultInvocationCount = new AtomicInteger();

   @CacheResult
   public String result(String input) {
      resultInvocationCount.incrementAndGet();
      return input;
   }

   @CachePut
   public String put(String input) {
      return input;
   }

   @CacheRemove
   public void remove(String input) {
      // do nothing
   }

   @CacheRemoveAll
   public void removeAll() {
      // do nothing
   }

   public int getResultInvocationCount() {
      return resultInvocationCount.get();
   }

}
