package org.infinispan.commons.api;

/**
 * Defines the methods used to obtain a {@link org.infinispan.commons.api.BasicCache}.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface BasicCacheContainer extends Lifecycle {
   @Deprecated
   String DEFAULT_CACHE_NAME = "___defaultcache";

   /**
    * Retrieves the default cache associated with this cache container.
    * <p/>
    * As such, this method is always guaranteed to return the default cache, unless one has not been supplied to the
    * cache container.
    * <p />
    * <b>NB:</b> Shared caches are supported (and in fact encouraged) but if they are used it's the users responsibility to
    * ensure that <i>at least one</i> but <i>only one</i> caller calls stop() on the cache, and it does so with the awareness
    * that others may be using the cache.
    *
    * @return the default cache.
    */
   <K, V> BasicCache<K, V> getCache();

   /**
    * Retrieves a named cache from the system.  If the cache has been previously created with the same name, the running
    * cache instance is returned.  Otherwise, this method attempts to create the cache first.
    * <p/>
    * Optionally, when creating a new cache, this method can also create the cache if it doesn't already exist.
    * <p />
    * <b>NB:</b> Shared caches are supported (and in fact encouraged) but if they are used it's the users responsibility to
    * ensure that <i>at least one</i> but <i>only one</i> caller calls stop() on the cache, and it does so with the awareness
    * that others may be using the cache.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName
    */
   <K, V> BasicCache<K, V> getCache(String cacheName);
}
