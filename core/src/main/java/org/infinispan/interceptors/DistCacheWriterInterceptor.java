package org.infinispan.interceptors;

/**
 * Cache store interceptor specific for the distribution and replication cache modes.
 *
 * <p>If the cache store is shared, only the primary owner of the key writes to the cache store.</p>
 * <p>If the cache store is not shared, every owner of a key writes to the cache store.</p>
 * <p>In non-tx caches, if the originator is an owner, the command is executed there twice. The first time,
 * ({@code isOriginLocal() == true}) we don't write anything to the cache store; the second time,
 * the normal rules apply.</p>
 * <p>For clear operations, either only the originator of the command clears the cache store (if it is
 * shared), or every node clears its cache store (if it is not shared). Note that in non-tx caches, this
 * happens without holding a lock on the primary owner of all the keys.</p>
 *
 * @author Galder Zamarreño
 * @author Dan Berindei
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class DistCacheWriterInterceptor extends CacheWriterInterceptor {
}
