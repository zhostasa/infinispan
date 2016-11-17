package org.infinispan.interceptors;

import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;

/**
 * Writes modifications back to the store on the way out: stores modifications back through the CacheLoader, either
 * after each method call (no TXs), or at TX commit.
 *
 * Only used for LOCAL and INVALIDATION caches.
 *
 * @author Bela Ban
 * @author Dan Berindei
 * @author Mircea Markus
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
@MBean(objectName = "CacheStore", description = "Component that handles storing of entries to a CacheStore from memory.")
public class CacheWriterInterceptor extends JmxStatsCommandInterceptor {
}
