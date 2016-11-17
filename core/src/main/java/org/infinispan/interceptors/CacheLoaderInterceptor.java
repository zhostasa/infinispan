package org.infinispan.interceptors;

import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;

/**
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
@MBean(objectName = "CacheLoader", description = "Component that handles loading entries from a CacheStore into memory.")
public class CacheLoaderInterceptor<K, V> extends JmxStatsCommandInterceptor {
}
