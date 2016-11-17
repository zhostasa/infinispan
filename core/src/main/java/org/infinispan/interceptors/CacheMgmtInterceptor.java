package org.infinispan.interceptors;

import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;

/**
 * Captures cache management statistics
 *
 * @author Jerry Gauthier
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
@MBean(objectName = "Statistics", description = "General statistics such as timings, hit/miss ratio, etc.")
public class CacheMgmtInterceptor extends JmxStatsCommandInterceptor {
}

