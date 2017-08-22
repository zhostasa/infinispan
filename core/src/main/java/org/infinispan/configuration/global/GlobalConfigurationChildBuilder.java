package org.infinispan.configuration.global;

import org.infinispan.manager.EmbeddedCacheManager;

public interface GlobalConfigurationChildBuilder {
   TransportConfigurationBuilder transport();

   GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics();

   SerializationConfigurationBuilder serialization();

   ThreadPoolConfigurationBuilder listenerThreadPool();

   /**
    * @deprecated Since 8.3, no longer used.
    */
   @Deprecated
   ThreadPoolConfigurationBuilder replicationQueueThreadPool();

   /**
    * Please use {@link GlobalConfigurationChildBuilder#expirationThreadPool()}
    */
   @Deprecated
   ThreadPoolConfigurationBuilder evictionThreadPool();

   ThreadPoolConfigurationBuilder expirationThreadPool();

   ThreadPoolConfigurationBuilder persistenceThreadPool();

   ThreadPoolConfigurationBuilder stateTransferThreadPool();

   ThreadPoolConfigurationBuilder asyncThreadPool();

   GlobalSecurityConfigurationBuilder security();

   ShutdownConfigurationBuilder shutdown();

   SiteConfigurationBuilder site();

   GlobalConfiguration build();

   GlobalStateConfigurationBuilder globalState();

   /**
    * Sets the name of the cache that acts as the default cache and is returned by
    * {@link EmbeddedCacheManager#getCache()}. Not
    */
   GlobalConfigurationBuilder defaultCacheName(String defaultCacheName);
}
