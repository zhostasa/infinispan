package org.infinispan.configuration.global;

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
}