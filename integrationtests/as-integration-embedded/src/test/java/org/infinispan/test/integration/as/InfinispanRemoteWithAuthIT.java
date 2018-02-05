package org.infinispan.test.integration.as;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.SaslQop;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
public class InfinispanRemoteWithAuthIT {
   private RemoteCacheManager rcm;

   private static RemoteCacheManager createRemoteCacheManager() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer()
            .host("127.0.0.1")
            .security()
            .authentication()
            .enable()
            .username("guest").password("password")
            .realm("ApplicationRealm")
            .serverName("caching-service")
            .saslMechanism("DIGEST-MD5")
            .saslQop(SaslQop.AUTH);
      return new RemoteCacheManager(config.build());
   }

   @After
   public void cleanUp() {
      if (rcm != null)
         rcm.stop();
   }

   @Test
   public void testCacheAccessible() {
      rcm = createRemoteCacheManager();
      RemoteCache<String, String> cache = rcm.getCache();
      cache.put("Key", "Value");
      cache.clear();
   }
}
