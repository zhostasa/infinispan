package org.infinispan.server.hotrod.test;

import static org.infinispan.server.core.test.ServerTestingUtil.findFreePort;
import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtils.getDefaultHotRodConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtils.startHotRodServer;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "server.hotrod.test.HotRodServerRegisterUnregisterTopologyCacheTest", groups = "functional")
public class HotRodServerRegisterUnregisterTopologyCacheTest extends MultipleCacheManagersTest {

   private static final int CLUSTER_SIZE = 2;

   private List<HotRodServer> hotRodServers;

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false), CLUSTER_SIZE);
      assertNotNull(manager(0).getCache());
      assertNotNull(manager(1).getCache());

      startServers();
      waitForClusterToForm();
   }

   @Test(dataProvider = "getServers")
   public void testServerRegisterTopologyCacheOnStart(HotRodServer server, Set<String> internalCaches) {
      String topologyCacheName = server.getConfiguration().topologyCacheName();
      assertTrue(internalCaches.contains(topologyCacheName));
   }

   @Test(dataProvider = "getServers")
   public void testServerUnregisterTopologyCacheOnStop(HotRodServer server, Set<String> internalCaches) {
      String topologyCacheName = server.getConfiguration().topologyCacheName();
      killServer(server);
      assertFalse(internalCaches.contains(topologyCacheName));
   }

   private void startServers() {
      hotRodServers = new ArrayList<>(CLUSTER_SIZE);
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         hotRodServers.add(startHotRodServer(manager(i), findFreePort(), getDefaultHotRodConfiguration()));
      }
   }

   @DataProvider
   public Object[][] getServers() {
      Object[][] data = new Object[CLUSTER_SIZE][];
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         HotRodServer server = hotRodServers.get(i);
         Set<String> internalCaches = server
               .getCacheManager()
               .getGlobalComponentRegistry()
               .getComponent(InternalCacheRegistry.class)
               .getInternalCacheNames();
         data[i] = new Object[]{server, internalCaches};
      }
      return data;
   }
}
