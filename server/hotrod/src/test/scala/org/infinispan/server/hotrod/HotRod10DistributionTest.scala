package org.infinispan.server.hotrod

import java.lang.reflect.Method

import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.server.hotrod.Constants._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.test.HotRodClient
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.test.AbstractCacheTest._
import org.infinispan.test.TestingUtil
import org.infinispan.topology.ClusterCacheStatus
import org.testng.Assert._
import org.testng.annotations.Test

/**
 * Tests Hot Rod logic when interacting with distributed caches, particularly logic to do with
 * hash-distribution-aware headers and how it behaves when cluster formation changes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodDistributionTest")
class HotRod10DistributionTest extends HotRodMultiNodeTest {

   override protected def cacheName: String = "hotRodDistSync"

   override protected def createCacheConfig: ConfigurationBuilder = {
      val cfg = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false))
      cfg.clustering().l1().disable() // Disable L1 explicitly
      cfg
   }

   override protected def protocolVersion : Byte = 10

   def testDistributedPutWithTopologyChanges(m: Method) {
      val client1 = clients.head
      val client2 = clients.tail.head

      var resp = client1.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
      assertStatus(resp, Success)
      assertHashTopology10Received(resp.topologyResponse, servers, cacheName, currentServerTopologyId)

      resp = client1.put(k(m) , 0, 0, v(m), INTELLIGENCE_BASIC, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, null)
      assertSuccess(client2.get(k(m), 0), v(m))

      resp = client1.put(k(m) , 0, 0, v(m, "v1-"), INTELLIGENCE_TOPOLOGY_AWARE, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse, servers, currentServerTopologyId)

      resp = client2.put(k(m) , 0, 0, v(m, "v2-"), INTELLIGENCE_TOPOLOGY_AWARE, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse, servers, currentServerTopologyId)

      resp = client1.put(k(m) , 0, 0, v(m, "v3-"), INTELLIGENCE_TOPOLOGY_AWARE, ClusterCacheStatus.INITIAL_TOPOLOGY_ID + nodeCount)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, null)
      assertSuccess(client2.get(k(m), 0), v(m, "v3-"))

      resp = client1.put(k(m) , 0, 0, v(m, "v4-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
      assertStatus(resp, Success)
      assertHashTopology10Received(resp.topologyResponse, servers, cacheName, currentServerTopologyId)
      assertSuccess(client2.get(k(m), 0), v(m, "v4-"))

      resp = client2.put(k(m) , 0, 0, v(m, "v5-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
      assertStatus(resp, Success)
      assertHashTopology10Received(resp.topologyResponse, servers, cacheName, currentServerTopologyId)
      assertSuccess(client2.get(k(m), 0), v(m, "v5-"))

      val newServer = startClusteredServer(servers.tail.head.getPort + 25)
      val newClient = new HotRodClient(
            "127.0.0.1", newServer.getPort, cacheName, 60, protocolVersion)
      val allServers = newServer :: servers
      try {
         log.trace("New client started, modify key to be v6-*")
         resp = newClient.put(k(m) , 0, 0, v(m, "v6-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
         assertStatus(resp, Success)
         assertHashTopology10Received(resp.topologyResponse, allServers, cacheName, currentServerTopologyId)

         log.trace("Get key and verify that's v6-*")
         assertSuccess(client2.get(k(m), 0), v(m, "v6-"))

         resp = client2.put(k(m), 0, 0, v(m, "v7-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
         assertStatus(resp, Success)
         assertHashTopology10Received(resp.topologyResponse, allServers, cacheName, currentServerTopologyId)

         assertSuccess(newClient.get(k(m), 0), v(m, "v7-"))
      } finally {
         log.trace("Stopping new server")
         killClient(newClient)
         stopClusteredServer(newServer)
         TestingUtil.waitForRehashToComplete(cache(0, cacheName), cache(1, cacheName))
         log.trace("New server stopped")
      }

      resp = client2.put(k(m) , 0, 0, v(m, "v8-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, ClusterCacheStatus.INITIAL_TOPOLOGY_ID + nodeCount)
      assertStatus(resp, Success)
      assertHashTopology10Received(resp.topologyResponse, servers, cacheName, currentServerTopologyId)

      assertSuccess(client1.get(k(m), 0), v(m, "v8-"))
   }
}