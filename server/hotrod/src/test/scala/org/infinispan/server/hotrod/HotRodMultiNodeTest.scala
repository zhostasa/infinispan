package org.infinispan.server.hotrod

import org.infinispan.configuration.cache.ConfigurationBuilder
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.test.ServerTestingUtil._
import org.infinispan.server.hotrod.test.HotRodClient
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.test.{MultipleCacheManagersTest, TestingUtil}
import org.testng.annotations.{AfterClass, AfterMethod, BeforeClass, Test}

import scala.collection.JavaConversions._

/**
 * Base test class for multi node or clustered Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class HotRodMultiNodeTest extends MultipleCacheManagersTest {
   private[this] var hotRodServers: List[HotRodServer] = List()
   private[this] var hotRodClients: List[HotRodClient] = List()

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers() {
      for (i <- 0 until nodeCount) {
         val cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration())
         cacheManagers.add(cm)
      }
      defineCaches(cacheName)
   }

   protected def defineCaches(cacheName: String): Unit = {
      cacheManagers.foreach(cm => cm.defineConfiguration(cacheName, createCacheConfig.build()))
   }

   @BeforeClass(alwaysRun = true)
   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createBeforeClass() {
      super.createBeforeClass()

      var nextServerPort = serverPort
      for (i <- 0 until nodeCount) {
         hotRodServers = hotRodServers ::: List(startTestHotRodServer(cacheManagers.get(i), nextServerPort))
         nextServerPort += 50
      }

      hotRodClients = createClients(cacheName)
   }

   protected def createClients(cacheName: String): List[HotRodClient] = {
      hotRodServers.map(s =>
         new HotRodClient("127.0.0.1", s.getPort, cacheName, 60, protocolVersion))
   }

   protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager, port: Int) = startHotRodServer(cacheManager, port)

   protected def startClusteredServer(port: Int): HotRodServer =
      startClusteredServer(port, doCrash = false)

   protected def startClusteredServer(port: Int, doCrash: Boolean): HotRodServer = {
      val cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration())
      cacheManagers.add(cm)
      cm.defineConfiguration(cacheName, createCacheConfig.build())

      val newServer =
         try {
            if (doCrash) startCrashingHotRodServer(cm, port)
            else startHotRodServer(cm, port)
         } catch {
            case e: Exception => {
               log.error("Exception starting Hot Rod server", e)
               TestingUtil.killCacheManagers(cm)
               throw e
            }
         }

      TestingUtil.blockUntilViewsReceived(50000, true, cacheManagers)
      newServer
   }

   protected def stopClusteredServer(server: HotRodServer) {
      killServer(server)
      TestingUtil.killCacheManagers(server.getCacheManager)
      cacheManagers.remove(server.getCacheManager)
      TestingUtil.blockUntilViewsReceived(50000, false, cacheManagers)
   }

   def currentServerTopologyId: Int = {
      getServerTopologyId(servers.head.getCacheManager, cacheName)
   }

   @AfterClass(alwaysRun = true)
   override def destroy() {
      try {
         log.debug("Test finished, close Hot Rod server")
         hotRodClients.foreach(killClient(_))
         hotRodServers.foreach(killServer(_))
      } finally {
        // Stop the caches last so that at stoppage time topology cache can be updated properly
         super.destroy()
      }
   }

   @AfterMethod(alwaysRun=true)
   override def clearContent() {
      // Do not clear cache between methods so that topology cache does not get cleared
   }

   protected def servers = hotRodServers

   protected def clients = hotRodClients

   protected def cacheName: String

   protected def createCacheConfig: ConfigurationBuilder

   protected def protocolVersion: Byte = 21

   protected def nodeCount: Int = 2
}
