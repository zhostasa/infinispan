package org.infinispan.query.remote.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.security.Security;

/**
 * SecurityActions for the org.infinispan.query.remote.impl package. Do not move and do not change class and method
 * visibility!
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static ComponentRegistry getCacheComponentRegistry(AdvancedCache<?, ?> cache) {
      return doPrivileged(cache::getComponentRegistry);
   }

   static Configuration getCacheConfiguration(AdvancedCache<?, ?> cache) {
      return doPrivileged(cache::getCacheConfiguration);
   }

   static <K, V> Cache<K, V> getCache(EmbeddedCacheManager cacheManager, String cacheName) {
      return doPrivileged(() -> cacheManager.getCache(cacheName));
   }

   static void registerProtobufMetadataManager(GlobalComponentRegistry gcr, ProtobufMetadataManager protobufMetadataManager) {
      gcr.registerComponent(protobufMetadataManager, ProtobufMetadataManager.class);
   }
}
