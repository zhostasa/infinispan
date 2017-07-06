package org.infinispan.query.backend;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.search.spi.CacheManagerService;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Simple wrapper to make the Cache's ComponentRegistry, the CacheManager and the LuceneAnalysisDefinitionProvider
 * available to the services managed by Hibernate Search.
 *
 * @author Sanne Grinovero
 * @since 7.0
 */
final class InfinispanLoopbackService implements CacheManagerService, ComponentRegistryService {

   private final ComponentRegistry componentRegistry;
   private final EmbeddedCacheManager cacheManager;

   InfinispanLoopbackService(ComponentRegistry componentRegistry, EmbeddedCacheManager cacheManager) {
      this.componentRegistry = componentRegistry;
      this.cacheManager = cacheManager;
   }

   @Override
   public ComponentRegistry getComponentRegistry() {
      return componentRegistry;
   }

   @Override
   public EmbeddedCacheManager getEmbeddedCacheManager() {
      return cacheManager;
   }
}
