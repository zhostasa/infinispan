package org.infinispan.query.impl.massindex;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.IdentityWrapper;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * Base class for mass indexer tasks.
 *
 * @author gustavonalle
 * @since 7.1
 */
public class IndexWorker implements DistributedCallable<Object, Object, Void> {

   protected final Class<?> entity;
   private final boolean flush;
   private final boolean clean;
   private final boolean primaryOwner;
   protected Cache<Object, Object> cache;
   protected IndexUpdater indexUpdater;

   private ClusteringDependentLogic clusteringDependentLogic;
   private DataConversion valueDataConversion;
   private DataConversion keyDataConversion;

   public IndexWorker(Class<?> entity, boolean flush, boolean clean, boolean primaryOwner) {
      this.entity = entity;
      this.flush = flush;
      this.clean = clean;
      this.primaryOwner = primaryOwner;
   }

   @Override
   public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
      Cache<Object, Object> unwrapped = SecurityActions.getUnwrappedCache(cache);
      MemoryConfiguration memory = unwrapped.getCacheConfiguration().memory();
      this.cache = cache;
      if (memory.storageType() == StorageType.OBJECT) {
         this.cache = unwrapped.getAdvancedCache().withWrapping(ByteArrayWrapper.class, IdentityWrapper.class);
      }
      this.indexUpdater = new IndexUpdater(this.cache);
      ComponentRegistry componentRegistry = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
      this.clusteringDependentLogic = componentRegistry.getComponent(ClusteringDependentLogic.class);
      keyDataConversion = cache.getAdvancedCache().getKeyDataConversion();
      valueDataConversion = cache.getAdvancedCache().getValueDataConversion();
   }

   protected void preIndex() {
      if (clean) indexUpdater.purge(entity);
   }

   protected void postIndex() {
      if (flush) indexUpdater.flush(entity);
   }

   private KeyValueFilter getFilter() {
      return primaryOwner ? new PrimaryOwnersKeyValueFilter() : AcceptAllKeyValueFilter.getInstance();
   }

   private Object extractValue(Object storageValue) {
      return valueDataConversion.extractIndexable(storageValue);
   }

   @Override
   @SuppressWarnings("unchecked")
   public Void call() throws Exception {
      preIndex();
      KeyValueFilter filter = getFilter();
      try (Stream<CacheEntry<Object, Object>> stream = cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
            .cacheEntrySet().stream()) {
         Iterator<CacheEntry<Object, Object>> iterator = stream.filter(CacheFilters.predicate(filter)).iterator();
         while (iterator.hasNext()) {
            CacheEntry<Object, Object> next = iterator.next();
            Object value = extractValue(next.getValue());
            if (value != null && value.getClass().equals(entity))
               indexUpdater.updateIndex(next.getKey(), value);
         }
      }
      postIndex();

      return null;
   }


   private class PrimaryOwnersKeyValueFilter implements KeyValueFilter {

      @Override
      public boolean accept(Object key, Object value, Metadata metadata) {
         return clusteringDependentLogic.getCacheTopology().getDistribution(keyDataConversion.toStorage(key)).isPrimary();
      }
   }

   public static class Externalizer extends AbstractExternalizer<IndexWorker> {

      @Override
      @SuppressWarnings("ALL")
      public Set<Class<? extends IndexWorker>> getTypeClasses() {
         return Util.<Class<? extends IndexWorker>>asSet(IndexWorker.class);
      }

      @Override
      public void writeObject(ObjectOutput output, IndexWorker worker) throws IOException {
         output.writeObject(worker.entity);
         output.writeBoolean(worker.flush);
         output.writeBoolean(worker.clean);
         output.writeBoolean(worker.primaryOwner);
      }

      @Override
      public IndexWorker readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new IndexWorker((Class<?>) input.readObject(), input.readBoolean(), input.readBoolean(), input.readBoolean());
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INDEX_WORKER;
      }
   }

}
