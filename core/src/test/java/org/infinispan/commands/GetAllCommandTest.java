package org.infinispan.commands;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @author William Burns
 */
@Test(groups = "functional")
public abstract class GetAllCommandTest extends MultipleCacheManagersTest {

   private final CacheMode cacheMode;
   private final boolean transactional;
   private final int numNodes = 4;
   private final int numEntries = 100;

   protected GetAllCommandTest(CacheMode cacheMode, boolean transactional) {
      this.cacheMode = cacheMode;
      this.transactional = transactional;
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testGetAllKeyNotPresent() {
      for (int i = 0; i < numEntries; ++i)
         advancedCache(i % numNodes).put("key" + i, "value" + i);
      List<Cache<String, String>> caches = caches();
      String notPresentString = "not-present";
      for (Cache<String, String> cache : caches) {
         Map<String, String> result = cache.getAdvancedCache().getAll(Collections.singleton(notPresentString));
         assertFalse(result.containsKey(notPresentString));
         assertNull(result.get(notPresentString));
      }
   }

   public void testGetAllCacheEntriesKeyNotPresent() {
      for (int i = 0; i < numEntries; ++i)
         advancedCache(i % numNodes).put("key" + i, "value" + i);
      List<Cache<String, String>> caches = caches();
      String notPresentString = "not-present";
      for (Cache<String, String> cache : caches) {
         Map<String, CacheEntry<String, String>> result = cache.getAdvancedCache().getAllCacheEntries(
               Collections.singleton(notPresentString));
         assertFalse(result.containsKey(notPresentString));
         assertNull(result.get(notPresentString));
      }
   }

   protected void amendConfiguration(ConfigurationBuilder builder) {
   }

   static void enableCompatibility(ConfigurationBuilder builder) {
      builder.compatibility().enable();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(cacheMode, transactional);
      amendConfiguration(dcc);
      createCluster(dcc, numNodes);
      waitForClusterToForm();
   }

   public void testGetAll() {
      for (int i = 0; i < numEntries; ++i)
         advancedCache(i % numNodes).put("key" + i, "value" + i);
      for (int i = 0; i < numEntries; ++i)
         for (Cache<Object, Object> cache : caches())
            assertEquals(cache.get("key" + i), "value" + i);

      for (int j = 0; j < 10; ++j) {
         Set<Object> mutableKeys = new HashSet<>();
         Map<Object, Object> expected = new HashMap<>();
         for (int i = j; i < numEntries; i += 10) {
            mutableKeys.add("key" + i);
            expected.put("key" + i, "value" + i);
         }
         Set<Object> immutableKeys = Collections.unmodifiableSet(mutableKeys);

         for (Cache<Object, Object> cache : caches()) {
            Map<Object, Object> result = cache.getAdvancedCache().getAll(immutableKeys);
            assertEquals(result, expected);
         }
      }
   }

   public void testGetAllCacheEntries() {
      for (int i = 0; i < numEntries; ++i)
         advancedCache(i % numNodes).put("key" + i, "value" + i);
      for (int i = 0; i < numEntries; ++i)
         for (Cache<Object, Object> cache : caches())
            assertEquals(cache.get("key" + i), "value" + i);

      for (int j = 0; j < 10; ++j) {
         Set<Object> mutableKeys = new HashSet<>();
         Map<Object, Object> expected = new HashMap<>();
         for (int i = j; i < numEntries; i += 10) {
            mutableKeys.add("key" + i);
            expected.put("key" + i, "value" + i);
         }
         Set<Object> immutableKeys = Collections.unmodifiableSet(mutableKeys);

         for (Cache<Object, Object> cache : caches()) {
            Map<Object, CacheEntry<Object, Object>> result = cache.getAdvancedCache().getAllCacheEntries(immutableKeys);
            expected.forEach((k, v) -> {
               CacheEntry<Object, Object> value = result.get(k);
               assertNotNull(value);
               assertEquals(k, value.getKey());
               assertEquals(v, value.getValue());
            });
         }
      }
   }
}
