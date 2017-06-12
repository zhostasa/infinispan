package org.infinispan.multimap.impl;

import static java.lang.String.format;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import javax.transaction.TransactionManager;

import org.infinispan.multimap.api.MultimapCache;
import org.infinispan.remoting.transport.Address;

public class MultimapTestUtils {
   public static final String NAMES_KEY = "names";
   public static final User JULIEN = new User("Julien", 33);
   public static final User OIHANA = new User("Oihana", 1);
   public static final User RAMON = new User("Ramon", 38);
   public static final User KOLDO = new User("Koldo", 0);


   public static TransactionManager getTransactionManager(MultimapCache multimapCache) {
      EmbeddedMultimapCache embeddedMultimapCache = (EmbeddedMultimapCache) multimapCache;
      return embeddedMultimapCache == null ? null : extractComponent(embeddedMultimapCache.getCache(), TransactionManager.class);
   }

   public static void putValuesOnMultimapCache(MultimapCache<String, User> multimapCache, String key, User... values) {
      for (int i = 0; i < values.length; i++) {
         await(multimapCache.put(key, values[i]));
      }
   }

   public static void putValuesOnMultimapCache(Map<Address, MultimapCache<String, User>> cluster, String key, User... values) {
      for (MultimapCache mc : cluster.values()) {
         putValuesOnMultimapCache(mc, key, values);
      }
   }

   public static void assertMultimapCacheSize(MultimapCache<String, User> multimapCache, int expectedSize) {
      assertEquals(expectedSize, await(multimapCache.size()).intValue());
   }

   public static void assertMultimapCacheSize(Map<Address, MultimapCache<String, User>> cluster, int expectedSize) {
      for (MultimapCache mc : cluster.values()) {
         assertMultimapCacheSize(mc, expectedSize);
      }
   }

   public static void assertContaisKeyValue(MultimapCache<String, User> multimapCache, String key, User value) {
      Address address = ((EmbeddedMultimapCache) multimapCache).getCache().getCacheManager().getAddress();
      await(multimapCache.get(key).thenAccept(v -> {
         assertTrue(format("get method call : multimap '%s' must contain key '%s' value '%s' pair", address, key, value), v.contains(value));
      }));
      await(multimapCache.containsEntry(key, value).thenAccept(v -> {
         assertTrue(format("containsEntry method call : multimap '%s' must contain key '%s' value '%s' pair", address, key, value), v);
      }));
   }

}
