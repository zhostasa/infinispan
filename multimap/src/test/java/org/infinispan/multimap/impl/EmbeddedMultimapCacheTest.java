package org.infinispan.multimap.impl;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.KOLDO;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.infinispan.multimap.impl.MultimapTestUtils.putValuesOnMultimapCache;
import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.MultimapCache;
import org.infinispan.multimap.api.MultimapCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Single Multimap Cache Test
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Test(groups = "functional", testName = "multimap.EmbeddedMultimapCacheTest")
public class EmbeddedMultimapCacheTest extends SingleCacheManagerTest {
   protected MultimapCache<String, User> multimapCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(false);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(true);
      MultimapCacheManager multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cm);
      multimapCache = multimapCacheManager.get("test");
      return cm;
   }

   public void testSupportsDuplicates() {
      assertFalse(multimapCache.supportsDuplicates());
   }

   public void testPut() {
      await(
            multimapCache.put(NAMES_KEY, JULIEN)
                  .thenCompose(r1 -> multimapCache.put(NAMES_KEY, OIHANA))
                  .thenCompose(r2 -> multimapCache.put(NAMES_KEY, JULIEN))
                  .thenCompose(r3 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v -> {
                           assertTrue(v.contains(JULIEN));
                           assertEquals(1, v.stream().filter(n -> n.equals(JULIEN)).count());
                        }
                  )

      );

      await(
            multimapCache.get(NAMES_KEY).thenAccept(v -> {
                     assertFalse(v.contains(KOLDO));
                     assertEquals(2, v.size());
                  }
            )
      );
   }

   public void testRemoveKey() {
      String key = NAMES_KEY;

      await(
            multimapCache.put(key, OIHANA)
                  .thenCompose(r1 -> multimapCache.size())
                  .thenAccept(s -> assertEquals(1, s.intValue()))
      );

      await(
            multimapCache.remove(key, OIHANA).thenCompose(r1 -> {
               assertTrue(r1);
               return multimapCache.get(key).thenAccept(v -> assertTrue(v.isEmpty()));
            })
      );

      await(multimapCache.remove(key).thenAccept(r -> assertFalse(r)));
   }

   public void testRemoveKeyValue() {

      await(
            multimapCache.put(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> multimapCache.size())
                  .thenAccept(s -> assertEquals(1, s.intValue()))
      );

      await(multimapCache.remove("unexistingKey", OIHANA).thenAccept(r -> assertFalse(r)));

      await(
            multimapCache.remove(NAMES_KEY, JULIEN).thenCompose(r1 -> {
                     assertFalse(r1);
                     return multimapCache.get(NAMES_KEY).thenAccept(v -> assertEquals(1, v.size()));
                  }
            )
      );

      await(
            multimapCache.remove(NAMES_KEY, OIHANA).thenCompose(r1 -> {
                     assertTrue(r1);
                     return multimapCache.get(NAMES_KEY).thenAccept(v -> assertTrue(v.isEmpty()));
                  }
            )
      );

      await(multimapCache.size().thenAccept(s -> assertEquals(0, s.intValue())));
   }

   public void testRemoveWithPredicate() {

      await(
            multimapCache.put(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> multimapCache.put(NAMES_KEY, JULIEN))
                  .thenCompose(r2 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v -> assertEquals(2, v.size()))
      );

      await(
            multimapCache.remove(o -> o.getName().contains("Ka"))
                  .thenCompose(r1 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v ->
                        assertEquals(2, v.size())

                  )
      );

      await(
            multimapCache.remove(o -> o.getName().contains("Ju"))
                  .thenCompose(r1 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v ->
                        assertEquals(1, v.size())
                  )
      );

      await(
            multimapCache.remove(o -> o.getName().contains("Oi"))
                  .thenCompose(r1 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v ->
                        assertTrue(v.isEmpty())
                  )
      );
   }

   public void testGetAndModifyResults() {
      User pepe = new User("Pepe", 17);

      await(
            multimapCache.put(NAMES_KEY, JULIEN)
                  .thenCompose(r1 -> multimapCache.put(NAMES_KEY, OIHANA))
                  .thenCompose(r2 -> multimapCache.put(NAMES_KEY, RAMON))
                  .thenCompose(r3 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v -> {
                           assertEquals(3, v.size());
                           List<User> modifiedList = new ArrayList<>(v);
                           modifiedList.add(pepe);
                           assertEquals(3, v.size());
                           assertEquals(4, modifiedList.size());
                        }
                  )

      );

      await(
            multimapCache.get(NAMES_KEY).thenAccept(v -> {
                     assertFalse(v.contains(pepe));
                     assertEquals(3, v.size());
                  }
            )
      );
   }

   public void testSize() {
      String anotherKey = "firstNames";

      await(
            multimapCache.put(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> multimapCache.put(NAMES_KEY, JULIEN))
                  .thenCompose(r2 -> multimapCache.put(anotherKey, OIHANA))
                  .thenCompose(r3 -> multimapCache.put(anotherKey, JULIEN))
                  .thenCompose(r4 -> multimapCache.size())
                  .thenAccept(s -> assertEquals(4, s.intValue()))
      );
   }

   public void testContainsKey() {
      await(
            multimapCache.containsKey(NAMES_KEY)
                  .thenAccept(containsKey -> assertFalse(containsKey))
      );

      await(
            multimapCache.put(NAMES_KEY, OIHANA)
                  .thenCompose(r -> multimapCache.containsKey(NAMES_KEY))
                  .thenAccept(containsKey -> assertTrue(containsKey))
      );
   }

   public void testContainsValue() {
      await(
            multimapCache.containsValue(OIHANA)
                  .thenAccept(containsValue -> assertFalse(containsValue))
      );

      putValuesOnMultimapCache(multimapCache, NAMES_KEY, OIHANA, JULIEN, RAMON, KOLDO);

      await(
            multimapCache.containsValue(RAMON)
                  .thenAccept(containsValue -> assertTrue(containsValue))
      );
   }

   public void testContainEntry() {
      await(
            multimapCache.containsEntry(NAMES_KEY, OIHANA)
                  .thenAccept(containsEntry -> assertFalse(containsEntry))
      );

      await(
            multimapCache.put(NAMES_KEY, OIHANA)
                  .thenCompose(r -> multimapCache.containsEntry(NAMES_KEY, OIHANA))
                  .thenAccept(containsEntry -> assertTrue(containsEntry))
      );

      await(
            multimapCache.put(NAMES_KEY, OIHANA)
                  .thenCompose(r -> multimapCache.containsEntry(NAMES_KEY, JULIEN))
                  .thenAccept(containsEntry -> assertFalse(containsEntry))
      );
   }

   public void testGetEntry() {
      await(
            multimapCache.getEntry(NAMES_KEY)
                  .thenAccept(maybeEntry -> {
                           assertFalse(NAMES_KEY, maybeEntry.isPresent());
                        }
                  )

      );

      await(
            multimapCache.put(NAMES_KEY, JULIEN)
                  .thenCompose(r3 -> multimapCache.getEntry(NAMES_KEY))
                  .thenAccept(maybeEntry -> {
                           assertTrue(NAMES_KEY, maybeEntry.isPresent());
                           CacheEntry<String, Collection<User>> entry = maybeEntry.get();
                           assertEquals(NAMES_KEY, entry.getKey());
                           assertTrue(entry.getValue().contains(JULIEN));
                        }
                  )
      );
   }

   public void testPutWithNull() {
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.put(null, OIHANA));
      expectException(NullPointerException.class, "value can't be null", () -> multimapCache.put(NAMES_KEY, null));
   }

   public void testGetWithNull() {
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.get(null));
   }

   public void testGetEntryWithNull() {
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.getEntry(null));
   }

   public void testRemoveKeyValueWithNull() {
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.remove(null, RAMON));
      expectException(NullPointerException.class, "value can't be null", () -> multimapCache.remove(NAMES_KEY, null));
   }

   public void testRemoveKeyWithNulll() {
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.remove((String) null));
   }

   public void testRemoveWithNullPredicate() {
      expectException(NullPointerException.class, "predicate can't be null", () -> multimapCache.remove((Predicate<? super User>) null));
   }

   public void testContainsKeyWithNull() {
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.containsKey(null));
   }

   public void testContainsValueWithNull() {
      expectException(NullPointerException.class, "value can't be null", () -> multimapCache.containsValue(null));
   }

   public void testContainsEntryWithNull() {
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.containsEntry(null, OIHANA));
      expectException(NullPointerException.class, "value can't be null", () -> multimapCache.containsEntry(NAMES_KEY, null));
   }
}
