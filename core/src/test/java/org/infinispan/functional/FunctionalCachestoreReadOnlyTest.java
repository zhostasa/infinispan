package org.infinispan.functional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.manager.PersistenceManager;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; && Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.FunctionalCachestoreReadOnlyTest")
public class FunctionalCachestoreReadOnlyTest extends AbstractFunctionalReadOnlyOpTest {

   @Test(dataProvider = "owningModeAndMethod")
   public void testLoad(boolean isSourceOwner, Method method) {
      Object key = getKey(isSourceOwner);
      List<Cache<Object, Object>> owners = caches(DIST).stream()
            .filter(cache -> cache.getAdvancedCache().getDistributionManager().getLocality(key).isLocal())
            .collect(Collectors.toList());

      method.action.eval(key, ro,
            (Consumer<ReadEntryView<Object, String>> & Serializable) view -> assertFalse(view.find().isPresent()));

      // we can't add from read-only cache, so we put manually:
      cache(0, DIST).put(key, "value");

      caches(DIST).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      caches(DIST).forEach(cache -> cache.evict(key));
      caches(DIST).forEach(cache -> assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString()));
      owners.forEach(cache -> {
         Set<DummyInMemoryStore> stores = cache.getAdvancedCache().getComponentRegistry().getComponent(PersistenceManager.class).getStores(DummyInMemoryStore.class);
         DummyInMemoryStore store = stores.iterator().next();
         assertTrue(store.contains(key), getAddress(cache).toString());
      });

      method.action.eval(key, ro,
            (Consumer<ReadEntryView<Object, String>> & Serializable) view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
            });
   }
}
