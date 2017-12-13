package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.MemoryConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.EvictionType;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.interceptors.impl.TransactionalExceptionEvictionInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "eviction.ExceptionEvictionTest")
public class ExceptionEvictionTest extends MultipleCacheManagersTest {
   private static final int SIZE = 10;

   protected ControlledTimeService timeService = new ControlledTimeService(0);

   static class ExceptionEvictionTestBuilder {
      private int nodeCount;
      private boolean optimisticTransaction;
      private StorageType storageType;
      private CacheMode cacheMode;

      public ExceptionEvictionTestBuilder cacheMode(CacheMode cacheMode) {
         this.cacheMode = cacheMode;
         return this;
      }

      public ExceptionEvictionTestBuilder nodeCount(int nodeCount) {
         this.nodeCount = nodeCount;
         return this;
      }

      public ExceptionEvictionTestBuilder storageType(StorageType storageType) {
         this.storageType = storageType;
         return this;
      }

      public ExceptionEvictionTestBuilder optimisticTransaction(boolean optimisticTransaction) {
         this.optimisticTransaction = optimisticTransaction;
         return this;
      }

      public ConfigurationBuilder tweakConfigurationBuilder(ConfigurationBuilder configurationBuilder) {
         MemoryConfigurationBuilder memoryConfigurationBuilder = configurationBuilder.memory();
         memoryConfigurationBuilder.storageType(storageType);
         switch (storageType) {
            case OBJECT:
               memoryConfigurationBuilder.evictionType(EvictionType.COUNT_EXCEPTION).size(SIZE);
               break;
            case BINARY:
               // 64 bytes per entry, however tests that add metadata require 16 more even
               memoryConfigurationBuilder.evictionType(EvictionType.MEMORY_EXCEPTION).size(convertAmountForStorage(this, SIZE) + 16);
               break;
            case OFF_HEAP:
               // Each entry takes up 63 bytes total for our tests, however tests that add expiration require 16 more
               memoryConfigurationBuilder.evictionType(EvictionType.MEMORY_EXCEPTION).size(24 +
                     // If we are running optimistic transactions we have to store version so it is larger than pessimistic
                     convertAmountForStorage(this, SIZE) +
                     UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(memoryConfigurationBuilder.addressCount() << 3));
               break;
         }

         configurationBuilder
               .transaction()
               .transactionMode(TransactionMode.TRANSACTIONAL)
               .lockingMode(optimisticTransaction ? LockingMode.OPTIMISTIC : LockingMode.PESSIMISTIC);
         configurationBuilder
               .clustering()
               .cacheMode(cacheMode)
               .hash()
               // Num owners has to be the same to guarantee amount of entries written
               .numOwners(nodeCount);
         return configurationBuilder;
      }

      public String getName() {
         return cacheMode + "-" + storageType + "-" + nodeCount + "-" + optimisticTransaction;
      }

      @Override
      public String toString() {
         return "Settings=" + getName();
      }
   }

   @DataProvider(name = "caches")
   public static Object[][] cacheBuilders() {
      return new Object[][] {
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.LOCAL).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true) },

            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.LOCAL).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.BINARY).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.BINARY).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true) },

            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.LOCAL).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.OBJECT).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.OBJECT).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true) },

            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.LOCAL).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false) },

            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.LOCAL).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.BINARY).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.BINARY).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false) },

            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.LOCAL).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.OBJECT).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false) },
            { new ExceptionEvictionTestBuilder().nodeCount(3).storageType(StorageType.OBJECT).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false) },
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Object[][] builders = cacheBuilders();
      // This assumes there are at most 3 nodes
      ConfigurationBuilderHolder holder1 = new ConfigurationBuilderHolder();
      holder1.getGlobalConfigurationBuilder().clusteredDefault();
      ConfigurationBuilderHolder holder2 = new ConfigurationBuilderHolder();
      holder2.getGlobalConfigurationBuilder().clusteredDefault();
      ConfigurationBuilderHolder holder3 = new ConfigurationBuilderHolder();
      holder3.getGlobalConfigurationBuilder().clusteredDefault();
      String threeNodeCacheName = null;
      for (Object[] objects : builders) {
         ExceptionEvictionTestBuilder builder = (ExceptionEvictionTestBuilder) objects[0];
            builder.tweakConfigurationBuilder(holder1.newConfigurationBuilder(builder.getName()));
         if (builder.nodeCount == 3) {
            threeNodeCacheName = builder.getName();
            builder.tweakConfigurationBuilder(holder2.newConfigurationBuilder(threeNodeCacheName));
            builder.tweakConfigurationBuilder(holder3.newConfigurationBuilder(threeNodeCacheName));
         }
      }

      addClusterEnabledCacheManager(holder1);

      if (threeNodeCacheName != null) {
         addClusterEnabledCacheManager(holder2);
         addClusterEnabledCacheManager(holder3);

         for (int i = 0; i < 3; ++i) {
            EmbeddedCacheManager manager = manager(i);
            TestingUtil.replaceComponent(manager, TimeService.class, timeService, true);
         }

         waitForClusterToForm(threeNodeCacheName);
      } else {
         EmbeddedCacheManager manager = manager(0);
         TestingUtil.replaceComponent(manager, TimeService.class, timeService, true);
      }
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      // Do nothing - this is handled in below clear content
   }

   @AfterMethod
   protected void clearContent(ITestResult tr) throws Throwable {
      super.clearContent();
      ExceptionEvictionTestBuilder builder = ((ExceptionEvictionTestBuilder) tr.getParameters()[0]);

      List<Cache<Object, Object>> caches = getCaches(builder);
      // Call actual clear to reset interceptor counter
      for (Cache cache : caches) {
         cache.clear();
      }

      for (Cache cache : caches) {
         long pendingTransactionCount = cache.getAdvancedCache().getAsyncInterceptorChain().findInterceptorWithClass(
               TransactionalExceptionEvictionInterceptor.class).pendingTransactionCount();
         assertEquals(0, pendingTransactionCount);
      }
   }

   Throwable getMostNestedSuppressedThrowable(Throwable t) {
      Throwable nested = getNestedThrowable(t);
      Throwable[] suppressedNested = nested.getSuppressed();
      if (suppressedNested.length > 0) {
         nested = getNestedThrowable(suppressedNested[0]);
      }
      return nested;
   }

   Throwable getNestedThrowable(Throwable t) {
      Throwable cause;
      while ((cause = t.getCause()) != null) {
         t = cause;
      }
      return t;
   }

   static long convertAmountForStorage(ExceptionEvictionTestBuilder builder, long expected) {
      switch (builder.storageType) {
         case OBJECT:
            return expected;
         case BINARY:
            return expected * (builder.optimisticTransaction ? 64 : 24);
         case OFF_HEAP:
            return expected * (builder.optimisticTransaction ? UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(63) :
                  UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(48));
         default:
            throw new IllegalStateException("Unconfigured storage type: " + builder.storageType);
      }
   }

   /**
    * Asserts that number of entries worth of counts is stored in the interceptors
    * @param entryCount
    */
   void assertInterceptorCount(ExceptionEvictionTestBuilder builder, long entryCount) {
      entryCount = convertAmountForStorage(builder, entryCount);
      long currentCount = 0;
      for (Cache cache : getCaches(builder)) {
         TransactionalExceptionEvictionInterceptor interceptor = cache.getAdvancedCache().getAsyncInterceptorChain()
               .findInterceptorWithClass(TransactionalExceptionEvictionInterceptor.class);
         currentCount += interceptor.getCurrentSize();
         entryCount += interceptor.getMinSize();
      }

      assertEquals(entryCount, currentCount);
   }

   <K, V> List<Cache<K, V>> getCaches(ExceptionEvictionTestBuilder builder) {
      if (builder.nodeCount == 3) {
         return caches(builder.getName());
      } else {
         return Collections.singletonList(cache(0, builder.getName()));
      }
   }

   @Test(dataProvider = "caches")
   public void testExceptionOnInsert(ExceptionEvictionTestBuilder builder) {
      String cacheName = builder.getName();
      for (int i = 0; i < SIZE; ++i) {
         cache(0, cacheName).put(i, i);
      }

      try {
         cache(0, cacheName).put(-1, -1);
         fail("Should have thrown an exception!");
      } catch (Throwable t) {
         Exceptions.assertException(IllegalArgumentException.class, getMostNestedSuppressedThrowable(t));
      }
   }

   @Test(dataProvider = "caches")
   public void testExceptionOnInsertFunctional(ExceptionEvictionTestBuilder builder) {
      String cacheName = builder.getName();
      for (int i = 0; i < SIZE; ++i) {
         cache(0, cacheName).computeIfAbsent(i, k -> SIZE);
      }

      try {
         cache(0, cacheName).computeIfAbsent(-1, k -> SIZE);
         fail("Should have thrown an exception!");
      } catch (Throwable t) {
         Exceptions.assertException(IllegalArgumentException.class, getMostNestedSuppressedThrowable(t));
      }
   }

   @Test(dataProvider = "caches")
   public void testExceptionOnInsertWithRemove(ExceptionEvictionTestBuilder builder) {
      String cacheName = builder.getName();
      for (int i = 0; i < SIZE; ++i) {
         cache(0, cacheName).put(i, i);
      }

      // Now we should have an extra space
      cache(0, cacheName).remove(0);

      // Have to use a cached Integer value otherwise this will blosw up as too large
      cache(0, cacheName).put(-128, -128);

      try {
         cache(0, cacheName).put(-1, -1);
         fail("Should have thrown an exception!");
      } catch (Throwable t) {
         Exceptions.assertException(IllegalArgumentException.class, getMostNestedSuppressedThrowable(t));
      }
   }

   @Test(dataProvider = "caches")
   public void testNoExceptionWhenReplacingEntry(ExceptionEvictionTestBuilder builder) {
      String cacheName = builder.getName();
      for (int i = 0; i < SIZE; ++i) {
         cache(0, cacheName).put(i, i);
      }

      // This should pass just fine
      cache(0, cacheName).put(0, 0);
   }

   @Test(dataProvider = "caches")
   public void testNoExceptionAfterRollback(ExceptionEvictionTestBuilder builder) throws SystemException, NotSupportedException {
      String cacheName = builder.getName();
      int nodeCount = builder.nodeCount;
      // We only inserted 9
      for (int i = 1; i < SIZE; ++i) {
         cache(0, cacheName).put(i, i);
      }

      assertInterceptorCount(builder, nodeCount * (SIZE - 1));

      TransactionManager tm = cache(0, cacheName).getAdvancedCache().getTransactionManager();
      tm.begin();

      cache(0, cacheName).put(0, 0);

      tm.rollback();

      assertInterceptorCount(builder, nodeCount * (SIZE - 1));

      assertNull(cache(0, cacheName).get(0));

      cache(0, cacheName).put(SIZE + 1, SIZE + 1);

      assertInterceptorCount(builder, nodeCount * SIZE);

      // This should fail now
      try {
         cache(0, cacheName).put(-1, -1);
         fail("Should have thrown an exception!");
      } catch (Throwable t) {
         Exceptions.assertException(IllegalArgumentException.class, getMostNestedSuppressedThrowable(t));
      }

      assertInterceptorCount(builder, nodeCount * SIZE);
   }

   /**
    * This test verifies that an insert would have caused an exception, but because the user rolled back it wasn't
    * an issue
    * @throws SystemException
    * @throws NotSupportedException
    */
   @Test(dataProvider = "caches")
   public void testRollbackPreventedException(ExceptionEvictionTestBuilder builder) throws SystemException, NotSupportedException {
      String cacheName = builder.getName();
      // Insert all 10
      for (int i = 0; i < SIZE; ++i) {
         cache(0, cacheName).put(i, i);
      }

      TransactionManager tm = cache(0, cacheName).getAdvancedCache().getTransactionManager();
      tm.begin();

      try {
         cache(0, cacheName).put(SIZE + 1, SIZE + 1);
      } finally {
         tm.rollback();
      }

      assertNull(cache(0, cacheName).get(SIZE + 1));
   }

   /**
    * This test verifies that if there are multiple entries that would cause an overflow to occur. Only one entry
    * would not cause an overflow, so this is specifically for when there is more than 1.
    * @throws SystemException
    * @throws NotSupportedException
    * @throws HeuristicRollbackException
    * @throws HeuristicMixedException
    */
   @Test(dataProvider = "caches")
   public void testExceptionWithCommitMultipleEntries(ExceptionEvictionTestBuilder builder) throws SystemException, NotSupportedException,
         HeuristicRollbackException, HeuristicMixedException {
      String cacheName = builder.getName();
      // We only inserted 9
      for (int i = 1; i < SIZE; ++i) {
         cache(0, cacheName).put(i, i);
      }

      TransactionManager tm = cache(0, cacheName).getAdvancedCache().getTransactionManager();
      tm.begin();

      try {
         cache(0, cacheName).put(0, 0);
         cache(0, cacheName).put(SIZE + 1, SIZE + 1);
      } catch (Throwable t) {
         tm.setRollbackOnly();
         throw t;
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) {
            try {
               tm.commit();
               fail("Should have thrown an exception!");
            } catch (RollbackException e) {
               Exceptions.assertException(IllegalArgumentException.class, getMostNestedSuppressedThrowable(e));
            }
         }
         else {
            tm.rollback();
            fail("Transaction was no longer active!");
         }
      }
   }

   /**
    * This tests to verify that when an entry is expired and removed from the data container that it properly updates
    * the current count
    */
   @Test(dataProvider = "caches")
   public void testOnEntryExpiration(ExceptionEvictionTestBuilder builder) {
      String cacheName = builder.getName();
      cache(0, cacheName).put(0, 0, 10, TimeUnit.SECONDS);

      for (int i = 1; i < SIZE; ++i) {
         cache(0, cacheName).put(i, i);
      }

      timeService.advance(TimeUnit.SECONDS.toMillis(11));

      // This should eventually expire all entries
      assertNull(cache(0, cacheName).get(0));

      // Off heap doesn't expire entries on access yet ISPN-8380
      if (builder.storageType == StorageType.OFF_HEAP) {
         for (Cache cache : getCaches(builder)) {
            ExpirationManager em = TestingUtil.extractComponent(cache, ExpirationManager.class);
            em.processExpiration();
         }
      }

      // Entry should be completely removed at some point - note that expired entries, that haven't been removed, still
      // count against counts
      for (Cache cache : getCaches(builder)) {
         eventually(() -> cache.getAdvancedCache().getDataContainer().peek(0) == null);
      }

      // This insert should work now
      cache(0, cacheName).put(-128, -128);

      // This should fail now
      try {
         cache(0, cacheName).put(-1, -1);
         fail("Should have thrown an exception!");
      } catch (Throwable t) {
         Exceptions.assertException(IllegalArgumentException.class, getMostNestedSuppressedThrowable(t));
      }
   }

   @Test(dataProvider = "caches")
   public void testDistributedOverflowOnPrimary(ExceptionEvictionTestBuilder builder) {
      testDistributedOverflow(builder, true);
   }

   @Test(dataProvider = "caches")
   public void testDistributedOverflowOnBackup(ExceptionEvictionTestBuilder builder) {
      testDistributedOverflow(builder, false);
   }

   void testDistributedOverflow(ExceptionEvictionTestBuilder builder, boolean onPrimary) {
      int nodeCount = builder.nodeCount;
      if (!builder.cacheMode.isDistributed() || nodeCount < 3) {
         // Ignore the test if it isn't distributed and doesn't have at least 3 nodes
         return;
      }

      String cacheName = builder.getName();

      for (int i = 0; i < 2; ++i) {
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
         holder.getGlobalConfigurationBuilder().clusteredDefault();
         builder.tweakConfigurationBuilder(holder.newConfigurationBuilder(cacheName));
         addClusterEnabledCacheManager(holder);
      }

      try {
         waitForClusterToForm(cacheName);

         LocalizedCacheTopology lct = cache(0, cacheName).getAdvancedCache().getDistributionManager().getCacheTopology();
         DataConversion dc = cache(0, cacheName).getAdvancedCache().getKeyDataConversion();

         int minKey = -128;
         int nextKey = minKey;
         Address targetNode;
         Iterator<Address> owners = lct.getWriteOwners(dc.toStorage(nextKey)).iterator();
         if (onPrimary) {
            targetNode = owners.next();
         } else {
            // Skip first one
            owners.next();
            targetNode = owners.next();
         }


         cache(0, cacheName).put(nextKey, nextKey);

         // This will fill up the cache with entries that all map to owners
         for (int i = 0; i < SIZE - 1; ++i) {
            nextKey = getNextIntWithOwners(nextKey, lct, dc, targetNode, null);
            log.fatal("Inserting for key: " + nextKey);
            cache(0, cacheName).put(nextKey, nextKey);
         }

         // We should have interceptor count equal to number of owners times how much storage takes up
         assertInterceptorCount(builder, nodeCount * SIZE);

         for (Cache cache : getCaches(builder)) {
            if (targetNode.equals(cache.getCacheManager().getAddress())) {
               assertEquals(10, cache.getAdvancedCache().getDataContainer().size());
               break;
            }
         }

         nextKey = getNextIntWithOwners(nextKey, lct, dc, targetNode, onPrimary);
         try {
            cache(0, cacheName).put(nextKey, nextKey);
            fail("Should have thrown an exception!");
         } catch (Throwable t) {
            Exceptions.assertException(IllegalArgumentException.class, getMostNestedSuppressedThrowable(t));
         }

         // Now that it partially failed it should have rolled back all the results
         assertInterceptorCount(builder, nodeCount * SIZE);
      } finally {
         killMember(3, cacheName);
         killMember(3, cacheName);
      }
   }

   /**
    *
    * @param exclusiveValue
    * @param lct
    * @param dc
    * @param ownerAddress
    * @param primary
    * @return
    */
   int getNextIntWithOwners(int exclusiveValue, LocalizedCacheTopology lct, DataConversion dc,
         Address ownerAddress, Boolean primary) {
      if (exclusiveValue < -128) {
         throw new IllegalArgumentException("We cannot support integers smaller than -128 as they will throw off BINARY sizing");
      }

      int valueToTest = exclusiveValue;
      while (true) {
         valueToTest = valueToTest + 1;
         // Unfortunately we can't generate values higher than 128
         if (valueToTest >= 128) {
            throw new IllegalStateException("Could not generate a key with the given owners");
         }
         Object keyAsStorage = dc.toStorage(valueToTest);

         DistributionInfo di = lct.getDistribution(keyAsStorage);
         if (primary == null) {
            if (di.writeOwners().contains(ownerAddress)) {
               return valueToTest;
            }
         } else if (primary == Boolean.TRUE) {
            if (di.primary().equals(ownerAddress)) {
               return valueToTest;
            }
         } else {
            if (di.writeOwners().contains(ownerAddress)) {
               return valueToTest;
            }
         }
      }
   }

   /**
    * Test to make sure the counts are properly updated after adding and taking down nodes
    */
   @Test(dataProvider = "caches")
   public void testSizeCorrectWithStateTransfer(ExceptionEvictionTestBuilder builder) {
      int nodeCount = builder.nodeCount;
      CacheMode cacheMode = builder.cacheMode;
      // Test only works with REPL or DIST (latter only if numOwners > 1)
      if (!cacheMode.isClustered() || cacheMode.isDistributed() && nodeCount == 1) {
         return;
      }

      String cacheName = builder.getName();
      for (int i = 0; i < SIZE; ++i) {
         cache(0, cacheName).put(i, i);
      }

      int numberToKill = 0;

      assertInterceptorCount(builder, nodeCount * SIZE);

      try {
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
         holder.getGlobalConfigurationBuilder().clusteredDefault();
         builder.tweakConfigurationBuilder(holder.newConfigurationBuilder(cacheName));
         addClusterEnabledCacheManager(holder);

         waitForClusterToForm(cacheName);

         numberToKill++;

         boolean dist = cacheMode.isDistributed();
         assertInterceptorCount(builder, (dist ? nodeCount : nodeCount + 1) * SIZE);

         holder = new ConfigurationBuilderHolder();
         holder.getGlobalConfigurationBuilder().clusteredDefault();
         builder.tweakConfigurationBuilder(holder.newConfigurationBuilder(cacheName));
         addClusterEnabledCacheManager(holder);

         waitForClusterToForm(cacheName);

         numberToKill++;

         assertInterceptorCount(builder, (dist ? nodeCount : nodeCount + 2) * SIZE);

         killMember(nodeCount, cacheName);

         numberToKill--;

         assertInterceptorCount(builder, (dist ? nodeCount : nodeCount + 1) * SIZE);

         killMember(nodeCount, cacheName);

         numberToKill--;

         assertInterceptorCount(builder, nodeCount * SIZE);
      } finally {
         for (int i = 0; i < numberToKill; ++i) {
            killMember(nodeCount, cacheName);
         }
      }
   }
}
