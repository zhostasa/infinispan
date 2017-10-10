package org.infinispan.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockKillingNodesTest")
public class ClusteredLockKillingNodesTest extends BaseClusteredLockTest {

   protected static final String LOCK_NAME = "ClusteredLockKillingNodesTest";

   @BeforeMethod(alwaysRun = true)
   public void createLock() throws Throwable {
      ClusteredLockManager firstLockOwner = clusteredLockManager(0);
      firstLockOwner.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
   }

   @AfterMethod(alwaysRun = true)
   protected void destroyLock() {
      ClusteredLockManager firstLockOwner = clusteredLockManager(0);
      await(firstLockOwner.remove(LOCK_NAME));
   }

   @Test
   public void testLockWithAcquisitionAndKill() throws Throwable {
      ClusteredLock secondLockOwner = clusteredLockManager(1).get(LOCK_NAME);
      ClusteredLock thirdLockOwner = clusteredLockManager(2).get(LOCK_NAME);

      StringBuilder value = new StringBuilder();
      await(secondLockOwner.lock().thenRun(() -> {
         stopCacheManager(1);
         await(thirdLockOwner.tryLock(1, TimeUnit.SECONDS).thenAccept(r -> {
            if (r) {
               value.append("hello");
            }
         }));
      }));

      assertEquals(value.toString(), "hello");
   }

   private void stopCacheManager(int cm) {
      try {
         manager(cm).stop();
         TimeUnit.MILLISECONDS.sleep(1);
      } catch (InterruptedException e) {
      }
   }

}
