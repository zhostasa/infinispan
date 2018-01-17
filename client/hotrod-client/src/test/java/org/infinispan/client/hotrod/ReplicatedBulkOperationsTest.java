package org.infinispan.client.hotrod;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "org.infinispan.client.hotrod.ReplicatedBulkOperationsTest")
public class ReplicatedBulkOperationsTest extends AbstractBulkOperationsTest {
   public ReplicatedBulkOperationsTest() {
      super(CacheMode.REPL_SYNC);
   }
}
