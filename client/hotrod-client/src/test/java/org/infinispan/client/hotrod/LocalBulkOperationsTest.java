package org.infinispan.client.hotrod;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "org.infinispan.client.hotrod.LocalBulkOperationsTest")
public class LocalBulkOperationsTest extends AbstractBulkOperationsTest {
   public LocalBulkOperationsTest() {
      super(CacheMode.LOCAL);
   }
}
