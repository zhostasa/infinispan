package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests to make sure that when expiration occurs it occurs across the cluster when in replication mode
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "expiration.impl.ClusterExpirationReplFunctionalTest")
public class ClusterExpirationReplFunctionalTest extends ClusterExpirationFunctionalTest {

   @Override
   protected void createCluster(ConfigurationBuilder builder, int count) {
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      super.createCluster(builder, count);
   }
}
