package org.infinispan.multimap.impl;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistributedMultimapCacheL1DisabledTest")
public class DistributedMultimapCacheL1DisabledTest extends DistributedMultimapCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      l1CacheEnabled = false;
      super.createCacheManagers();
   }
}
