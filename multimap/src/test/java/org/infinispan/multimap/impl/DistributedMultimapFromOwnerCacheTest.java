package org.infinispan.multimap.impl;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistributedMultimapFromOwnerCacheTest")
public class DistributedMultimapFromOwnerCacheTest extends DistributedMultimapCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      fromOwner = true;
      super.createCacheManagers();
   }
}
