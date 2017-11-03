package org.infinispan.conflict.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.conflict.MergePolicies;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.MergePolicyPreferredNonNullTest")
public class MergePolicyPreferredNonNullTest extends BaseMergePolicyTest {

   @Factory
   public Object[] factory() {
      return new Object[] {
            new MergePolicyPreferredNonNullTest().setPartitions(new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyPreferredNonNullTest().setPartitions(new int[]{0,1}, new int[]{2,3}),
            new MergePolicyPreferredNonNullTest().setPartitions(new int[]{0,1}, new int[]{2})
      };
   }

   public MergePolicyPreferredNonNullTest() {
      super();
      this.mergePolicy = MergePolicies.PREFERRED_NON_NULL;
   }

   @Override
   protected void duringSplit(AdvancedCache preferredPartitionCache, AdvancedCache otherCache) {
      preferredPartitionCache.remove(conflictKey);
      otherCache.put(conflictKey, "DURING SPLIT");
   }
}
