package org.infinispan.commons.util;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collection;

import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 8.3
 */

@Test(testName = "util.ServiceFinderTest", groups = "functional")
public class ServiceFinderTest {

   public void testDuplicateServiceFinder() {
      ClassLoader mainClassLoader = this.getClass().getClassLoader();
      ClassLoader otherClassLoader = new ClonedClassLoader(mainClassLoader);
      Collection<SampleSPI> spis = ServiceFinder.load(SampleSPI.class, mainClassLoader, otherClassLoader);
      assertEquals(1, spis.size());
   }

   public static class ClonedClassLoader extends ClassLoader {
      public ClonedClassLoader(ClassLoader cl) {
         super(cl);
      }
   }

}
