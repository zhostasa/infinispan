package org.infinispan.counter.impl;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

/**
 * @author Pedro Ruivo
 * @since 8.5
 */
@MetaInfServices
public class CounterMetadataFileFinder implements ModuleMetadataFileFinder {

   @Override
   public String getMetadataFilename() {
      return "infinispan-clustered-counter-component-metadata.dat";
   }
}
