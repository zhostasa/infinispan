package org.infinispan.rest.operations.mediatypes.impl;

import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.rest.operations.mediatypes.Charset;
import org.infinispan.rest.operations.mediatypes.OutputPrinter;
import org.infinispan.stream.CacheCollectors;

/**
 * {@link OutputPrinter} for binary values.
 *
 * @author Sebastian Łaskawiec
 */
public class BinaryOutputPrinter implements OutputPrinter {

   @Override
   public byte[] print(String cacheName, CacheSet<?> keys, Charset charset) {
      return keys.stream()
            .map(b -> b.toString())
            .collect(CacheCollectors.serializableCollector(() -> Collectors.joining(",", "[", "]")))
            .getBytes(charset.getJavaCharset());
   }
}
