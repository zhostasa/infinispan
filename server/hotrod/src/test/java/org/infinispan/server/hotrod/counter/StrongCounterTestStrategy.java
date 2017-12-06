package org.infinispan.server.hotrod.counter;

import java.lang.reflect.Method;

import org.infinispan.counter.api.StrongCounter;

/**
 * The {@link StrongCounter} API test interface.
 *
 * @author Pedro Ruivo
 * @since 8.5
 */
public interface StrongCounterTestStrategy extends BaseCounterTestStrategy {

   void testCompareAndSet(Method method);

   void testCompareAndSwap(Method method);

   void testBoundaries(Method method);

   void testListenerWithBounds(Method method) throws InterruptedException;

}
