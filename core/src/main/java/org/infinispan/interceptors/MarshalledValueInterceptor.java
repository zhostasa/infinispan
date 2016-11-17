package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * Interceptor that handles the wrapping and unwrapping of cached data using {@link
 * org.infinispan.marshall.core.MarshalledValue}s. Known "excluded" types are not wrapped/unwrapped, which at this time
 * include {@link String}, Java primitives and their Object wrappers, as well as arrays of excluded types.
 * <p/>
 * The {@link org.infinispan.marshall.core.MarshalledValue} wrapper handles lazy deserialization from byte array
 * representations.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @see org.infinispan.marshall.core.MarshalledValue
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class MarshalledValueInterceptor<K, V> extends CommandInterceptor {
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }
}
