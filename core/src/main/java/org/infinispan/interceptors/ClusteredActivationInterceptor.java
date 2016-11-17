package org.infinispan.interceptors;

/**
 * The same as a regular cache loader interceptor, except that it contains additional logic to force loading from the
 * cache loader if needed on a remote node, in certain conditions.
 *
 * @author Manik Surtani
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class ClusteredActivationInterceptor extends ActivationInterceptor {
}
