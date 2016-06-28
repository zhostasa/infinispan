package org.infinispan.commands.module;

import org.infinispan.commands.remote.CacheRpcCommand;

/**
 * This class is provided for compatibility between Infinispan 7.x and 8.x
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @deprecated Since 8.0, will be removed.
 */
@Deprecated
public interface ExtendedModuleCommandFactory extends ModuleCommandFactory {
}
