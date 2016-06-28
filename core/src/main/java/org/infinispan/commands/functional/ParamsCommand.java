package org.infinispan.commands.functional;

import org.infinispan.functional.impl.Params;

/**
 * A command that carries parameters.
 * @deprecated Since 8.3, will be removed.
 */
@Deprecated
public interface ParamsCommand {

   Params getParams();

}
