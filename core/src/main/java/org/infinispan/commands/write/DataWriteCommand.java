package org.infinispan.commands.write;

import org.infinispan.commands.DataCommand;

/**
 * Mixes features from DataCommand and WriteCommand
 *
 * @author Manik Surtani
 * @deprecated Since 8.3, will be removed.
 */
@Deprecated
public interface DataWriteCommand extends WriteCommand, DataCommand {
}
