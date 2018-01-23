package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.DataCommand;

/**
 * Mixes features from DataCommand and WriteCommand
 *
 * @author Manik Surtani
 * @deprecated Since 8.3, will be removed.
 */
@Deprecated
public interface DataWriteCommand extends WriteCommand, DataCommand {

   /**
    * @return the {@link CommandInvocationId} associated to the command.
    */
   CommandInvocationId getCommandInvocationId();

   /**
    * Initializes the {@link BackupWriteRpcCommand} to send the update to backup owner of a key.
    * <p>
    * This method will be invoked in the primary owner only.
    *
    * @param command the {@link BackupWriteRpcCommand} to initialize.
    */
   default void initBackupWriteRcpCommand(BackupWriteRpcCommand command) {
      throw new UnsupportedOperationException();
   }

}
