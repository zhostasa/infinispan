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
    * Create the {@link BackupWriteCommand} to send to the backup owners.
    * <p>
    * The primary owner is the only member which creates the command to send it.
    *
    * @return the {@link BackupWriteCommand} to send to the backup owners.
    */
   default BackupWriteCommand createBackupWriteCommand() {
      throw new UnsupportedOperationException();
   }

   /**
    * Initializes the primary owner acknowledges with the return value and if it is successful or not.
    *
    * @param command     the {@link PrimaryAckCommand} to initialize.
    * @param returnValue the local return value.
    */
   default void initPrimaryAck(PrimaryAckCommand command, Object returnValue) {
      throw new UnsupportedOperationException();
   }

}
