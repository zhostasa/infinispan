package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.functional.ParamsCommand;
import org.infinispan.functional.impl.Params;
import org.infinispan.util.ByteString;

/**
 * A base {@link BackupWriteCommand} used by {@link ParamsCommand}.
 *
 * @author Pedro Ruivo
 * @since 8.4
 */
abstract class FunctionalBackupWriteCommand extends BackupWriteCommand {

   Object function;
   Params params;

   FunctionalBackupWriteCommand(ByteString cacheName) {
      super(cacheName);
   }

   final void writeFunctionAndParams(ObjectOutput output) throws IOException {
      output.writeObject(function);
      Params.writeObject(output, params);
   }

   final void readFunctionAndParams(ObjectInput input) throws IOException, ClassNotFoundException {
      function = input.readObject();
      params = Params.readObject(input);
   }

   final void setFunctionalCommand(ParamsCommand command) {
      this.params = command.getParams();
   }
}
