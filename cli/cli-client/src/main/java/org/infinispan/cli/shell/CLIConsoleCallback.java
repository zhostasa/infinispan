package org.infinispan.cli.shell;

import org.jboss.aesh.console.AeshConsoleCallback;
import org.jboss.aesh.console.ConsoleCallback;
import org.jboss.aesh.console.ConsoleOperation;
import org.jboss.aesh.console.Process;
import org.jboss.aesh.console.command.CommandOperation;

/**
 * CLIConsoleCallback
 *
 * @author Tristan Tarrant
 * @since 8.3
 */

public class CLIConsoleCallback extends AeshConsoleCallback{
   @Override
   public int execute(ConsoleOperation output) throws InterruptedException {
      return 0;
   }

}
