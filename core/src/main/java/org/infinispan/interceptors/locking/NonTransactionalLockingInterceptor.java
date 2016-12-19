package org.infinispan.interceptors.locking;

import java.util.function.Predicate;

import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Locking interceptor to be used for non-transactional caches.
 *
 * @author Mircea Markus
 * @deprecated Since 9.0, no longer public API.
 */
public class NonTransactionalLockingInterceptor extends AbstractLockingInterceptor {
   private static final Log log = LogFactory.getLog(NonTransactionalLockingInterceptor.class);

   private final Predicate<Object> shouldLockKey = this::shouldLockKey;

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected final BasicInvocationStage visitDataReadCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      assertNonTransactional(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   protected BasicInvocationStage visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      assertNonTransactional(ctx);
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      assertNonTransactional(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      assertNonTransactional(ctx);
      if (command.isForwarded() || hasSkipLocking(command)) {
         return invokeNext(ctx, command);
      }
      try {
         lockAllAndRecord(ctx, command.getMap().keySet().stream().filter(shouldLockKey), getLockTimeoutMillis(command));
      } catch (Throwable t) {
         lockManager.unlockAll(ctx);
         throw t;
      }
      return invokeNext(ctx, command).handle(unlockAllReturnHandler);
   }

   private void assertNonTransactional(InvocationContext ctx) {
      //this only happens if the cache is used in a transaction's scope
      if (ctx.isInTxScope()) {
         throw new InvalidCacheUsageException(
               "This is a non-transactional cache and cannot be accessed with a transactional InvocationContext.");
      }
   }
}
