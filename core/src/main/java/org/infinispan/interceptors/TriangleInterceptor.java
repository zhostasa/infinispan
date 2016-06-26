package org.infinispan.interceptors;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.BackupWriteCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PrimaryAckCommand;
import org.infinispan.commands.write.PrimaryPutMapAckCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * It handles the acknowledges for the triangle algorithm (distributed mode only!)
 * <p>
 * It is placed between the {@link org.infinispan.statetransfer.StateTransferInterceptor} and the {@link
 * org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor}.
 * <p>
 * The acknowledges are sent while the lock isn't acquire and it interacts with the {@link
 * org.infinispan.statetransfer.StateTransferInterceptor} to trigger the retries when topology changes while the
 * commands are processing.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleInterceptor extends DDAsyncInterceptor {

   private static final Log log = LogFactory.getLog(TriangleInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private CommandAckCollector commandAckCollector;
   private DistributionManager distributionManager;

   private Address localAddress;
   private long timeoutNanoseconds;

   private static Collection<Integer> calculateSegments(Collection<?> keys, ConsistentHash ch) {
      return keys.stream().map(ch::getSegment).collect(Collectors.toSet());
   }

   @Inject
   public void inject(RpcManager rpcManager, CommandsFactory commandsFactory, CommandAckCollector commandAckCollector,
                      DistributionManager distributionManager) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.commandAckCollector = commandAckCollector;
      this.distributionManager = distributionManager;
   }

   @Start
   public void start() {
      localAddress = rpcManager.getAddress();
      RpcOptions options = rpcManager.getDefaultRpcOptions(true);
      timeoutNanoseconds = options.timeUnit().toNanos(options.timeout());
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommands(ctx, command);
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommands(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommands(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return handleLocalPutMapCommand(ctx, command);
      } else {
         return handleRemotePutMapCommand(ctx, command);
      }
   }

   @Override
   public BasicInvocationStage visitBackupWriteCommand(InvocationContext ctx, BackupWriteCommand command) throws Throwable {
      return invokeNext(ctx, command).handle(this::onBackupCommand);
   }

   private BasicInvocationStage handleRemotePutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> {
         final PutMapCommand cmd = (PutMapCommand) rCommand;
         if (t != null) {
            sendExceptionAck(cmd.getCommandInvocationId(), cmd.getTopologyId(), t);
            return stage;
         }
         final Collection<Integer> segments = calculateSegments(cmd.getAffectedKeys(),
               distributionManager.getWriteConsistentHash());
         if (cmd.isForwarded()) {
            sendPutMapBackupAck(cmd, segments);
         } else {
            sendPrimaryPutMapAck(cmd, segments, (Map<Object, Object>) rv);
         }
         return stage;
      });
   }

   private BasicInvocationStage handleLocalPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, throwable) -> {
         final PutMapCommand cmd = (PutMapCommand) rCommand;
         if (throwable != null) {
            disposeCollectorOnException(cmd.getCommandInvocationId());
            return stage;
         }
         return waitCollector(cmd.getCommandInvocationId(), rv);
      });
   }


   private BasicInvocationStage handleWriteCommands(InvocationContext ctx, DataWriteCommand command) {
      return invokeNext(ctx, command).compose(this::onWriteCommand);
   }

   private BasicInvocationStage onWriteCommand(BasicInvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable t) throws Throwable {
      final DataWriteCommand cmd = (DataWriteCommand) rCommand;
      final CommandInvocationId id = cmd.getCommandInvocationId();
      if (rCtx.isOriginLocal()) {
         if (t != null) {
            disposeCollectorOnException(id);
            return stage;
         }
         return waitCollector(id, rv);
      } else {
         //we are the primary owner! send back ack.
         if (t != null) {
            sendExceptionAck(id, cmd.getTopologyId(), t);
         } else {
            sendPrimaryAck(cmd, rv);
         }
      }
      return stage;
   }

   private void disposeCollectorOnException(CommandInvocationId id) {
      //a local exception occur. No need to wait for acks.
      commandAckCollector.dispose(id);
   }

   private BasicInvocationStage waitCollector(CommandInvocationId id, Object currentReturnValue) throws Throwable {
      //waiting for acks based on default rpc timeout.
      return returnWith(commandAckCollector.awaitCollector(id, timeoutNanoseconds, TimeUnit.NANOSECONDS, currentReturnValue));
   }

   private void onBackupCommand(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable) {
      BackupWriteCommand cmd = (BackupWriteCommand) rCommand;
      if (throwable != null) {
         sendExceptionAck(cmd.getCommandInvocationId(), cmd.getTopologyId(), throwable);
      } else {
         sendBackupAck(cmd);
      }
   }

   private void sendPrimaryAck(DataWriteCommand command, Object returnValue) {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      PrimaryAckCommand ackCommand = commandsFactory.buildPrimaryAckCommand(id, command.getTopologyId());
      command.initPrimaryAck(ackCommand, returnValue);
      rpcManager.sendTo(id.getAddress(), ackCommand, command.isSuccessful() ? DeliverOrder.NONE : DeliverOrder.PER_SENDER);
   }

   private void sendBackupAck(BackupWriteCommand command) {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      if (origin.equals(localAddress)) {
         commandAckCollector.backupAck(id, origin, command.getTopologyId());
      } else {
         rpcManager.sendTo(origin, commandsFactory.buildBackupAckCommand(id, command.getTopologyId()), DeliverOrder.NONE);
      }
   }

   private void sendPutMapBackupAck(PutMapCommand command, Collection<Integer> segments) {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      if (id.getAddress().equals(localAddress)) {
         commandAckCollector.putMapBackupAck(id, localAddress, segments, command.getTopologyId());
      } else {
         rpcManager.sendTo(id.getAddress(), commandsFactory.buildBackupPutMapAckCommand(id, segments, command.getTopologyId()), DeliverOrder.NONE);
      }
   }

   private void sendPrimaryPutMapAck(PutMapCommand command, Collection<Integer> segments, Map<Object, Object> returnValue) {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      PrimaryPutMapAckCommand ack = commandsFactory.buildPrimaryPutMapAckCommand(
            command.getCommandInvocationId(),
            command.getTopologyId());
      if (command.hasFlag(Flag.IGNORE_RETURN_VALUES)) {
         ack.initWithoutReturnValue(segments);
      } else {
         ack.initWithReturnValue(segments, returnValue);
      }
      rpcManager.sendTo(id.getAddress(), ack, command.isSuccessful() ? DeliverOrder.NONE : DeliverOrder.PER_SENDER);
   }

   private void sendExceptionAck(CommandInvocationId id, int topologyId, Throwable throwable) {
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending exception ack for command %s. Originator=%s.", id, origin);
      }
      if (origin.equals(localAddress)) {
         commandAckCollector.completeExceptionally(id, throwable, topologyId);
      } else {
         rpcManager.sendTo(origin, commandsFactory.buildExceptionAckCommand(id, throwable, topologyId), DeliverOrder.NONE);
      }
   }

   public enum KeyOwnership {
      PRIMARY, BACKUP, NONE;

      public static KeyOwnership ownership(List<Address> owners, Address localNode) {
         Iterator<Address> iterator = owners.iterator();
         if (localNode.equals(iterator.next())) {
            return PRIMARY;
         }
         while (iterator.hasNext()) {
            if (localNode.equals(iterator.next())) {
               return BACKUP;
            }
         }
         return NONE;
      }
   }
}