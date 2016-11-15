package org.infinispan.interceptors.distribution;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.TriangleInterceptor;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Non-transactional interceptor used by distributed caches that supports concurrent writes.
 * <p>
 * It is implemented base on the Triangle algorithm and the read operation didn't change. The write operation is sent to
 * the primary owner that executes the operation under the lock. If the operation succeeds, it is forwarded to the
 * backups owner with FIFO order and then the lock is released. The backups will send an acknowledge when the operation
 * is applied. The originator will wait for all the acknowledges.
 * <p>
 * The acknowledges management is done in the {@link TriangleInterceptor} by the {@link CommandAckCollector}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleDistributionInterceptor extends NonTxDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TriangleDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private CommandAckCollector commandAckCollector;

   @Inject
   public void inject(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
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
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   private BasicInvocationStage handleRemotePutMapCommand(InvocationContext ctx, PutMapCommand command) {
      final ConsistentHash ch = dm.getWriteConsistentHash();
      if (command.isForwarded() || ch.getNumOwners() == 1) {
         //backup & remote || no backups
         return invokeNext(ctx, command);
      }
      //primary, we need to send the command to the backups ordered!
      sendToBackups(command, command.getMap(), ch);
      return invokeNext(ctx, command);
   }

   private void sendToBackups(PutMapCommand command, Map<Object, Object> entries, ConsistentHash ch) {
      BackupOwnerFilter filter = new BackupOwnerFilter(ch);
      entries.entrySet().forEach(filter::add);
      for (Map.Entry<Address, Map<Object, Object>> entry : filter.perBackupKeyValue.entrySet()) {
         PutMapCommand copy = new PutMapCommand(command, false);
         copy.setMap(entry.getValue());
         copy.setForwarded(true);
         copy.addFlag(Flag.SKIP_LOCKING);
         rpcManager.sendTo(entry.getKey(), copy, DeliverOrder.PER_SENDER);
      }
   }

   private BasicInvocationStage handleLocalPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      //local command. we need to split by primary owner to send the command to them
      final ConsistentHash consistentHash = dm.getWriteConsistentHash();
      final PrimaryOwnerFilter filter = new PrimaryOwnerFilter(consistentHash);
      final boolean sync = isSynchronous(command);
      final Address localAddress = rpcManager.getAddress();

      command.getMap().entrySet().forEach(filter::add);

      if (sync) {
         commandAckCollector.createPutMapCollector(command.getCommandInvocationId(), filter.toPrimarySegments(), filter.backups, command.getTopologyId());
         final PrimaryKeyValue localEntries = filter.primaries.remove(localAddress);
         forwardToPrimaryOwners(command, filter);
         if (localEntries != null) {
            sendToBackups(command, localEntries.entries, consistentHash);
         }
         return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> {
            PutMapCommand cmd = (PutMapCommand) rCommand;
            if (t != null) {
               commandAckCollector.completeExceptionally(cmd.getCommandInvocationId(), t, cmd.getTopologyId());
            } else if (localEntries != null) {
               //noinspection unchecked
               commandAckCollector.putMapPrimaryAck(cmd.getCommandInvocationId(), localAddress, localEntries.segments, (Map<Object, Object>) rv, cmd.getTopologyId());
            }
            return stage;
         });
      }

      final PrimaryKeyValue localEntries = filter.primaries.remove(localAddress);
      forwardToPrimaryOwners(command, filter);
      if (localEntries != null) {
         sendToBackups(command, localEntries.entries, consistentHash);
      }
      return invokeNext(ctx, command);
   }

   private void forwardToPrimaryOwners(PutMapCommand command, PrimaryOwnerFilter splitter) {
      for (Map.Entry<Address, PrimaryKeyValue> entry : splitter.primaries.entrySet()) {
         PutMapCommand copy = new PutMapCommand(command, false);
         copy.setMap(entry.getValue().entries);
         rpcManager.sendTo(entry.getKey(), copy, DeliverOrder.NONE);
      }
   }

   private BasicInvocationStage handleDataWriteCommand(InvocationContext context, DataWriteCommand command) {
      assert !context.isInTxScope();
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         //don't go through the triangle
         return invokeNext(context, command);
      }
      final CacheTopology topology  =stateTransferManager.getCacheTopology();
      checkTopology(command.getTopologyId(), topology.getTopologyId());

      final List<Address> owners = topology.getWriteConsistentHash().locateOwners(command.getKey());
      //TODO remove this when Radim's PR is integrated!
      TriangleInterceptor.KeyOwnership ownership = TriangleInterceptor.KeyOwnership.ownership(owners, rpcManager.getAddress());

      switch (ownership) {
         case PRIMARY:
            return primaryOwnerWrite(context, command, owners);
         case BACKUP:
         case NONE:
            //always local! in remote, BackupWriteCommand is used!
            assert context.isOriginLocal();
            return localWriteInvocation(context, command, owners);
      }
      throw new IllegalStateException();
   }

   private BasicInvocationStage primaryOwnerWrite(InvocationContext context, DataWriteCommand command, final List<Address> owners) {
      //we are the primary owner. we need to execute the command, check if successful, send to backups and reply to originator is needed.
      if (command.hasFlag(Flag.COMMAND_RETRY)) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
      }

      return invokeNext(context, command).thenAccept((rCtx, rCommand, rv) -> {
         final DataWriteCommand dwCommand = (DataWriteCommand) rCommand;
         final CommandInvocationId id = dwCommand.getCommandInvocationId();
         if (!dwCommand.isSuccessful()) {
            if (trace) {
               log.tracef("Command %s not successful in primary owner.", id);
            }
            return;
         }
         final int size = owners.size();
         if (size > 1) {
            Collection<Address> backupOwners = owners.subList(1, size);
            if (rCtx.isOriginLocal() && (isSynchronous(dwCommand) || dwCommand.isReturnValueExpected())) {
               commandAckCollector.create(id, rv, owners, dwCommand.getTopologyId());
            }
            if (trace) {
               log.tracef("Command %s send to backup owner %s.", dwCommand.getCommandInvocationId(), backupOwners);
            }

            // we must send the message only after the collector is registered in the map
            rpcManager.sendTo(dwCommand.createBackupWriteCommand(), DeliverOrder.PER_SENDER, backupOwners);
         }
      });
   }

   private BasicInvocationStage localWriteInvocation(InvocationContext context, DataWriteCommand command,
                                                     List<Address> owners) {
      assert context.isOriginLocal();
      final CommandInvocationId invocationId = command.getCommandInvocationId();
      if ((isSynchronous(command) || command.isReturnValueExpected()) &&
            !command.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
         commandAckCollector.create(invocationId, owners, command.getTopologyId());
      }
      if (command.hasFlag(Flag.COMMAND_RETRY)) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
      }
      rpcManager.sendTo(owners.get(0), command, DeliverOrder.NONE);
      return returnWith(null);
   }

   private void checkTopology(int commandTopologyId, int currentTopologyId) {
      if (commandTopologyId != -1 && commandTopologyId != currentTopologyId) {
         throw OutdatedTopologyException.getCachedInstance();
      }
   }

   /**
    * Filters the keys by primary owner (address => keys & segments) and backup owners (address => segments).
    * <p>
    * The first map is used to forward the command to the primary owner with the subset of keys.
    * <p>
    * The second map is used to initialize the {@link CommandAckCollector} to wait for the backups acknowledges.
    */
   private static class PrimaryOwnerFilter {
      private final Map<Address, Collection<Integer>> backups = new HashMap<>();
      private final Map<Address, PrimaryKeyValue> primaries = new HashMap<>();
      private final ConsistentHash consistentHash;

      private PrimaryOwnerFilter(ConsistentHash consistentHash) {
         this.consistentHash = consistentHash;
      }

      public void add(Map.Entry<Object, Object> entry) {
         final int segment = consistentHash.getSegment(entry.getKey());
         final Iterator<Address> iterator = consistentHash.locateOwnersForSegment(segment).iterator();
         final Address primaryOwner = iterator.next();
         primaries.computeIfAbsent(primaryOwner, address -> new PrimaryKeyValue()).put(entry, segment);
         while (iterator.hasNext()) {
            Address backup = iterator.next();
            backups.computeIfAbsent(backup, address -> new HashSet<>()).add(segment);
         }
      }

      private Map<Address, Collection<Integer>> toPrimarySegments() {
         Map<Address, Collection<Integer>> map = new HashMap<>();
         for (Map.Entry<Address, PrimaryKeyValue> entry : primaries.entrySet()) {
            map.put(entry.getKey(), entry.getValue().segments);
         }
         return map;
      }
   }

   /**
    * A filter use in the primary owner when handles a remote {@link PutMapCommand}.
    * <p>
    * It maps the backup owner address to the subset of keys.
    */
   private static class BackupOwnerFilter {
      private final Map<Address, Map<Object, Object>> perBackupKeyValue = new HashMap<>();
      private final ConsistentHash consistentHash;

      private BackupOwnerFilter(ConsistentHash consistentHash) {
         this.consistentHash = consistentHash;
      }

      public void add(Map.Entry<Object, Object> entry) {
         Iterator<Address> iterator = consistentHash.locateOwners(entry.getKey()).iterator();
         iterator.next();
         while (iterator.hasNext()) {
            perBackupKeyValue.computeIfAbsent(iterator.next(), address -> new HashMap<>())
                  .put(entry.getKey(), entry.getValue());
         }
      }
   }

   /**
    * A primary owner subset of keys and affected segments.
    */
   private static class PrimaryKeyValue {
      private final Map<Object, Object> entries;
      private final Collection<Integer> segments;

      private PrimaryKeyValue() {
         entries = new HashMap<>();
         segments = new HashSet<>();
      }

      private void put(Map.Entry<Object, Object> entry, int segment) {
         entries.put(entry.getKey(), entry.getValue());
         segments.add(segment);
      }
   }

}
