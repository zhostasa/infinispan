package org.infinispan.commands.functional;

import static org.infinispan.functional.impl.EntryViews.snapshot;
import static org.infinispan.util.TriangleFunctionsUtil.filterEntries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.BackupMultiKeyWriteRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * @deprecated Since 8.3, will be removed.
 */
// TODO: the command does not carry previous values to backup, so it can cause
// the values on primary and backup owners to diverge in case of topology change
@Deprecated
public final class ReadWriteManyEntriesCommand<K, V, R> extends AbstractWriteManyCommand {

   public static final byte COMMAND_ID = 53;

   private Map<? extends K, ? extends V> entries;
   private BiFunction<V, ReadWriteEntryView<K, V>, R> f;

   boolean isForwarded = false;

   public ReadWriteManyEntriesCommand(Map<? extends K, ? extends V> entries, BiFunction<V, ReadWriteEntryView<K, V>, R> f, Params params, CommandInvocationId commandInvocationId) {
      super(commandInvocationId, params);
      this.entries = entries;
      this.f = f;
   }

   public ReadWriteManyEntriesCommand() {
   }

   public ReadWriteManyEntriesCommand(ReadWriteManyEntriesCommand command) {
      super(command);
      this.entries = command.entries;
      this.f = command.f;
   }

   public Map<? extends K, ? extends V> getEntries() {
      return entries;
   }

   public void setEntries(Map<? extends K, ? extends V> entries) {
      this.entries = entries;
   }

   public final ReadWriteManyEntriesCommand<K, V, R> withEntries(Map<? extends K, ? extends V> entries) {
      setEntries(entries);
      return this;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallMap(entries, output);
      output.writeObject(f);
      output.writeBoolean(isForwarded);
      Params.writeObject(output, params);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      // We use LinkedHashMap in order to guarantee the same order of iteration
      entries = MarshallUtil.unmarshallMap(input, LinkedHashMap::new);
      f = (BiFunction<V, ReadWriteEntryView<K, V>, R>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
   }

   public boolean isForwarded() {
      return isForwarded;
   }

   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteManyEntriesCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // Can't return a lazy stream here because the current code in
      // EntryWrappingInterceptor expects any changes to be done eagerly,
      // otherwise they're not applied. So, apply the function eagerly and
      // return a lazy stream of the void returns.
      List<R> returns = new ArrayList<>(entries.size());
      entries.forEach((k, v) -> {
         CacheEntry<K, V> entry = ctx.lookupEntry(k);

         if (entry == null) {
            throw new IllegalStateException();
         }
         R r = f.apply(v, EntryViews.readWrite(entry));
         returns.add(snapshot(r));
      });
      return returns;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return entries.keySet();
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // TODO: Customise this generated block
   }

   @Override
   public boolean canBlock() {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;  // TODO: Customise this generated block
   }

   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ReadWriteManyEntriesCommand{");
      sb.append("entries=").append(entries);
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", isForwarded=").append(isForwarded);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public Collection<?> getKeysToLock() {
      return entries.keySet();
   }

   @Override
   public void initBackupMultiKeyWriteRpcCommand(BackupMultiKeyWriteRpcCommand command, Collection<Object> keys) {
      //noinspection unchecked
      command.setReadWriteEntries(commandInvocationId, filterEntries(entries, keys), f, params, getFlagsBitSet(), getTopologyId());
   }
}
