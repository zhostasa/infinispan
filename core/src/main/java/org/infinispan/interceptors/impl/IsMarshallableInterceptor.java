package org.infinispan.interceptors.impl;

import java.util.Map;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.DDAsyncInterceptor;

/**
 * Interceptor to verify whether parameters passed into cache are marshallables
 * or not.
 *
 * <p>This is handy when marshalling happens in a separate
 * thread and marshalling failures might be swallowed.
 * Currently, this only happens when we have an asynchronous store.</p>
 *
 * @author Galder Zamarreño
 * @since 9.0
 */
public class IsMarshallableInterceptor extends DDAsyncInterceptor {

   private StreamingMarshaller marshaller;
   private boolean usingAsyncStore;

   @Inject
   protected void injectMarshaller(StreamingMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Start
   protected void start() {
      usingAsyncStore = cacheConfiguration.persistence().usingAsyncStore();
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (isUsingAsyncStore(ctx, command)) {
         checkMarshallable(command.getValue());
      }
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (isUsingAsyncStore(ctx, command)) {
         checkMarshallable(command.getMap());
      }
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (isUsingAsyncStore(ctx, command)) {
         checkMarshallable(command.getKey());
      }
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (isUsingAsyncStore(ctx, command)) {
         checkMarshallable(command.getNewValue());
      }
      return invokeNext(ctx, command);
   }

   private boolean isUsingAsyncStore(InvocationContext ctx, FlagAffectedCommand command) {
      return usingAsyncStore && ctx.isOriginLocal() && !command.hasAnyFlag(FlagBitSets.SKIP_CACHE_STORE);
   }

   private void checkMarshallable(Object o) throws NotSerializableException {
      boolean marshallable = false;
      try {
         marshallable = marshaller.isMarshallable(o);
      } catch (Exception e) {
         throwNotSerializable(o, e);
      }

      if (!marshallable)
         throwNotSerializable(o, null);
   }

   private void throwNotSerializable(Object o, Throwable t) {
      String msg = String.format(
            "Object of type %s expected to be marshallable", o.getClass());
      if (t == null)
         throw new NotSerializableException(msg);
      else
         throw new NotSerializableException(msg, t);
   }

   private void checkMarshallable(Map<Object, Object> objs) throws NotSerializableException {
      for (Map.Entry<Object, Object> entry : objs.entrySet()) {
         checkMarshallable(entry.getKey());
         checkMarshallable(entry.getValue());
      }
   }

}
