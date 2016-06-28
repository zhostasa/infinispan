package org.infinispan.query.remote.impl.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.query.remote.client.ContinuousQueryResult;
import org.infinispan.query.remote.impl.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * A 'remote' ContinuousQueryResult needs jboss-marshalling serializability between nodes when running in compat mode.
 * It will only be marshalled using protobuf before passing it to the remote client.
 *
 * @author anistor@redhat.com
 * @since 8.3
 */
public final class ContinuousQueryResultExternalizer extends AbstractExternalizer<ContinuousQueryResult> {

   @Override
   public void writeObject(ObjectOutput output, ContinuousQueryResult continuousQueryResult) throws IOException {
      output.writeBoolean(continuousQueryResult.isJoining());
      output.writeInt(continuousQueryResult.getKey().length);
      output.write(continuousQueryResult.getKey());
      if (continuousQueryResult.isJoining()) {
         Object[] projection = continuousQueryResult.getProjection();
         if (projection == null) {
            output.writeInt(continuousQueryResult.getValue().length);
            output.writeObject(continuousQueryResult.getValue());
         } else {
            // skip serializing the instance if there is a projection
            output.writeInt(-1);
            int projLen = projection.length;
            output.writeInt(projLen);
            for (Object prj : projection) {
               output.writeObject(prj);
            }
         }
      }
   }

   @Override
   public ContinuousQueryResult readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      boolean isJoining = input.readBoolean();
      int keyLen = input.readInt();
      byte[] key = new byte[keyLen];
      input.readFully(key);
      byte[] value = null;
      Object[] projection = null;
      if (isJoining) {
         int valueLen = input.readInt();
         if (valueLen == -1) {
            int projLen = input.readInt();
            projection = new Object[projLen];
            for (int i = 0; i < projLen; i++) {
               projection[i] = input.readObject();
            }
         } else {
            value = new byte[valueLen];
            input.readFully(value);
         }
      }
      return new ContinuousQueryResult(isJoining, key, value, projection);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.JPA_CONTINUOUS_QUERY_RESULT;
   }

   @Override
   public Set<Class<? extends ContinuousQueryResult>> getTypeClasses() {
      return Collections.singleton(ContinuousQueryResult.class);
   }
}
