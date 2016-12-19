package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.marshall.core.Ids;

public final class UuidExternalizer extends AbstractExternalizer<UUID> {

   @Override
   public Set<Class<? extends UUID>> getTypeClasses() {
      return Collections.<Class<? extends UUID>>singleton(UUID.class);
   }

   @Override
   public Integer getId() {
      return Ids.UUID;
   }

   @Override
   public void writeObject(ObjectOutput output, UUID object) throws IOException {
      MarshallUtil.marshallUUID(object, output, false);
   }

   @Override
   public UUID readObject(ObjectInput input) throws IOException {
      return MarshallUtil.unmarshallUUID(input, false);
   }

}
