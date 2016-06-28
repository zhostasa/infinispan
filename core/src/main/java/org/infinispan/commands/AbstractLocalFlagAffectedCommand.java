package org.infinispan.commands;

import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;

/**
 * Base class for those local commands that can carry flags.
 *
 * @author William Burns
 * @deprecated Since 8.3, will be removed.
 */
@Deprecated
public abstract class AbstractLocalFlagAffectedCommand implements LocalFlagAffectedCommand {

   private long flags = 0;

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      this.flags = bitSet;
   }

   protected final boolean hasSameFlags(LocalFlagAffectedCommand other) {
      return this.flags == other.getFlagsBitSet();
   }

   protected final String printFlags() {
      return EnumUtil.prettyPrintBitSet(flags, Flag.class);
   }
}
