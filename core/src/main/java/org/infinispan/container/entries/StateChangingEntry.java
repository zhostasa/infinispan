package org.infinispan.container.entries;

/**
 * An entry that may have state, such as created, changed, valid, etc.
 *
 * @author Manik Surtani
 * @deprecated Since 8.3, will be removed.
 */
@Deprecated
public interface StateChangingEntry {

   @Deprecated
   default byte getStateFlags() {
      return 0;
   }

   @Deprecated
   default void copyStateFlagsFrom(StateChangingEntry other) {}

}
