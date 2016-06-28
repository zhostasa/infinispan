package org.infinispan.container.versioning;

import java.util.HashMap;

/**
 * @deprecated Since 8.3, will be removed.
 */
@Deprecated
public class EntryVersionsMap extends HashMap<Object, IncrementableEntryVersion> {

   public EntryVersionsMap(int size) {
      super(size);
   }

   public EntryVersionsMap() {
      super();
   }

   public EntryVersionsMap merge(EntryVersionsMap updatedVersions) {
      if (updatedVersions != null && !updatedVersions.isEmpty()) {
         updatedVersions.putAll(this);
         return updatedVersions;
      } else {
         return this;
      }
   }
}
