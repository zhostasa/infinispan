package org.infinispan.query;

import java.util.Set;

import org.hibernate.search.spi.CustomTypeMetadata;

public class IndexedTypeMetadata implements CustomTypeMetadata {

   private final Class<?> indexedType;
   private final Set<String> sortableFields;

   public IndexedTypeMetadata(Class<?> indexedType, Set<String> sortableFields) {
      this.indexedType = indexedType;
      this.sortableFields = sortableFields;
   }

   @Override
   public Class<?> getEntityType() {
      return indexedType;
   }

   @Override
   public Set<String> getSortableFields() {
      return sortableFields;
   }

}
