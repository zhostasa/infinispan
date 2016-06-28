package org.infinispan.commands;

import org.infinispan.metadata.Metadata;

/**
 * A command that contains metadata information.
 *
 * @author Galder Zamarreño
 * @deprecated Since 8.3, will be removed.
 */
@Deprecated
public interface MetadataAwareCommand {

   /**
    * Get metadata of this command.
    *
    * @return an instance of Metadata
    */
   Metadata getMetadata();

   /**
    * Sets metadata for this command.
    */
   void setMetadata(Metadata metadata);

}
