package org.infinispan.commands;


/**
 * Commands of this type manipulate data in the cache.
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @deprecated Since 8.3, will be removed.
 */
@Deprecated
public interface DataCommand extends VisitableCommand, TopologyAffectedCommand, FlagAffectedCommand {
   Object getKey();
}