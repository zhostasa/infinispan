package org.infinispan.server.rhq;

import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.pluginapi.inventory.CreateChildResourceFacet;
import org.rhq.modules.plugins.wildfly10.json.ReadAttribute;
import org.rhq.modules.plugins.wildfly10.json.Result;

/**
 * Component class for Infinispan caches
 *
 * @author Heiko W. Rupp
 * @author William Burns
 */
public class CacheComponent extends FlavoredBaseComponent<CacheComponent> implements CreateChildResourceFacet {
   /**
    * Get the availability check based on if the cache is actually running or not
    *
    * @see org.rhq.core.pluginapi.inventory.ResourceComponent#getAvailability()
    */
   @Override
   public AvailabilityType getAvailability() {
      ReadAttribute op = new ReadAttribute(getAddress(), "cache-status");
      Result res = getASConnection().execute(op);
      if (res != null && res.isSuccess()) {
         if ("RUNNING".equals(res.getResult())) {
            return AvailabilityType.UP;
         }
      }
      return AvailabilityType.DOWN;
   }
}
