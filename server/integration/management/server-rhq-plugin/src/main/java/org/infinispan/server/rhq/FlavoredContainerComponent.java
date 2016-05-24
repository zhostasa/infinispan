package org.infinispan.server.rhq;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.resource.CreateResourceStatus;
import org.rhq.core.pluginapi.inventory.CreateChildResourceFacet;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.modules.plugins.wildfly10.BaseComponent;
import org.rhq.modules.plugins.wildfly10.json.Address;
import org.rhq.modules.plugins.wildfly10.json.Operation;
import org.rhq.modules.plugins.wildfly10.json.Result;

/**
 * @author Heiko W. Rupp
 * @author Tristan Tarrant
 * @author Will Burns
 */
public abstract class FlavoredContainerComponent<T extends FlavoredContainerComponent<?>> extends BaseComponent<T> implements CreateChildResourceFacet {
   static final String FLAVOR = "_flavor";

   /**
    * Create embedded cache elements Trick is to take the _flavor property and turn it into a part of the path
    *
    * @param report contains all of the necessary information to create the specified resource and should be populated
    *               with the
    * @return
    */
   @Override
   public CreateResourceReport createResource(CreateResourceReport report) {
      Configuration config = report.getResourceConfiguration();
      String flavor = config.getSimpleValue(FLAVOR, null);
      if (flavor == null) {
         report.setStatus(CreateResourceStatus.INVALID_CONFIGURATION);
         report.setErrorMessage("No flavor given");

         return report;
      }
      String newName = report.getUserSpecifiedResourceName();
      Address address = new Address(this.getAddress());
      address.add(flavor, newName);
      Operation add = new Operation("add", address);
      for (Property prop : config.getProperties()) {
         if (prop.getName().equals(FLAVOR)) {
            continue;
         }
         PropertySimple ps = (PropertySimple) prop;
         add.addAdditionalProperty(prop.getName(), ps.getStringValue()); // TODO format conversion?

      }
      Result result = getASConnection().execute(add);
      if (result.isSuccess()) {
         report.setResourceKey(address.getPath());
         report.setResourceName(address.getPath());
         report.setStatus(CreateResourceStatus.SUCCESS);
      } else {
         report.setErrorMessage(result.getFailureDescription());
         report.setStatus(CreateResourceStatus.FAILURE);
      }

      return report;
   }
}
