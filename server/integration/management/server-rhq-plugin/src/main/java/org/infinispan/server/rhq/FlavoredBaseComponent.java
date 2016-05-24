package org.infinispan.server.rhq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.ConfigurationUpdateStatus;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.ConfigurationDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionMap;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionSimple;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.domain.resource.CreateResourceStatus;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.inventory.CreateChildResourceFacet;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.modules.plugins.wildfly10.ASConnection;
import org.rhq.modules.plugins.wildfly10.BaseComponent;
import org.rhq.modules.plugins.wildfly10.ConfigurationLoadDelegate;
import org.rhq.modules.plugins.wildfly10.ConfigurationWriteDelegate;
import org.rhq.modules.plugins.wildfly10.CreateResourceDelegate;
import org.rhq.modules.plugins.wildfly10.json.Address;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Heiko W. Rupp
 * @author William Burns
 */
public abstract class FlavoredBaseComponent<T extends FlavoredBaseComponent<?>> extends BaseComponent<T> implements CreateChildResourceFacet {
   final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   static final String FLAVOR = "_flavor";

   protected ResourceContext<T> context;

   @Override
   public void start(ResourceContext<T> context) throws Exception {
      this.context = context;
      super.start(context);
   }

   @Override
   public CreateResourceReport createResource(CreateResourceReport report) {
      if (report.getPluginConfiguration().getSimpleValue("path").contains("jdbc")) {
         ASConnection connection = getASConnection();
         ConfigurationDefinition configDef = report.getResourceType().getResourceConfigurationDefinition();

         CreateResourceDelegate delegate = new CreateResourceDelegate(configDef, connection, getAddress()) {
            @Override
            protected Map<String, Object> prepareSimplePropertyMap(PropertyMap property, PropertyDefinitionMap propertyDefinition) {
               // Note this is all pretty much copied from ConfigurationWriteDelegate.prepareSimplePropertyMap
               Map<String, PropertyDefinition> memberDefinitions = propertyDefinition.getMap();

               Map<String, Object> results = new HashMap<String, Object>();
               for (String name : memberDefinitions.keySet()) {
                  PropertyDefinition memberDefinition = memberDefinitions.get(name);

                  if (memberDefinition instanceof PropertyDefinitionSimple) {
                     PropertyDefinitionSimple pds = (PropertyDefinitionSimple) memberDefinition;
                     PropertySimple ps = (PropertySimple) property.get(name);
                     if ((ps == null || ps.getStringValue() == null) && !pds.isRequired())
                        continue;
                     if (ps != null)
                        results.put(name, ps.getStringValue());
                  }
                  // This is added since it isn't supported already.
                  // Should be merged with https://github.com/rhq-project/rhq/pull/128
                  else if (memberDefinition instanceof PropertyDefinitionMap) {
                     PropertyDefinitionMap pdm = (PropertyDefinitionMap) memberDefinition;
                     PropertyMap pm = (PropertyMap) property.get(name);
                     if ((pm == null || pm.getMap().isEmpty()) && !pdm.isRequired())
                        continue;
                     if (pm != null) {
                        Map<String, Object> innerMap = prepareSimplePropertyMap(pm, pdm);
                        results.put(name, innerMap);
                     }
                  } else {
                     log.error(" *** not yet supported *** : " + memberDefinition.getName());
                  }
               }
               return results;
            }
         };
         report = delegate.createResource(report);
      } else {

         report = super.createResource(report);
      }

      // Since our properties can be added at parent resource creation time, we have to make sure they are added.
      if (report.getStatus() == CreateResourceStatus.SUCCESS) {
         // Now we have to send this as an update, so the properties are created properly
         ConfigurationUpdateReport updateReport = new ConfigurationUpdateReport(report.getResourceConfiguration());
         ConfigurationDefinition configDef = report.getResourceType().getResourceConfigurationDefinition();
         Address address = new Address(getAddress());
         address.add(report.getPluginConfiguration().getSimpleValue("path"), report.getUserSpecifiedResourceName());
         ConfigurationWriteDelegate delegate = new ConfigurationWriteDelegate(configDef, getASConnection(), address);
         delegate.updateResourceConfiguration(updateReport);

         if (updateReport.getStatus() != ConfigurationUpdateStatus.SUCCESS) {
            report.setErrorMessage(updateReport.getErrorMessage());
            report.setStatus(CreateResourceStatus.FAILURE);
         }
      }
      return report;
   }

   public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> metrics) throws Exception {
      Set<MeasurementScheduleRequest> requests = metrics;
      Set<MeasurementScheduleRequest> todo = new HashSet<MeasurementScheduleRequest>();
      for (MeasurementScheduleRequest req : requests) {
         if (req.getName().equals("__flavor")) {
            String flavor = getFlavorFromPath();
            MeasurementDataTrait trait = new MeasurementDataTrait(req, flavor);
            report.addData(trait);
         } else {
            todo.add(req);
         }
      }
      super.getValues(report, todo);
   }

   private String getFlavorFromPath() {
      String flavor = getPath().substring(getPath().lastIndexOf(",") + 1);
      flavor = flavor.substring(0, flavor.indexOf("="));
      return flavor;
   }

   @Override
   public Configuration loadResourceConfiguration() throws Exception {
      ConfigurationDefinition configDef = context.getResourceType().getResourceConfigurationDefinition();
      ConfigurationLoadDelegate delegate = new ConfigurationLoadDelegate(configDef, getASConnection(), getAddress(), true);
      Configuration config = delegate.loadResourceConfiguration();
      String f = getFlavorFromPath();
      PropertySimple flavor = new PropertySimple(FLAVOR, f);
      config.put(flavor);
      return config;
   }

   @Override
   public void updateResourceConfiguration(ConfigurationUpdateReport report) {
      report.getConfiguration().remove(FLAVOR);
      super.updateResourceConfiguration(report);
   }
}
