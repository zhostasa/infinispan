package org.infinispan.server.commons.modules;

import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;

/**
 * A utility class for common tasks related to jboss-modules
 *
 * @author Ryan Emerson
 * @since 8.5
 */
public class JbossModulesUtil {

   private static final String ELYTRON_MODULE = "org.wildfly.security.elytron-private";

   public static boolean isModuleAvailable(String moduleName) {
      return isModuleAvailable(Module.getBootModuleLoader(), moduleName);
   }

   public static boolean isModuleAvailable(ModuleLoader loader, String moduleName) {
      ModuleIdentifier moduleIdentifier = ModuleIdentifier.create(moduleName);
      try {
         loader.loadModule(moduleIdentifier);
      } catch (ModuleLoadException e) {
         return false;
      }
      return true;
   }

   public static boolean isElytronAvailable() {
      return isModuleAvailable(ELYTRON_MODULE);
   }

}
