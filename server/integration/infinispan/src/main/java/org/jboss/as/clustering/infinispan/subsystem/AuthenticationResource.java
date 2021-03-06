/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelType;

/**
 * AuthenticationResource.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class AuthenticationResource extends SimpleResourceDefinition {

   private static final PathElement AUTHENTICATION_PATH = PathElement.pathElement(ModelKeys.AUTHENTICATION, ModelKeys.AUTHENTICATION_NAME);

   static final SimpleAttributeDefinition MECHANISM =
         new SimpleAttributeDefinitionBuilder(ModelKeys.MECHANISM, ModelType.STRING, false)
               .setAllowExpression(false)
               .setXmlName(ModelKeys.MECHANISM)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .build();
   static final SimpleAttributeDefinition USERNAME =
         new SimpleAttributeDefinitionBuilder(ModelKeys.USERNAME, ModelType.STRING, true)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.USERNAME)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .build();
   static final SimpleAttributeDefinition PASSWORD =
         new SimpleAttributeDefinitionBuilder(ModelKeys.PASSWORD, ModelType.STRING, false)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.PASSWORD)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .build();
   static final SimpleAttributeDefinition REALM =
         new SimpleAttributeDefinitionBuilder(ModelKeys.REALM, ModelType.STRING, false)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.REALM)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .build();


   static final SimpleAttributeDefinition[] AUTHENTICATION_ATTRIBUTES = { MECHANISM, USERNAME, PASSWORD, REALM };

   public AuthenticationResource() {
      super(AUTHENTICATION_PATH, new InfinispanResourceDescriptionResolver(ModelKeys.REMOTE_STORE, ModelKeys.AUTHENTICATION), AuthenticationAdd.INSTANCE, ReloadRequiredRemoveStepHandler.INSTANCE);
   }

   @Override
   public void registerChildren(ManagementResourceRegistration resourceRegistration) {
      super.registerChildren(resourceRegistration);
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
       super.registerAttributes(resourceRegistration);

       final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(AUTHENTICATION_ATTRIBUTES);
       for (AttributeDefinition attr : AUTHENTICATION_ATTRIBUTES) {
           resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
       }
   }
}
