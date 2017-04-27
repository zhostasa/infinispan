package org.infinispan.server.test.rollingupgrades;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

import javax.management.ObjectName;

import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.test.category.RollingUpgrades;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for rolling upgrades functionality.
 *
 * @author Tomas Sykora (tsykora@redhat.com)
 */
@RunWith(Arquillian.class)
@Category({RollingUpgrades.class})
public class HotRodRollingUpgradesIT extends AbstractHotRodRollingUpgradesIT {

    @Test
    public void testHotRodRollingUpgradesDiffVersions() throws Exception {
        // Target node
        final int managementPortServer1 = 9990;
        MBeanServerConnectionProvider provider1;
        final String subsystemName1 = JDG_SUBSYSTEM_NAME;
        // Source node
        int managementPortServer2 = 10090; //jboss-as mgmt port
        String remotingProtocol2 = JDG_REMOTING_PROTOCOL;
        MBeanServerConnectionProvider provider2;
        String subsystemName2 = JDG_SUBSYSTEM_NAME;

        try {

            if (IS_JDG6) { 
                // server already started by Ant task
                managementPortServer2 = 10099;
                remotingProtocol2 = JDG6_REMOTING_PROTOCOL;
                subsystemName2 = JDG6_SUBSYSTEM_NAME;
            } else {
               // start it by Arquillian
               controller.start("hotrod-rolling-upgrade-2-old");
            }

            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.addServer()
                    .host("127.0.0.1")
                    .port(11322)
                    .version(HR_VERSION);

            RemoteCacheManager rcm = new RemoteCacheManager(builder.build());
            final RemoteCache<String, String> c2 = rcm.getCache("default");

            c2.put("key1", "value1");
            assertEquals("value1", c2.get("key1"));

            for (int i = 0; i < 50; i++) {
                c2.put("keyLoad" + i, "valueLoad" + i);
            }

            controller.start("hotrod-rolling-upgrade-1");

            RemoteInfinispanMBeans s1 = createRemotes("hotrod-rolling-upgrade-1", "local", DEFAULT_CACHE_NAME);
            // hotrod.protocol.version, if explicitly defined, is set in createRemotes() method
            final RemoteCache<Object, Object> c1 = createCache(s1);

            assertEquals("Can't access entries stored in source node (target's RemoteCacheStore).", "value1", c1.get("key1"));

            provider2 = new MBeanServerConnectionProvider("127.0.0.1", managementPortServer2, remotingProtocol2);

            final ObjectName rollMan1 = new ObjectName("jboss." + subsystemName1 + ":type=Cache," + "name=\"default(local)\","
                    + "manager=\"local\"," + "component=RollingUpgradeManager");
            
            final ObjectName rollMan2 = new ObjectName("jboss." + subsystemName2 + ":type=Cache," + "name=\"default(local)\","
                  + "manager=\"local\"," + "component=RollingUpgradeManager");

            invokeOperation(provider2, rollMan2.toString(), "recordKnownGlobalKeyset", new Object[]{}, new String[]{});

            provider1 = new MBeanServerConnectionProvider(s1.server.getHotrodEndpoint().getInetAddress().getHostName(),
                    managementPortServer1);

            invokeOperation(provider1, rollMan1.toString(), "synchronizeData", new Object[]{"hotrod"},
                    new String[]{"java.lang.String"});

            invokeOperation(provider1, rollMan1.toString(), "disconnectSource", new Object[]{"hotrod"},
                    new String[]{"java.lang.String"});

            // is source (RemoteCacheStore) really disconnected?
            c2.put("disconnected", "source");
            assertEquals("Can't obtain value from cache2 (source node).", "source", c2.get("disconnected"));
            assertNull("Source node entries should NOT be accessible from target node (after RCS disconnection)",
                    c1.get("disconnected"));

            // all entries migrated?
            assertEquals("Entry was not successfully migrated.", "value1", c1.get("key1"));
            for (int i = 0; i < 50; i++) {
                assertEquals("Entry was not successfully migrated.", "valueLoad" + i, c1.get("keyLoad" + i));
            }
        } finally {
            if (controller.isStarted("hotrod-rolling-upgrade-1")) {
                controller.stop("hotrod-rolling-upgrade-1");
            }
            if (controller.isStarted("hotrod-rolling-upgrade-2-old")) {
                controller.stop("hotrod-rolling-upgrade-2-old");
            }
        }
    }
}
