package org.infinispan.server.test.cs.jdbc;

import static org.infinispan.server.test.util.ITestUtils.createMBeans;
import static org.infinispan.server.test.util.ITestUtils.createMemcachedClient;
import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.infinispan.server.test.util.ITestUtils.getRealKeyFromStored;
import static org.infinispan.server.test.util.ITestUtils.getRealKeyStored;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.support.Bucket;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.infinispan.server.test.util.jdbc.DBServer;
import org.infinispan.test.TestingUtil;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * These are the tests for various JDBC stores (string, binary, mixed) with a single server.
 * We test each store with 2 configurations:
 * 1. passivation = true, preload = false
 * 2. passivation = false, preload = true
 * To speed things up, the tests use hotrod client so we can reuse a single server.
 * Test for the write-behind store uses memcached client.
 * Mixed store is not fully tested, because DefaultTwoWayKey2StringMapper (which does the decision string/binary) can handle
 * both string keys (memcached) and byte array keys (hotrod), which means all the keys go into the string store.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 * @author <a href="mailto:jmarkos@redhat.com">Jakub Markos</a>
 */
@RunWith(Arquillian.class)
@Category(CacheStore.class)
public class SingleNodeJdbcStoreIT {
    public static final Log log = LogFactory.getLog(SingleNodeJdbcStoreIT.class);

    public final String CONTAINER = "jdbc";

    @ArquillianResource
    protected ContainerController controller;

    @InfinispanResource(CONTAINER)
    protected RemoteInfinispanServer server;

    public final String ID_COLUMN_NAME = "id";
    public final String DATA_COLUMN_NAME = "datum";

    public static RemoteCacheManagerFactory rcmFactory;
    public static StreamingMarshaller globalMarshaller; // Necessary to unmarshall buckets

    // WP = without passivation
    static DBServer stringDB, stringWPDB, binaryDB, binaryWPDB, mixedDB, mixedWPDB, stringAsyncDB;
    static RemoteInfinispanMBeans stringMBeans, stringWPMBeans, binaryMBeans, binaryWPMBeans, mixedMBeans, mixedWPMBeans;
    static RemoteCache stringCache, stringWPCache, binaryCache, binaryWPCache, mixedCache, mixedWPCache;

    @BeforeClass
    public static void startup() {
        rcmFactory = new RemoteCacheManagerFactory();
        globalMarshaller = TestingUtil.marshaller(new DefaultCacheManager().getCache());
    }

    @AfterClass
    public static void cleanup() {
        /**
         * We need to drop the tables, because of DB2 SQL Error: SQLCODE=-204, SQLSTATE=42704
         */
        DBServer[] dbservers = {stringDB, stringWPDB, binaryDB, binaryWPDB, mixedDB, mixedWPDB, stringAsyncDB};
        for (DBServer dbServer : dbservers) {
            try {
                DBServer.TableManipulation bucketTable = dbServer.bucketTable;
                if (bucketTable != null) {
                   if (bucketTable.getConnectionUrl().contains("db2")) {
                      bucketTable.dropTable();
                      if (dbServer.stringTable != null) {
                         dbServer.stringTable.dropTable();
                      }
                   }
                }
            } catch (Exception e) {
                // catching the exception, because the drop is not part of the tests
                log.trace("Couldn't drop the tables: ", e);
            }
        }

        if (rcmFactory != null) {
            rcmFactory.stopManagers();
        }
        rcmFactory = null;
    }

    @Before
    public void setUp() throws Exception {
        if (stringDB == null) { // initialize only once (can't do it in BeforeClass because no server is running at that time)
            stringMBeans = createMBeans(server, CONTAINER, "stringWithPassivation", "local");
            stringCache = createCache(stringMBeans);
            stringDB = new DBServer(null, "STRING_WITH_PASSIVATION" + "_" + stringMBeans.cacheName, ID_COLUMN_NAME, DATA_COLUMN_NAME);

            stringWPMBeans = createMBeans(server, CONTAINER, "stringNoPassivation", "local");
            stringWPCache = createCache(stringWPMBeans);
            stringWPDB = new DBServer(null, "STRING_NO_PASSIVATION" + "_" + stringWPMBeans.cacheName, ID_COLUMN_NAME, DATA_COLUMN_NAME);

            binaryMBeans = createMBeans(server, CONTAINER, "binaryWithPassivation", "local");
            binaryCache = createCache(binaryMBeans);
            binaryDB = new DBServer("BINARY_WITH_PASSIVATION" + "_" + binaryMBeans.cacheName, null, ID_COLUMN_NAME, DATA_COLUMN_NAME);

            binaryWPMBeans = createMBeans(server, CONTAINER, "binaryNoPassivation", "local");
            binaryWPCache = createCache(binaryWPMBeans);
            binaryWPDB = new DBServer("BINARY_NO_PASSIVATION" + "_" + binaryWPMBeans.cacheName, null, ID_COLUMN_NAME, DATA_COLUMN_NAME);

            mixedMBeans = createMBeans(server, CONTAINER, "mixedWithPassivation", "local");
            mixedCache = createCache(mixedMBeans);
            mixedDB = new DBServer("MIXED_WITH_PASSIVATION_BKT" + "_" + mixedMBeans.cacheName, "MIXED_WITH_PASSIVATION_STR" + "_" + mixedMBeans.cacheName, ID_COLUMN_NAME, DATA_COLUMN_NAME);

            mixedWPMBeans = createMBeans(server, CONTAINER, "mixedNoPassivation", "local");
            mixedWPCache = createCache(mixedWPMBeans);
            mixedWPDB = new DBServer("MIXED_NO_PASSIVATION_BKT" + "_" + mixedWPMBeans.cacheName, "MIXED_NO_PASSIVATION_STR" + "_" + mixedWPMBeans.cacheName, ID_COLUMN_NAME, DATA_COLUMN_NAME);

            stringAsyncDB = new DBServer(null, "STRING_ASYNC" + "_" + "memcachedCache", ID_COLUMN_NAME, DATA_COLUMN_NAME);
        }
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER)})
    public void testNormalShutdown() throws Exception {
        // passivation = true, preload = false, purge = false, eviction.max-entries = 2 (LRU)
        testRestartStringStoreBefore();
        testRestartBinaryStoreBefore();
        testRestartMixedStoreBefore();

        // passivation = false, preload = true, purge = false
        testRestartStringStoreWPBefore();
        testRestartBinaryStoreWPBefore();
        testRestartMixedStoreWPBefore();

        controller.stop(CONTAINER); // normal shutdown - should store all entries from cache to store
        controller.start(CONTAINER);

        testRestartStringStoreAfter(false);
        testRestartBinaryStoreAfter(false);
        testRestartMixedStoreAfter(false);

        testRestartStringStoreWPAfter();
        testRestartBinaryStoreWPAfter();
        testRestartMixedStoreWPAfter();
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER)})
    public void testForcedShutdown() throws Exception {
        // passivation = true, preload = false, purge = false, eviction.max-entries = 2 (LRU)
        testRestartStringStoreBefore();
        testRestartBinaryStoreBefore();
        testRestartMixedStoreBefore();

        // passivation = false, preload = true, purge = false
        testRestartStringStoreWPBefore();
        testRestartBinaryStoreWPBefore();
        testRestartMixedStoreWPBefore();

        controller.kill(CONTAINER); // (kill -9)-ing the server
        controller.start(CONTAINER);

        testRestartStringStoreAfter(true);
        testRestartBinaryStoreAfter(true);
        testRestartMixedStoreAfter(true);

        testRestartStringStoreWPAfter();
        testRestartBinaryStoreWPAfter();
        testRestartMixedStoreWPAfter();
    }

    // simple test to see that write-behind store works
    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER)})
    public void testAsyncStringStore() throws Exception {
        MemcachedClient mc = createMemcachedClient(server);
        int numEntries = 1000;

        for (int i = 0; i != numEntries; i++) {
            mc.set("key" + i, "value" + i);
        }
        eventually(new ITestUtils.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return stringAsyncDB.stringTable.exists();
            }
        }, 10000);
        for (int i = 0; i != numEntries; i++) {
            assertNotNull("key" + i + " was not found in DB in " + DBServer.TIMEOUT + " ms", stringAsyncDB.stringTable.getValueByKeyAwait("key" + i));
        }
        for (int i = 0; i != numEntries; i++) {
            mc.delete("key" + i);
        }
        eventually(new ITestUtils.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return stringAsyncDB.stringTable.getAllRows().isEmpty();
            }
        }, 10000);
    }

    public void testRestartStringStoreBefore() throws Exception {
        assertCleanCacheAndStoreHotrod(stringCache, stringDB.stringTable);
        stringCache.put("k1", "v1");
        stringCache.put("k2", "v2");
        boolean tableExists = stringDB.stringTable.exists();
        if (tableExists) {
            assertNull(stringDB.stringTable.getValueByKey(getStoredStringKey(stringCache, "k1")));
            assertNull(stringDB.stringTable.getValueByKey(getStoredStringKey(stringCache, "k2")));
        }
        stringCache.put("k3", "v3");
        //now a key would be evicted and stored in store
        assertTrue(2 >= server.getCacheManager(stringMBeans.managerName).getCache(stringMBeans.cacheName).getNumberOfEntries());
        if (tableExists) {
            assertEquals(1, stringDB.stringTable.getAllKeys().size());
        }
    }

    public void testRestartStringStoreAfter(boolean killed) throws Exception {
        assertEquals(0, server.getCacheManager(stringMBeans.managerName).getCache(stringMBeans.cacheName).getNumberOfEntries());

        if (killed) {
            List<String> passivatedKeys = stringDB.stringTable.getAllKeys();
            assertEquals(1, passivatedKeys.size());
            String passivatedKey = getStoredKey(stringCache, passivatedKeys.get(0));
            assertEquals("v"+passivatedKey.substring(1), stringCache.get(passivatedKey)); // removed from store
            assertNull(stringDB.stringTable.getValueByKey(getStoredStringKey(stringCache, passivatedKey)));
            Set<String> allKeys = new HashSet<>(Arrays.asList("k1", "k2", "k3"));
            allKeys.remove(passivatedKey);
            for(String key : allKeys) {
                assertNull(stringCache.get(key));
            }
        } else {
            assertEquals(3, stringDB.stringTable.getAllRows().size());
            assertEquals("v1", stringCache.get("k1"));
            assertEquals("v2", stringCache.get("k2"));
            assertEquals("v3", stringCache.get("k3"));
            assertNull(stringDB.stringTable.getValueByKey(getStoredStringKey(stringCache, "k3")));
        }
    }

    public void testRestartBinaryStoreBefore() throws Exception {
        String key1 = "key1";
        String key2 = "anotherExtraUniqueKey";
        String key3 = "key3";
        assertCleanCacheAndStoreHotrod(binaryCache, binaryDB.bucketTable);

        binaryCache.put(key1, "v1");
        binaryCache.put(key2, "v2");
        assertTrue(!binaryDB.bucketTable.exists() || binaryDB.bucketTable.getAllRows().isEmpty());
        binaryCache.put(key3, "v3");
        assertTrue(2 >= server.getCacheManager(binaryMBeans.managerName).getCache(binaryMBeans.cacheName).getNumberOfEntries());
        assertTrue(!binaryDB.bucketTable.getAllRows().isEmpty());
        assertNotNull(getFirstKeyInBucket(binaryCache, binaryDB));
    }

    public void testRestartBinaryStoreAfter(boolean killed) throws Exception {
        String key1 = "key1";
        String key2 = "anotherExtraUniqueKey";
        String key3 = "key3";
        assertEquals(0, server.getCacheManager(binaryMBeans.managerName).getCache(binaryMBeans.cacheName).getNumberOfEntries());
        if (killed) {
            assertEquals(1, binaryDB.bucketTable.getAllRows().size());
            Set<String> allKeys = new HashSet<>(Arrays.asList(key1, key2, key3));
            String passivatedKey = (String) getFirstKeyInBucket(binaryCache, binaryDB);
            allKeys.remove(passivatedKey);
            for (String key : allKeys) {
                assertNull(binaryCache.get(key));
            }
        } else {
            assertTrue(binaryDB.bucketTable.getAllRows().size() >= 2); // can't test for 3, the row may be shared
            assertEquals("v1", binaryCache.get(key1));
            assertEquals("v2", binaryCache.get(key2));
            assertEquals("v3", binaryCache.get(key3));
        }
    }

    public void testRestartMixedStoreBefore() throws Exception {
        assertCleanCacheAndStoreHotrod(mixedCache, mixedDB.stringTable);
        assertCleanCacheAndStoreHotrod(mixedCache, mixedDB.bucketTable);

        mixedCache.put("k1", "v1");
        mixedCache.put("k2", "v2");
        assertTrue(!mixedDB.stringTable.exists() || mixedDB.stringTable.getAllRows().isEmpty());
        assertTrue(!mixedDB.bucketTable.exists() || mixedDB.bucketTable.getAllRows().isEmpty());
        //now k1 evicted and stored in store
        mixedCache.put("k3", "v3");
        assertEquals(2, server.getCacheManager(mixedMBeans.managerName).getCache(mixedMBeans.cacheName).getNumberOfEntries());
        assertEquals(1, mixedDB.stringTable.getAllRows().size());
        assertTrue(!mixedDB.bucketTable.exists() || mixedDB.bucketTable.getAllRows().isEmpty());
    }

    public void testRestartMixedStoreAfter(boolean killed) throws Exception {
        assertEquals(0, server.getCacheManager(mixedMBeans.managerName).getCache(mixedMBeans.cacheName).getNumberOfEntries());
        assertTrue(mixedDB.bucketTable.getAllRows().isEmpty());
        if (killed) {
            Set<String> allKeys = new HashSet<>(Arrays.asList("k1", "k2", "k3"));
            List<String> passivatedKeys = mixedDB.stringTable.getAllKeys();
            assertEquals(1, passivatedKeys.size());
            String passivatedKey = getStoredKey(mixedCache, passivatedKeys.get(0));
            assertEquals("v"+passivatedKey.substring(1), mixedCache.get(passivatedKey)); // removed from store
            allKeys.remove(passivatedKey);
            for (String key : allKeys) {
                assertNull(mixedCache.get(key));
            }
        } else {
            assertEquals(3, mixedDB.stringTable.getAllRows().size());
            assertEquals("v1", mixedCache.get("k1"));
            assertEquals("v2", mixedCache.get("k2"));
            assertEquals("v3", mixedCache.get("k3"));
            assertNull(mixedDB.stringTable.getValueByKey(getStoredStringKey(mixedCache, "k3")));
        }
    }

    public void testRestartStringStoreWPBefore() throws Exception {
        assertCleanCacheAndStoreHotrod(stringWPCache, stringWPDB.stringTable);
        stringWPCache.put("k1", "v1");
        stringWPCache.put("k2", "v2");
        assertNotNull(stringWPDB.stringTable.getValueByKey(getStoredStringKey(stringWPCache, "k1")));
        assertNotNull(stringWPDB.stringTable.getValueByKey(getStoredStringKey(stringWPCache, "k2")));
    }

    public void testRestartStringStoreWPAfter() throws Exception {
        eventually(new ITestUtils.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return 2 == server.getCacheManager(stringWPMBeans.managerName).getCache(stringWPMBeans.cacheName).getNumberOfEntries();
            }
        }, 10000);
        assertEquals("v1", stringWPCache.get("k1"));
        assertEquals("v2", stringWPCache.get("k2"));
        stringWPCache.remove("k1");
        assertNull(stringWPDB.stringTable.getValueByKey(getStoredStringKey(stringWPCache, "k1")));
        assertNotNull(stringWPDB.stringTable.getValueByKey(getStoredStringKey(stringWPCache, "k2")));
    }

    public void testRestartBinaryStoreWPBefore() throws Exception {
        String key1 = "key1";
        String key2 = "myBestPersonalKeyWhichHasNeverBeenBetter";
        assertCleanCacheAndStoreHotrod(binaryWPCache, binaryWPDB.bucketTable);

        binaryWPCache.put(key1, "v1");
        binaryWPCache.put(key2, "v2");
        byte[] k1Stored = getRealKeyStored(key1, binaryWPCache);
        byte[] k2Stored = getRealKeyStored(key2, binaryWPCache);
        binaryWPDB.bucketTable.waitForTableCreation();
        assertEquals(2, binaryWPDB.bucketTable.getAllRows().size());
        assertNotNull(binaryWPDB.bucketTable.getBucketByKey(new WrappedByteArray(k1Stored)));
        assertNotNull(binaryWPDB.bucketTable.getBucketByKey(new WrappedByteArray(k2Stored)));
    }

    public void testRestartBinaryStoreWPAfter() throws Exception {
        String key1 = "key1";
        String key2 = "myBestPersonalKeyWhichHasNeverBeenBetter";
        byte[] k1Stored = getRealKeyStored(key1, binaryWPCache);
        byte[] k2Stored = getRealKeyStored(key2, binaryWPCache);
        eventually(new ITestUtils.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return 2 == server.getCacheManager(binaryWPMBeans.managerName).getCache(binaryWPMBeans.cacheName).getNumberOfEntries();
            }
        }, 10000);
        assertEquals("v1", binaryWPCache.get(key1));
        assertEquals("v2", binaryWPCache.get(key2));
        assertEquals(2, binaryWPDB.bucketTable.getAllRows().size());
        assertNotNull(binaryWPDB.bucketTable.getBucketByKey(new WrappedByteArray(k1Stored)));
        assertNotNull(binaryWPDB.bucketTable.getBucketByKey(new WrappedByteArray(k2Stored)));
    }

    public void testRestartMixedStoreWPBefore() throws Exception {
        assertCleanCacheAndStoreHotrod(mixedWPCache, mixedWPDB.bucketTable);
        assertCleanCacheAndStoreHotrod(mixedWPCache, mixedWPDB.stringTable);

        mixedWPCache.put("k1", "v1");
        mixedWPCache.put("k2", "v2");
        assertEquals(2, mixedWPDB.stringTable.getAllRows().size());
        assertTrue(!mixedWPDB.bucketTable.exists() || mixedWPDB.bucketTable.getAllRows().isEmpty());
    }

    public void testRestartMixedStoreWPAfter() throws Exception {
        eventually(new ITestUtils.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return 2 == server.getCacheManager(mixedWPMBeans.managerName).getCache(mixedWPMBeans.cacheName).getNumberOfEntries();
            }
        }, 10000);
        assertEquals(2, mixedWPDB.stringTable.getAllRows().size());
        assertEquals("v1", mixedWPCache.get("k1"));
        assertEquals("v2", mixedWPCache.get("k2"));
        mixedWPCache.remove("k2");
        assertNull(mixedWPDB.stringTable.getValueByKey(getStoredStringKey(mixedWPCache, "k2")));
    }

    public void assertCleanCacheAndStoreHotrod(RemoteCache cache, final DBServer.TableManipulation table) throws Exception {
        cache.clear();
        if (table.exists() && !table.getAllRows().isEmpty()) {
            table.deleteAllRows();
            eventually(new ITestUtils.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return table.getAllRows().isEmpty();
                }
            }, 10000);
        }
    }

    public Object getFirstKeyInBucket(RemoteCache cache, DBServer db) throws Exception {
        Bucket bucket = db.bucketTable.getFirstNonEmptyBucket(globalMarshaller);
        assertNotNull(bucket);
        Map<Object, MarshalledEntry> entries = bucket.getStoredEntries();
        assertNotNull(entries);
        assertEquals(1, entries.size());
        WrappedByteArray key = (WrappedByteArray) entries.keySet().iterator().next();
        return getRealKeyFromStored(key.getBytes(), cache);
    }

    // gets the database representation of the String key stored with hotrod client (when using string store)
    public String getStoredStringKey(RemoteCache cache, String key) throws IOException, InterruptedException {
        // 1. marshall the key
        // 2. encode it with base64 (that's what DefaultTwoWayKey2StringMapper does)
        // 3. prefix it with 8 (again, done by DefaultTwoWayKey2StringMapper to mark the key as wrapped byte array type)
        // 4. prefix it with UTF-16 BOM (that is what DefaultTwoWayKey2StringMapper does for non string values)
        return '\uFEFF' + "8" + Base64.getEncoder().encodeToString(cache.getRemoteCacheManager().getMarshaller().objectToByteBuffer(key));
    }

    private StreamingMarshaller getMarshaller(RemoteCache cache) {
        return (StreamingMarshaller) cache.getRemoteCacheManager().getMarshaller();
    }

    private String getStoredKey(RemoteCache cache, String key) throws IOException, InterruptedException, ClassNotFoundException {
        Object o = getMarshaller(cache).objectFromByteBuffer(Base64.getDecoder().decode(key.substring(2)));
        log.tracef("Key in DB=%s > %s", key, o);
        return (String)o;
    }

    public RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans mbeans) {
        return rcmFactory.createCache(mbeans);
    }

}
