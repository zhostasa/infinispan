package org.infinispan.tools.config.v6;

import java.util.HashMap;
import java.util.Map;

/**
 * An enumeration of all the recognized XML element local names, by name.
 *
 * @author Pete Muir
 */
public enum Element {
    // must be first
    UNKNOWN(null),

    //
    ADVANCED_EXTERNALIZER("advancedExternalizer"),
    ADVANCED_EXTERNALIZERS("advancedExternalizers"),
    ASYNC("async"),
    ASYNC_LISTENER_EXECUTOR("asyncListenerExecutor"),
    ASYNC_OPERATIONS_EXECUTOR("asyncOperationsExecutor"),
    AUTHORIZATION("authorization"),
    PERSISTENCE_EXECUTOR("persistenceExecutor"),
    ASYNC_TRANSPORT_EXECUTOR("asyncTransportExecutor"),
    REMOTE_COMMAND_EXECUTOR("remoteCommandsExecutor"),
    CLUSTERING("clustering"),
    CLUSTER_STORE("cluster"),
    CLUSTER_ROLE_MAPPER("cluster-role-mapper"),
    COMMON_NAME_ROLE_MAPPER("common-name-role-mapper"),
    COMPATIBILITY("compatibility"),
    CUSTOM_INTERCEPTORS("customInterceptors"),
    CUSTOM_ROLE_MAPPER("custom-role-mapper"),
    DATA_CONTAINER("dataContainer"),
    DEADLOCK_DETECTION("deadlockDetection"),
    DEFAULT("default"),
    EVICTION("eviction"),
    EVICTION_SCHEDULED_EXECUTOR("evictionScheduledExecutor"),
    EXPIRATION("expiration"),
    SINGLE_FILE_STORE("singleFile"),
    GROUPS("groups"),
    GROUPER("grouper"),
    GLOBAL("global"),
    GLOBAL_STATE("global-state"),
    GLOBAL_JMX_STATISTICS("globalJmxStatistics"),
    HASH("hash"),
    IDENTITY_ROLE_MAPPER("identity-role-mapper"),
    INDEXING("indexing"),
    INTERCEPTOR("interceptor"),
    INVOCATION_BATCHING("invocationBatching"),
    JMX_STATISTICS("jmxStatistics"),
    L1("l1"),
    LAZY_DESERIALIZATION("lazyDeserialization"),
    PERSISTENCE("persistence"),
    PERSISTENT_LOCATION("persistent-location"),
    LOCKING("locking"),
    MODULES("modules"),
    NAMED_CACHE("namedCache"),
    PROPERTIES("properties"),
    PROPERTY("property"),
    RECOVERY("recovery"),
    REPLICATION_QUEUE_SCHEDULED_EXECUTOR("replicationQueueScheduledExecutor"),
    ROLE("role"),
    ROOT("infinispan"),
    SECURITY("security"),
    SERIALIZATION("serialization"),
    SHUTDOWN("shutdown"),
    SINGLETON_STORE("singleton"),
    STATE_RETRIEVAL("stateRetrieval"),
    STATE_TRANSFER("stateTransfer"),
    STATE_TRANSFER_EXECUTOR("stateTransferExecutor"),
    STORE("store"),
    STORE_AS_BINARY("storeAsBinary"),
    SYNC("sync"),
    TRANSACTION("transaction"),
    TRANSPORT("transport"),
    UNSAFE("unsafe"),
    VERSIONING("versioning"),
    SITES("sites"),
    SITE("site"),
    BACKUPS("backups"),
    BACKUP("backup"),
    BACKUP_FOR("backupFor"),
    TAKE_OFFLINE("takeOffline"),
    TOTAL_ORDER_EXECUTOR("totalOrderExecutor"),
    PARTITION_HANDLING("partitionHandling"),
    ;

    private final String name;

    Element(final String name) {
        this.name = name;
    }

    /**
     * Get the local name of this element.
     *
     * @return the local name
     */
    public String getLocalName() {
        return name;
    }

    private static final Map<String, Element> MAP;

    static {
        final Map<String, Element> map = new HashMap<String, Element>(8);
        for (Element element : values()) {
            final String name = element.getLocalName();
            if (name != null) map.put(name, element);
        }
        MAP = map;
    }

    public static Element forName(String localName) {
        final Element element = MAP.get(localName);
        return element == null ? UNKNOWN : element;
    }
}
