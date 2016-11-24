package org.infinispan.marshall.core;

import java.io.Serializable;

// Temporary class, should become active by default when testing
final class ExternallyMarshallable {

   private ExternallyMarshallable() {
      // Static class
   }

   public static boolean isAllowed(Object obj) {
      Class<?> clazz = obj.getClass();
      return isAllowed(clazz);
   }

   public static boolean isAllowed(Class<?> clazz) {
      Package pkg = clazz.getPackage();
      if (pkg == null) {
         if (clazz.isArray()) {
            return false;
         } else {
            throw new IllegalStateException("No package info for " + clazz + ", runtime-generated class?");
         }
      }
      String pkgName = pkg.getName();
      boolean isBlackList =
            Serializable.class.isAssignableFrom(clazz)
            && isMarshallablePackage(pkgName)
            && !isWhiteList(clazz.getName());
      return !isBlackList;
   }

   private static boolean isMarshallablePackage(String pkg) {
      return pkg.startsWith("java.")
            || pkg.startsWith("org.infinispan.")
            || pkg.startsWith("org.jgroups.")
            || pkg.startsWith("org.hibernate")
            || pkg.startsWith("org.apache")
            || pkg.startsWith("org.jboss")
            || pkg.startsWith("com.arjuna")
            ;
   }

   private static boolean isWhiteList(String className) {
      return className.endsWith("Exception")
            || className.contains("$$Lambda$")
            || className.equals("java.lang.Class")
            || className.equals("java.util.Date") // test
            || className.equals("org.infinispan.api.lazy.LazyCacheAPITest$CustomPojo")
            || className.equals("org.infinispan.atomic.TestDeltaAware")
            || className.equals("org.infinispan.atomic.TestDeltaAware$TestDelta")
            || className.equals("org.infinispan.configuration.override.XMLConfigurationOverridingTest$NonIndexedClass")
            || className.equals("org.infinispan.context.MarshalledValueContextTest$Key")
            || className.equals("org.infinispan.distribution.MagicKey")
            || className.equals("org.infinispan.distribution.SingleOwnerTest$ExceptionExternalizable")
            || className.equals("org.infinispan.distribution.ch.AffinityPartitionerTest$AffinityKey")
            || className.equals("org.infinispan.distribution.groups.BaseUtilGroupTest$GroupKey")
            || className.equals("org.infinispan.distribution.groups.StateTransferGetGroupKeysTest$CustomConsistentHashFactory")
            || className.equals("org.infinispan.distribution.rehash.NonTxBackupOwnerBecomingPrimaryOwnerTest$CustomConsistentHashFactory")
            || className.equals("org.infinispan.distribution.rehash.NonTxPrimaryOwnerBecomingNonOwnerTest$CustomConsistentHashFactory")
            || className.equals("org.infinispan.distexec.BasicDistributedExecutorTest$ExceptionThrowingCallable")
            || className.equals("org.infinispan.distexec.BasicDistributedExecutorTest$FailOnlyOnceDistributedCallable")
            || className.equals("org.infinispan.distexec.BasicDistributedExecutorTest$SimpleCallable")
            || className.equals("org.infinispan.distexec.DefaultExecutorService$RunnableAdapter")
            || className.equals("org.infinispan.distexec.DistributedExecutionCompletionTest$SimpleCallable")
            || className.equals("org.infinispan.distexec.DistributedExecutionCompletionTest$SimpleDistributedCallable")
            || className.equals("org.infinispan.distexec.DistributedExecutorBadResponseFailoverTest$SimpleCallable")
            || className.equals("org.infinispan.distexec.DistributedExecutorTest$LongRunningCallable")
            || className.equals("org.infinispan.distexec.DistributedExecutorFailoverTest$SleepingSimpleCallable")
            || className.equals("org.infinispan.distexec.LocalDistributedExecutorTest$ExceptionThrowingCallable")
            || className.equals("org.infinispan.distexec.LocalDistributedExecutorTest$SimpleCallable")
            || className.equals("org.infinispan.distexec.LocalDistributedExecutorTest$SimpleCallableWithField")
            || className.equals("org.infinispan.distexec.LocalDistributedExecutorTest$SimpleDistributedCallable")
            || className.equals("org.infinispan.distexec.LocalDistributedExecutorTest$SleepingSimpleCallable")
            || className.equals("org.infinispan.eviction.impl.EvictionWithConcurrentOperationsTest$SameHashCodeKey")
            || className.equals("org.infinispan.eviction.impl.ManualEvictionWithSizeBasedAndConcurrentOperationsInPrimaryOwnerTest$SameHashCodeKey")
            || className.equals("org.infinispan.eviction.impl.MarshalledValuesManualEvictionTest$ManualEvictionPojo")
            || className.equals("org.infinispan.filter.CompositeKeyValueFilterConverter")
            || className.equals("org.infinispan.jmx.RpcManagerMBeanTest$SlowToSerialize")
            || className.equals("org.infinispan.marshall.CustomClass")
            || className.equals("org.infinispan.marshall.InvalidatedMarshalledValueTest$InvalidatedPojo")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Child1")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Child2")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Human")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$HumanComparator")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Pojo")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$PojoAnnotated")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$PojoExtended")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$PojoWithExternalizer")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$PojoWithMultiExternalizer")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$PojoWithExternalAndInternal")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$PojoWhichFailsOnUnmarshalling")
            || className.equals("org.infinispan.marshall.core.JBossMarshallingTest$ObjectThatContainsACustomReadObjectMethod")
            || className.equals("org.infinispan.marshall.core.MarshalledValueTest$Pojo")
            || className.equals("org.infinispan.marshall.core.MarshalledValueTest$CustomReadObjectMethod")
            || className.equals("org.infinispan.marshall.core.MarshalledValueTest$ObjectThatContainsACustomReadObjectMethod")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$FilterConverter")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$LifespanConverter")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$LifespanFilter")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$NewLifespanLargerFilter")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$StringAppender")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$StringTruncator")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.NoOpCacheEventFilterConverterWithDependencies")
            || className.equals("org.infinispan.persistence.BaseStoreFunctionalTest$Pojo")
            || className.equals("org.infinispan.persistence.BaseStoreTest$Pojo")
            || className.equals("org.infinispan.remoting.TransportSenderExceptionHandlingTest$FailureType")
            || className.equals("org.infinispan.replication.ReplicationExceptionTest$ContainerData")
            || className.equals("org.infinispan.statetransfer.BigObject")
            || className.equals("org.infinispan.statetransfer.ReadAfterLosingOwnershipTest$SingleKeyConsistentHashFactory")
            || className.equals("org.infinispan.statetransfer.RemoteGetDuringStateTransferTest$SingleKeyConsistentHashFactory")
            || className.equals("org.infinispan.statetransfer.StateTransferCacheLoaderFunctionalTest$DelayedUnmarshal")
            || className.equals("org.infinispan.statetransfer.StateTransferFunctionalTest$DelayTransfer")
            || className.equals("org.infinispan.statetransfer.WriteSkewDuringStateTransferTest$ConsistentHashFactoryImpl")
            || className.equals("org.infinispan.stats.impl.ClusterCacheStatsImpl$DistributedCacheStatsCallable") // prod
            || className.equals("org.infinispan.stream.BaseSetupStreamIteratorTest$StringTruncator")
            || className.equals("org.infinispan.stream.BaseStreamTest$ForEachDoubleInjected")
            || className.equals("org.infinispan.stream.BaseStreamTest$ForEachInjected")
            || className.equals("org.infinispan.stream.BaseStreamTest$ForEachIntInjected")
            || className.equals("org.infinispan.stream.BaseStreamTest$ForEachLongInjected")
            || className.equals("org.infinispan.stream.DistributedStreamIteratorWithStoreAsBinaryTest$MagicKeyStringFilter")
            || className.equals("org.infinispan.test.data.Key")
            || className.equals("org.infinispan.test.data.Person")
            || className.equals("org.infinispan.xsite.BackupSender$TakeSiteOfflineResponse") // prod
            || className.equals("org.infinispan.xsite.BackupSender$BringSiteOnlineResponse") // prod
            || className.equals("org.infinispan.xsite.XSiteAdminCommand$Status") // prod

            || className.equals("org.infinispan.persistence.jdbc.TableNameUniquenessTest$Person")
            || className.equals("org.infinispan.persistence.jdbc.binary.JdbcBinaryStoreTest$FixedHashKey")
            || className.equals("org.infinispan.persistence.jdbc.stringbased.Person")

            || className.equals("org.infinispan.persistence.jpa.entity.KeyValueEntity")
            || className.equals("org.infinispan.persistence.jpa.entity.Vehicle")
            || className.equals("org.infinispan.persistence.jpa.entity.VehicleId")

            || className.equals("org.infinispan.query.affinity.Entity")
            || className.equals("org.infinispan.query.api.NotIndexedType")
            || className.equals("org.infinispan.query.clustered.QueryResponse") // prod
            || className.equals("org.infinispan.query.distributed.Block")
            || className.equals("org.infinispan.query.distributed.Transaction")
            || className.equals("org.infinispan.query.dsl.embedded.testdomain.NotIndexed")
            || className.equals("org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS")
            || className.equals("org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS")
            || className.equals("org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS")
            || className.equals("org.infinispan.query.indexedembedded.Country")
            || className.equals("org.infinispan.query.nulls.NullCollectionElementsClusteredTest$Foo")
            || className.equals("org.infinispan.query.persistence.InconsistentIndexesAfterRestartTest$SEntity")
            || className.equals("org.infinispan.query.queries.faceting.Car")
            || className.equals("org.infinispan.query.test.AnotherGrassEater")
            || className.equals("org.infinispan.query.test.CustomKey3")
            || className.equals("org.infinispan.query.test.Person")
            || className.equals("org.infinispan.query.test.VeryLongIndexNamedClass")

            || className.equals("org.hibernate.search.query.engine.impl.LuceneHSQuery") // prod

            || className.equals("org.infinispan.scripting.impl.DataType") // prod
            || className.equals("org.infinispan.scripting.impl.DistributedScript")

            || className.equals("org.infinispan.server.hotrod.CheckAddressTask") // prod

            || className.startsWith("org.infinispan.server.hotrod.event.AbstractHotRodClusterEventsTest")

            || className.equals("org.infinispan.client.hotrod.event.ClientEventsOOMTest$CustomConverterFactory$CustomConverter")
            || className.equals("org.infinispan.client.hotrod.event.ClientListenerWithFilterAndProtobufTest$CustomEventFilter")
            || className.equals("org.infinispan.client.hotrod.event.CustomEventLogListener$FilterConverterFactory$FilterConverter")
            || className.equals("org.infinispan.client.hotrod.event.CustomEventLogListener$StaticConverterFactory$StaticConverter")
            || className.equals("org.infinispan.client.hotrod.event.EventLogListener$StaticCacheEventFilterFactory$StaticCacheEventFilter")
            || className.equals("org.infinispan.client.hotrod.event.ClientListenerWithFilterAndRawProtobufTest$CustomEventFilter")
            || className.equals("org.infinispan.client.hotrod.impl.iteration.BaseMultiServerRemoteIteratorTest$SubstringFilterFactory$SubstringFilterConverter")
            || className.equals("org.infinispan.client.hotrod.impl.iteration.BaseMultiServerRemoteIteratorTest$ToHexConverterFactory$HexFilterConverter")

            || className.equals("org.infinispan.jcache.JCacheCustomKeyGenerator$CustomGeneratedCacheKey")

            || className.equals("org.infinispan.util.logging.events.EventLogLevel") // prod
            || className.equals("org.infinispan.util.logging.events.EventLogCategory") // prod
            || className.equals("java.time.Instant") // prod

            || className.startsWith("org.infinispan.test")
            || className.startsWith("org.infinispan.server.test")
            || className.startsWith("org.infinispan.it")
            || className.startsWith("org.infinispan.all")

            || className.equals("org.infinispan.server.infinispan.task.DistributedServerTask") // prod

            || className.equals("org.infinispan.jcache.annotation.DefaultCacheKey") // prod

            || className.equals("org.infinispan.security.ExecutionAuthorizationTest$SimpleCallable")
            || className.equals("org.infinispan.cdi.embedded.test.distexec.DistributedExecutorCDITest$SimpleCallable")
            || className.equals("org.infinispan.server.core.transport.NettyTransport$ConnectionAdderTask") // prod

            || className.contains("ConsistentHashFactory")

            || className.equals("org.jboss.as.clustering.infinispan.equivalence.AnyServerEquivalence")
            || className.contains("org.jboss.as.quickstarts.datagrid") // quickstarts testing
            ;
   }

}
