package org.infinispan.hibernate.search.spi;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.hibernate.search.cfg.Environment;
import org.hibernate.search.engine.service.spi.ServiceManager;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.spi.BuildContext;
import org.hibernate.search.store.spi.DirectoryHelper;
import org.hibernate.search.store.spi.LockFactoryCreator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.hibernate.search.impl.AsyncDeleteExecutorService;
import org.infinispan.hibernate.search.impl.LoggerFactory;
import org.infinispan.hibernate.search.logging.Log;
import org.infinispan.hibernate.search.util.configuration.impl.ConfigurationParseHelper;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A DirectoryProvider using Infinispan to store the Index. This depends on the CacheManagerServiceProvider to get a
 * reference to the Infinispan {@link EmbeddedCacheManager}.
 *
 * @author Sanne Grinovero
 */
public class InfinispanDirectoryProvider implements org.hibernate.search.store.DirectoryProvider<Directory> {

   private static final Log log = LoggerFactory.make();

   private ServiceManager serviceManager;
   private String directoryProviderName;

   private String metadataCacheName;
   private String dataCacheName;
   private String lockingCacheName;
   private Integer chunkSize;

   private Directory directory;

   private EmbeddedCacheManager cacheManager;

   private AsyncDeleteExecutorService deletesExecutor;

   private boolean writeFileListAsync;

   private LockFactory indexWriterLockFactory;
   private int affinityId;


   public InfinispanDirectoryProvider(int affinityId) {
      this.affinityId = affinityId;
   }

   public InfinispanDirectoryProvider() {
      this.affinityId = -1;
   }

   @Override
   public void initialize(String directoryProviderName, Properties properties, BuildContext context) {
      this.directoryProviderName = directoryProviderName;
      this.serviceManager = context.getServiceManager();
      this.cacheManager = serviceManager.requestService(CacheManagerService.class).getEmbeddedCacheManager();
      metadataCacheName = InfinispanIntegration.getMetadataCacheName(properties);
      dataCacheName = InfinispanIntegration.getDataCacheName(properties);
      lockingCacheName = InfinispanIntegration.getLockingCacheName(properties);
      //Let it return null if it's not set, so that we can avoid applying any override.
      chunkSize = ConfigurationParseHelper.getIntValue(properties, "chunk_size");
      writeFileListAsync = getWriteFileListAsync(properties);

      //Only override the default Infinispan LockDirectory if an explicit option is set:
      if (configurationExplicitlySetsLockFactory(properties)) {
         File verifiedIndexDir = null;
         if (isNativeLockingStrategy(properties)) {
            verifiedIndexDir = DirectoryHelper.getVerifiedIndexDir(
                  directoryProviderName,
                  properties,
                  true
            );
         }
         indexWriterLockFactory = getLockFactory(verifiedIndexDir, properties);
      }
   }

   private LockFactory getLockFactory(File indexDir, Properties properties) {
      try {
         return serviceManager.requestService(LockFactoryCreator.class).createLockFactory(indexDir, properties);
      } finally {
         serviceManager.releaseService(LockFactoryCreator.class);
      }
   }

   private boolean getWriteFileListAsync(Properties properties) {
      return ConfigurationParseHelper.getBooleanValue(
            properties,
            InfinispanIntegration.WRITE_METADATA_ASYNC,
            false
      );
   }

   /**
    * @param dirConfiguration the properties representing the configuration for this index
    * @return {@code true} if the configuration contains an override for the locking_strategy
    */
   private boolean configurationExplicitlySetsLockFactory(Properties dirConfiguration) {
      return dirConfiguration.getProperty(Environment.LOCKING_STRATEGY) != null;
   }

   private boolean isNativeLockingStrategy(Properties dirConfiguration) {
      return "native".equals(dirConfiguration.getProperty(Environment.LOCKING_STRATEGY));
   }

   @Override
   public void start(DirectoryBasedIndexManager indexManager) {
      log.debug("Starting InfinispanDirectory");
      deletesExecutor = getDeleteOperationsExecutor();
      validateCacheManagerConfiguration();
      cacheManager.startCaches(metadataCacheName, dataCacheName, lockingCacheName);
      Cache<?, ?> metadataCache = cacheManager.getCache(metadataCacheName);
      Cache<?, ?> dataCache = cacheManager.getCache(dataCacheName);
      Cache<?, ?> lockingCache = cacheManager.getCache(lockingCacheName);
      org.infinispan.lucene.directory.BuildContext directoryBuildContext = DirectoryBuilder
            .newDirectoryInstance(metadataCache, dataCache, lockingCache, directoryProviderName)
            .writeFileListAsynchronously(writeFileListAsync)
            .deleteOperationsExecutor(deletesExecutor.getExecutor());
      if (chunkSize != null) {
         directoryBuildContext.chunkSize(chunkSize.intValue());
      }
      if (indexWriterLockFactory != null) {
         directoryBuildContext.overrideWriteLocker(indexWriterLockFactory);
      }
      if (affinityId >= 0) {
         directoryBuildContext.affinityLocationIntoSegment(affinityId);

      }
      directory = directoryBuildContext.create();
      DirectoryHelper.initializeIndexIfNeeded(directory);
      log.debugf("Initialized Infinispan index: '%s'", directoryProviderName);
   }

   private void validateCacheManagerConfiguration() {
      if (cacheManager.getCacheConfiguration(metadataCacheName) == null) {
         log.missingIndexCacheConfiguration(metadataCacheName);
         ConfigurationBuilder builder = new ConfigurationBuilder();
         if (cacheManager.getCacheManagerConfiguration().isClustered()) {
            // Clustered Metadata cache configuration
            builder
                  .clustering().cacheMode(CacheMode.REPL_SYNC).remoteTimeout(25000)
                  .stateTransfer().awaitInitialTransfer(true).timeout(480000)
                  .locking().useLockStriping(false).lockAcquisitionTimeout(10000).concurrencyLevel(500).writeSkewCheck(false)
            ;
         } else {
            builder.simpleCache(true);
         }
         cacheManager.defineConfiguration(metadataCacheName, builder.build());
      }

      if (cacheManager.getCacheConfiguration(dataCacheName) == null) {
         log.missingIndexCacheConfiguration(dataCacheName);
         ConfigurationBuilder builder = new ConfigurationBuilder();
         if (cacheManager.getCacheManagerConfiguration().isClustered()) {
            // Clustered Metadata cache configuration
            builder
                  .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(25000)
                  .stateTransfer().awaitInitialTransfer(true).timeout(480000)
                  .locking().useLockStriping(false).lockAcquisitionTimeout(10000).concurrencyLevel(500).writeSkewCheck(false)
            ;
         } else {
            builder.simpleCache(true);
         }
         cacheManager.defineConfiguration(dataCacheName, builder.build());
      }

      if (cacheManager.getCacheConfiguration(lockingCacheName) == null) {
         log.missingIndexCacheConfiguration(lockingCacheName);
         ConfigurationBuilder builder = new ConfigurationBuilder();
         if (cacheManager.getCacheManagerConfiguration().isClustered()) {
            // Clustered Metadata cache configuration
            builder
                  .clustering().cacheMode(CacheMode.REPL_SYNC).remoteTimeout(25000)
                  .stateTransfer().awaitInitialTransfer(true).timeout(480000)
                  .locking().useLockStriping(false).lockAcquisitionTimeout(10000).concurrencyLevel(500).writeSkewCheck(false)
            ;
         } else {
            builder.simpleCache(true);
         }
         cacheManager.defineConfiguration(lockingCacheName, builder.build());
      }
   }

   private AsyncDeleteExecutorService getDeleteOperationsExecutor() {
      return serviceManager.requestService(AsyncDeleteExecutorService.class);
   }

   @Override
   public void stop() {
      deletesExecutor.closeAndFlush();
      serviceManager.releaseService(AsyncDeleteExecutorService.class);
      try {
         directory.close();
      } catch (IOException e) {
         log.unableToCloseLuceneDirectory(directory, e);
      }
      serviceManager.releaseService(CacheManagerService.class);
      log.debug("Stopped InfinispanDirectory");
   }

   @Override
   public Directory getDirectory() {
      return directory;
   }

   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

}
