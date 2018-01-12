package org.infinispan.xsite;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Mircea Markus
 * @since 5.2
 * @private
 */
@Listener
public class BackupReceiverRepositoryImpl implements BackupReceiverRepository {

   private static Log log = LogFactory.getLog(BackupReceiverRepositoryImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentMap<SiteCachePair, BackupReceiver> backupReceivers = new ConcurrentHashMap<>();

   public EmbeddedCacheManager cacheManager;

   @Inject
   public void setup(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Start
   public void start() {
      cacheManager.addListener(this);
   }

   @Stop
   public void stop() {
      cacheManager.removeListener(this);
   }

   @CacheStopped
   public void cacheStopped(CacheStoppedEvent cse) {
      log.debugf("Processing cache stop: %s. Cache name: '%s'", cse, cse.getCacheName());
      for (SiteCachePair scp : backupReceivers.keySet()) {
         log.debugf("Processing entry %s", scp);
         if (scp.localCacheName.equals(cse.getCacheName())) {
            log.debugf("Deregistering backup receiver %s", scp);
            backupReceivers.remove(scp);
         }
      }
   }

   /**
    * Returns the local cache associated defined as backup for the provided remote (site, cache) combo, or throws an
    * exception of no such site is defined.
    * <p/>
    * Also starts the cache if not already stated; that is because the cache is needed for update after when this method
    * is invoked.
    */
   @Override
   public BackupReceiver getBackupReceiver(String remoteSite, String remoteCache) {
      SiteCachePair toLookFor = new SiteCachePair(remoteCache, remoteSite);
      BackupReceiver backupManager = backupReceivers.get(toLookFor);
      if (backupManager != null) return backupManager;

      //check the default cache first
      Configuration configuration = cacheManager.getDefaultCacheConfiguration();
      if (configuration != null && isBackupForRemoteCache(toLookFor, configuration)) {
         return setBackupToUse(cacheManager.getCache(), toLookFor, EmbeddedCacheManager.DEFAULT_CACHE_NAME);
      }

      Set<String> cacheNames = cacheManager.getCacheNames();
      for (String name : cacheNames) {
         configuration = cacheManager.getCacheConfiguration(name);
         if (configuration != null && isBackupForRemoteCache(toLookFor, configuration)) {
            return setBackupToUse(cacheManager.getCache(name), toLookFor, name);
         }
      }
      log.debugf("Did not find any backup explicitly configured backup cache for remote cache/site: %s/%s. Using %s",
                 remoteSite, remoteCache, remoteCache);

      return setBackupToUse(cacheManager.getCache(remoteCache), toLookFor, remoteCache);
   }

   private boolean isBackupForRemoteCache(SiteCachePair toLookFor, Configuration configuration) {
      return configuration.sites().backupFor().isBackupFor(toLookFor.remoteSite,toLookFor.remoteCache);
   }

   private BackupReceiver setBackupToUse(Cache<Object,Object> cache, SiteCachePair toLookFor, String cacheName) {
      if (trace) {
         log.tracef("Found local cache '%s' is backup for cache '%s' from site '%s'",
               cacheName, toLookFor.remoteCache, toLookFor.remoteSite);
      }
      backupReceivers.putIfAbsent(toLookFor, createBackupReceiver(cache));
      toLookFor.setLocalCacheName(cacheName);
      return backupReceivers.get(toLookFor);
   }

   static class SiteCachePair {
      public final String remoteSite;
      public final String remoteCache;
      String localCacheName;

      /**
       * Important: do not include the localCacheName field in the equals and hash code comparison. This is mainly used
       * as a key in a map and the localCacheName field might change causing troubles.
       */
      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof SiteCachePair)) return false;

         SiteCachePair that = (SiteCachePair) o;

         return remoteCache.equals(that.remoteCache) && remoteSite.equals(that.remoteSite);
      }

      @Override
      public int hashCode() {
         return  31 * remoteSite.hashCode() + remoteCache.hashCode();
      }

      SiteCachePair(String remoteCache, String remoteSite) {
         this.remoteCache = remoteCache;
         this.remoteSite = remoteSite;
      }

      void setLocalCacheName(String localCacheName) {
         this.localCacheName = localCacheName;
      }

      @Override
      public String toString() {
         return "SiteCachePair{" +
               "site='" + remoteSite + '\'' +
               ", cache='" + remoteCache + '\'' +
               '}';
      }
   }

   public void replace(String site, String cache, BackupReceiver bcr) {
      backupReceivers.replace(new SiteCachePair(cache, site), bcr);
   }

   public BackupReceiver get(String site, String cache) {
      return backupReceivers.get(new SiteCachePair(site, cache));
   }

   private static BackupReceiver createBackupReceiver(Cache<Object,Object> cache) {
      Cache<Object, Object> receiverCache = SecurityActions.getUnwrappedCache(cache);
      return receiverCache.getCacheConfiguration().clustering().cacheMode().isClustered() ?
            new ClusteredCacheBackupReceiver(receiverCache) :
            new LocalCacheBackupReceiver(receiverCache);
   }
}
