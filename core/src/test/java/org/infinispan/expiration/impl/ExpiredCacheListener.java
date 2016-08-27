package org.infinispan.expiration.impl;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;

@Listener
public class ExpiredCacheListener {
   List<CacheEntryExpiredEvent> events = new ArrayList<>();
   int invocationCount;

   public void reset() {
      events.clear();
      invocationCount = 0;
   }

   public List<CacheEntryExpiredEvent> getEvents() {
      return events;
   }

   public int getInvocationCount() {
      return invocationCount;
   }


   // handler

   @CacheEntryExpired
   public void handle(CacheEntryExpiredEvent e) {
      events.add(e);

      invocationCount++;
   }
}
