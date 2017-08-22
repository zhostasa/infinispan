package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class ConnectionPoolConfiguration {

   static final AttributeDefinition<ExhaustedAction> EXHAUSTED_ACTION = AttributeDefinition.builder("exhaustedAction", ExhaustedAction.WAIT, ExhaustedAction.class).immutable().build();
   static final AttributeDefinition<Integer> MAX_ACTIVE = AttributeDefinition.builder("maxActive", -1).immutable().build();
   static final AttributeDefinition<Integer> MAX_TOTAL = AttributeDefinition.builder("maxTotal", -1).immutable().build();
   static final AttributeDefinition<Integer> MAX_IDLE = AttributeDefinition.builder("maxIdle", -1).immutable().build();
   static final AttributeDefinition<Integer> MIN_IDLE = AttributeDefinition.builder("minIdle", -1).immutable().build();
   static final AttributeDefinition<Long> TIME_BETWEEN_EVICTION_RUNS = AttributeDefinition.builder("timeBetweenEvictionRuns", 120000L).immutable().build();
   static final AttributeDefinition<Long> MIN_EVICTABLE_IDLE_TIME = AttributeDefinition.builder("minEvictableIdleTime", 1800000L).immutable().build();
   static final AttributeDefinition<Boolean> TEST_WHILE_IDLE = AttributeDefinition.builder("testWhileIdle", true).immutable().build();

   private final ExhaustedAction exhaustedAction;
   private final int maxActive;
   private final int maxTotal;
   private final int maxIdle;
   private final int minIdle;
   private final long timeBetweenEvictionRuns;
   private final long minEvictableIdleTime;
   private final boolean testWhileIdle;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ConnectionPoolConfiguration.class, EXHAUSTED_ACTION, MAX_ACTIVE, MAX_TOTAL, MAX_IDLE,
            MIN_IDLE, TIME_BETWEEN_EVICTION_RUNS, MIN_EVICTABLE_IDLE_TIME, TEST_WHILE_IDLE);
   }
   ConnectionPoolConfiguration(ExhaustedAction exhaustedAction, int maxActive, int maxTotal, int maxIdle, int minIdle,
         long timeBetweenEvictionRuns, long minEvictableIdleTime, boolean testWhileIdle) {
      this.exhaustedAction = exhaustedAction;
      this.maxActive = maxActive;
      this.maxTotal = maxTotal;
      this.maxIdle = maxIdle;
      this.minIdle = minIdle;
      this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
      this.minEvictableIdleTime = minEvictableIdleTime;
      this.testWhileIdle = testWhileIdle;
   }

   public ExhaustedAction exhaustedAction() {
      return exhaustedAction;
   }

   public int maxActive() {
      return maxActive;
   }

   public int maxTotal() {
      return maxTotal;
   }

   public int maxIdle() {
      return maxIdle;
   }

   public int minIdle() {
      return minIdle;
   }

   public long timeBetweenEvictionRuns() {
      return timeBetweenEvictionRuns;
   }

   public long minEvictableIdleTime() {
      return minEvictableIdleTime;
   }

   public boolean testWhileIdle() {
      return testWhileIdle;
   }

   @Override
   public String toString() {
      return "ConnectionPoolConfiguration [exhaustedAction=" + exhaustedAction + ", maxActive=" + maxActive
            + ", maxTotal=" + maxTotal + ", maxIdle=" + maxIdle + ", minIdle=" + minIdle + ", timeBetweenEvictionRuns="
            + timeBetweenEvictionRuns + ", minEvictableIdleTime=" + minEvictableIdleTime + ", testWhileIdle="
            + testWhileIdle + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ConnectionPoolConfiguration that = (ConnectionPoolConfiguration) o;

      if (maxActive != that.maxActive) return false;
      if (maxTotal != that.maxTotal) return false;
      if (maxIdle != that.maxIdle) return false;
      if (minIdle != that.minIdle) return false;
      if (timeBetweenEvictionRuns != that.timeBetweenEvictionRuns) return false;
      if (minEvictableIdleTime != that.minEvictableIdleTime) return false;
      if (testWhileIdle != that.testWhileIdle) return false;
      return exhaustedAction == that.exhaustedAction;

   }

   @Override
   public int hashCode() {
      int result = exhaustedAction != null ? exhaustedAction.hashCode() : 0;
      result = 31 * result + maxActive;
      result = 31 * result + maxTotal;
      result = 31 * result + maxIdle;
      result = 31 * result + minIdle;
      result = 31 * result + (int) (timeBetweenEvictionRuns ^ (timeBetweenEvictionRuns >>> 32));
      result = 31 * result + (int) (minEvictableIdleTime ^ (minEvictableIdleTime >>> 32));
      result = 31 * result + (testWhileIdle ? 1 : 0);
      return result;
   }
}
