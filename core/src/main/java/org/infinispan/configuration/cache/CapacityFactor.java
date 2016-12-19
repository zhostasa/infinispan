package org.infinispan.configuration.cache;

/**
 * Workaround for JDG-118 - makes capacity factor invisible in javadoc
 *
 * @private
 */
public class CapacityFactor {
    public static float capacityFactor(HashConfiguration hash) {
        return hash.capacityFactor();
    }

    public static HashConfigurationBuilder capacityFactor(HashConfigurationBuilder builder, float capacityFactor) {
        return builder.capacityFactor(capacityFactor);
    }
}
