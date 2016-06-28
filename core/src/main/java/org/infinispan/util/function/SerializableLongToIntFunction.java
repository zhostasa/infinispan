package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.LongToIntFunction;

/**
 * This is a functional interface that is the same as a {@link LongToIntFunction} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 8.3
 */
public interface SerializableLongToIntFunction extends Serializable, LongToIntFunction {
}
