package org.infinispan.commons.io;

/**
 * @author Mircea Markus
 * @deprecated Since 8.3, will be removed.
 */
@Deprecated
public class ByteBufferFactoryImpl implements ByteBufferFactory {

   @Override
   public ByteBuffer newByteBuffer(byte[] b, int offset, int length) {
      return new ByteBufferImpl(b, offset, length);
   }
}
