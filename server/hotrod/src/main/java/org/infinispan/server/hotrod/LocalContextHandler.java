package org.infinispan.server.hotrod;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.security.Security;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.server.hotrod.util.BulkUtil;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import scala.None$;
import scala.Option;
import scala.Tuple2;
import scala.Tuple4;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.util.BitSet;
import java.util.Map;

import static org.infinispan.server.hotrod.ResponseWriting.writeResponse;

/**
 * Handler that performs actual cache operations.  Note this handler should be on a separate executor group than the
 * decoder.
 *
 * @author wburns
 * @since 8.3
 */
public class LocalContextHandler extends ChannelInboundHandlerAdapter {
   private final NettyTransport transport;

   public LocalContextHandler(NettyTransport transport) {
      this.transport = transport;
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof CacheDecodeContext) {
         CacheDecodeContext cdc = (CacheDecodeContext) msg;
         Subject subject = ((CacheDecodeContext) msg).subject;
         if (subject == null)
            realChannelRead(ctx, msg, cdc);
         else Security.doAs(subject, (PrivilegedExceptionAction<Void>) () -> {
            realChannelRead(ctx, msg, cdc);
            return null;
         });
      } else {
         super.channelRead(ctx, msg);
      }
   }

   private void realChannelRead(ChannelHandlerContext ctx, Object msg, CacheDecodeContext cdc) throws Exception {
      HotRodHeader h = cdc.header;
      switch (h.op) {
         case CONTAINS_KEY:
            writeResponse(cdc, ctx.channel(), cdc.containsKey());
            break;
         case GET:
         case GET_WITH_VERSION:
            writeResponse(cdc, ctx.channel(), cdc.get());
            break;
         case GET_WITH_METADATA:
            writeResponse(cdc, ctx.channel(), cdc.getKeyMetadata());
            break;
         case PING:
            writeResponse(cdc, ctx.channel(), new EmptyResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, HotRodOperation.PING, OperationStatus.Success, h.topologyId));
            break;
         case STATS:
            writeResponse(cdc, ctx.channel(), cdc.decoder.createStatsResponse(cdc, transport));
            break;
         default:
            super.channelRead(ctx, msg);
      }
   }

}
