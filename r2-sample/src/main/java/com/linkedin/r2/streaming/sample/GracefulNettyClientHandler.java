package com.linkedin.r2.streaming.sample;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;


import java.net.URL;
import java.util.Map;


/**
 * This illustrates how the Reader on the Netty Client side would look like. This is just to address Chris's concern
 * about inter-locking between Reader and Writer. Basically we just introduce some kind of pipelining or buffering
 * instead of the inter-locking step of read-one-chunk-wait-for-it-to-be-written-then-request-the-next-chunk problem.
 * @author Zhenkai Zhu
 */
public class GracefulNettyClientHandler implements ChannelDownstreamHandler
{
  final private RequestEncoder _encoder = new RequestEncoder();

  @Override
  public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception
  {
    _encoder.handleDownstream(ctx, e);
  }

  private class RequestEncoder extends OneToOneEncoder
  {
    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
        throws Exception
    {
      RestRequest request = (RestRequest) msg;

      // hook up the reader with the EntityStream of the request
      // the maximum pipelining or buffering is 256 KB
      EntityStream entityStream = request.getEntityStream();
      entityStream.setReader(new BufferedReader(ctx, 256 * 1024));

      HttpMethod nettyMethod = HttpMethod.valueOf(request.getMethod());
      URL url = new URL(request.getURI().toString());
      String path = url.getFile();
      // RFC 2616, section 5.1.2:
      //   Note that the absolute path cannot be empty; if none is present in the original URI,
      //   it MUST be given as "/" (the server root).
      if (path.isEmpty())
      {
        path = "/";
      }

      HttpRequest nettyRequest =
          new DefaultHttpRequest(HttpVersion.HTTP_1_1, nettyMethod, path);

      nettyRequest.setHeader(HttpHeaders.Names.HOST, url.getAuthority());
      for (Map.Entry<String, String> e : request.getHeaders().entrySet())
      {
        nettyRequest.setHeader(e.getKey(), e.getValue());
      }
      nettyRequest.setChunked(true);

      // set out the headers first
      return nettyRequest;
    }
  }

  /**
   * A reader that has pipelining/buffered reading
   *
   * The bufferSize = the permitted size that the writer can still write + the size of data that has been provided
   * by writer but not yet written to socket
   */
  private static class BufferedReader implements Reader
  {
    final private int _bufferSize;
    final private ChannelHandlerContext _ctx;
    private ReadHandle _readHandle;

    BufferedReader(ChannelHandlerContext ctx, int bufferSize)
    {
      _bufferSize = bufferSize;
      _ctx = ctx;
    }

    public void onInit(ReadHandle rh)
    {
      _readHandle = rh;
      // signal the Writer that we can accept _bufferSize bytes
      _readHandle.read(_bufferSize);
    }

    public void onReadPossible(ByteString data)
    {
      ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(data.asByteBuffer());
      HttpChunk chunk = new DefaultHttpChunk(channelBuffer);
      final int dataSize = data.length();
      ChannelFuture writeFuture = _ctx.getChannel().write(chunk);
      writeFuture.addListener(new ChannelFutureListener()
      {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception
        {
          // dataSize bytes have been written out, we can tell the writer that we can accept dataSize more bytes
          _readHandle.read(dataSize);
        }
      });
    }

    public void onDone()
    {
      ChannelFuture writeFuture = _ctx.getChannel().write(HttpChunk.LAST_CHUNK);
      writeFuture.addListener(ChannelFutureListener.CLOSE);
    }

    public void onError(Throwable e)
    {
      _ctx.getChannel().close();
    }

  }
}
