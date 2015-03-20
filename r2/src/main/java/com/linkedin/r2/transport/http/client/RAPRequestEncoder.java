package com.linkedin.r2.transport.http.client;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.transport.http.common.HttpConstants;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpChunkTrailer;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.net.URL;
import java.util.Map;

/**
* @author Zhenkai Zhu
*/
/** package private */class RAPRequestEncoder implements ChannelDownstreamHandler
{

  private static final int MAX_BUFFER_SIZE = 1024 * 128;

  @Override
  public void handleDownstream(final ChannelHandlerContext ctx, final ChannelEvent e)
          throws Exception
  {

    if( e instanceof MessageEvent)
    {

      MessageEvent event = (MessageEvent) e;
      StreamRequest request = (StreamRequest) event.getMessage();

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
      for (Map.Entry<String, String> entry : request.getHeaders().entrySet())
      {
        nettyRequest.setHeader(entry.getKey(), entry.getValue());
      }

      nettyRequest.setHeader(HttpConstants.REQUEST_COOKIE_HEADER_NAME, request.getCookies());

      // code in if block works; for now always use streaming during dev
      //if (request instanceof RestRequest)
      if (false)
      {
        // this is small optimization for RestRequest so that we don't chunk over the wire because we
        // don't really gain anything for chunking in such case, but slightly increase the transmitting overhead
        final ByteString entity = ((RestRequest) request).getEntity();
        ChannelBuffer buf = ChannelBuffers.wrappedBuffer(entity.asByteBuffer());
        nettyRequest.setContent(buf);
        nettyRequest.setHeader(HttpHeaders.Names.CONTENT_LENGTH, entity.length());
        Channels.write(ctx, Channels.future(ctx.getChannel()), nettyRequest);

      }
      else
      {
        nettyRequest.setChunked(true);
        nettyRequest.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
        ChannelFuture future = Channels.future(ctx.getChannel());
        final EntityStream entityStream = request.getEntityStream();
        future.addListener(new ChannelFutureListener()
        {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception
          {
            entityStream.setReader(new BufferedReader(ctx, MAX_BUFFER_SIZE));
          }
        });

        Channels.write(ctx, future, nettyRequest);

      }
    }
    else
    {
      ctx.sendDownstream(e);
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

    public void onDataAvailable(ByteString data)
    {
      // No need to do smaller chunking here as that won't buy us anything
      // jetty buffer size by default is 8k, and it's parsing is smarter enought so that it
      // won't wait to buffer the whole chunk if the chunk is larger than 8k, so the memory consumption
      // won't not be affected by the chunk size here.
      ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(data.asByteBuffer());
      HttpChunk chunk = new DefaultHttpChunk(channelBuffer);
      ChannelFuture writeFuture = Channels.future(_ctx.getChannel());
      final int dataLen = data.length();
      writeFuture.addListener(new ChannelFutureListener()
      {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception
        {
          // data have been written out, we can tell the writer that we can accept more bytes
          _readHandle.read(dataLen);
        }
      });
      Channels.write(_ctx, writeFuture, chunk);
    }

    public void onDone()
    {
      Channels.write(_ctx, Channels.future(_ctx.getChannel()), HttpChunk.LAST_CHUNK);
    }

    public void onError(Throwable e)
    {
      Channels.fireExceptionCaught(_ctx, e);
    }

  }
}
