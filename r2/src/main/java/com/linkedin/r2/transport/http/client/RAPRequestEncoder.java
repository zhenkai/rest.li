package com.linkedin.r2.transport.http.client;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.transport.http.common.HttpConstants;

import com.linkedin.r2.util.LinkedDeque;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.net.URL;
import java.util.Map;
import java.util.Queue;

/**
* @author Zhenkai Zhu
*/
/** package private */class RAPRequestEncoder extends ChannelDuplexHandler
{
  private static final int MAX_BUFFERED_CHUNKS = 10;
  // this threshold is to mitigate the effect of the inter-play of Nagle's algorithm & Delayed ACK
  // when sending requests with small entity
  private static final int FLUSH_THRESHOLD = 8092;

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
  {
    StreamRequest request = (StreamRequest) msg;
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

    HttpRequest nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, nettyMethod, path);

    for (Map.Entry<String, String> entry : request.getHeaders().entrySet())
    {
      nettyRequest.headers().set(entry.getKey(), entry.getValue());
    }
    nettyRequest.headers().set(HttpHeaders.Names.HOST, url.getAuthority());
    nettyRequest.headers().set(HttpConstants.REQUEST_COOKIE_HEADER_NAME, request.getCookies());
    nettyRequest.headers().set(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);

    ctx.write(nettyRequest);
    Reader reader = new BufferedReader(ctx, MAX_BUFFERED_CHUNKS, FLUSH_THRESHOLD);
    request.getEntityStream().setReader(reader);
  }

  @Override
  public void flush(ChannelHandlerContext ctx)
      throws Exception
  {
    // do nothing, we control when to flush
  }

  /**
   * A reader that has pipelining/buffered reading
   *
   * Buffering is actually done by Netty; we just enforce the upper bound of the buffering
   */
  private static class BufferedReader implements Reader
  {
    private final int _maxBufferedChunks;
    private final int _flushThreshold;
    private final ChannelHandlerContext _ctx;
    private volatile ReadHandle _readHandle;
    private int _notFlushedBytes;
    private int _notFlushedChunks;

    BufferedReader(ChannelHandlerContext ctx, int maxBufferedChunks, int flushThreshold)
    {
      _maxBufferedChunks = maxBufferedChunks;
      _flushThreshold = flushThreshold;
      _ctx = ctx;
      _notFlushedBytes = 0;
      _notFlushedChunks = 0;
    }

    public void onInit(ReadHandle rh)
    {
      _readHandle = rh;
      _readHandle.request(_maxBufferedChunks);
    }

    public void onDataAvailable(final ByteString data)
    {
      HttpContent content = new DefaultHttpContent(Unpooled.wrappedBuffer(data.asByteBuffer()));
      _ctx.write(content).addListener(new ChannelFutureListener()
      {
        @Override
        public void operationComplete(ChannelFuture future)
            throws Exception
        {
          // this will not be invoked until flush() is called and the data is actually written to socket
          _readHandle.request(1);
        }
      });

      _notFlushedBytes += data.length();
      _notFlushedChunks++;
      if (_notFlushedBytes >= _flushThreshold || _notFlushedChunks == _maxBufferedChunks)
      {
        _ctx.flush();
        _notFlushedBytes = 0;
        _notFlushedChunks = 0;
      }
    }

    public void onDone()
    {
      _ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
    }

    public void onError(Throwable e)
    {
      _ctx.fireExceptionCaught(e);
    }
  }
}
