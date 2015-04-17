package com.linkedin.r2.transport.http.client;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.transport.http.common.HttpConstants;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
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

/**
* @author Zhenkai Zhu
*/
/** package private */class RAPRequestEncoder extends ChannelDuplexHandler
{

  private static final int MAX_BUFFER_SIZE = 1024 * 128;
  private BufferedReader _currentReader;

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

    HttpRequest nettyRequest =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, nettyMethod, path);

    nettyRequest.headers().set(HttpHeaders.Names.HOST, url.getAuthority());
    for (Map.Entry<String, String> entry : request.getHeaders().entrySet())
    {
      nettyRequest.headers().set(entry.getKey(), entry.getValue());
    }

    nettyRequest.headers().set(HttpConstants.REQUEST_COOKIE_HEADER_NAME, request.getCookies());

    if (request instanceof RestRequest)
    {
      // this is small optimization for RestRequest so that we don't chunk over the wire because we
      // don't really gain anything for chunking in such case, but slightly increase the transmitting overhead
      final ByteString entity = ((RestRequest) request).getEntity();
      ByteBuf buf = Unpooled.wrappedBuffer(entity.asByteBuffer());
      HttpContent content = new DefaultHttpContent(buf);

      nettyRequest.headers().set(HttpHeaders.Names.CONTENT_LENGTH, entity.length());
      ctx.write(nettyRequest);
      ctx.write(content);
      ctx.write(LastHttpContent.EMPTY_LAST_CONTENT);
    }
    else
    {
      ctx.write(nettyRequest);
      _currentReader = new BufferedReader(ctx, MAX_BUFFER_SIZE);
      request.getEntityStream().setReader(_currentReader);
    }
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception
  {
    if (_currentReader != null)
    {
      _currentReader.flush();
    }
    else
    {
      ctx.flush();
    }
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception
  {
    if (_currentReader != null)
    {
      _currentReader.writeIfPossible();
    }
    ctx.fireChannelWritabilityChanged();
  }


  /**
   * A reader that has pipelining/buffered reading
   *
   * The bufferSize = the permitted size that the writer can still write + the size of data that has been provided
   * by writer but not yet written to socket
   *
   * Buffering is actually done by Netty; we just enforce the upper bound of the buffering
   */
  private static class BufferedReader implements Reader
  {
    final private int _bufferSize;
    final private ChannelHandlerContext _ctx;
    private ReadHandle _readHandle;
    private CompositeByteBuf _buffer = Unpooled.compositeBuffer(1024);


    BufferedReader(ChannelHandlerContext ctx, int bufferSize)
    {
      //_bufferSize = bufferSize;
      _ctx = ctx;
      _bufferSize = ctx.channel().config().getWriteBufferHighWaterMark();
    }

    public void onInit(ReadHandle rh)
    {
      _readHandle = rh;
    }

    public void onDataAvailable(ByteString data)
    {
      // No need to do smaller chunking here as that won't buy us anything
      // jetty buffer size by default is 8k, and it's parsing is smarter enought so that it
      // won't wait to buffer the whole chunk if the chunk is larger than 8k, so the memory consumption
      // won't not be affected by the chunk size here.
      ByteBuf channelBuffer = Unpooled.wrappedBuffer(data.asByteBuffer());
      _buffer.addComponent(channelBuffer);
      _buffer.writerIndex(_buffer.writerIndex() + channelBuffer.readableBytes());

      writeIfPossible();
    }

    public void onDone()
    {
      _ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
    }

    public void onError(Throwable e)
    {
      _ctx.fireExceptionCaught(e);
    }

    public void flush()
    {
      // signal the Writer that we can accept _bufferSize bytes
      _readHandle.read(_bufferSize);
    }

    public void writeIfPossible()
    {
      if (_ctx.channel().isWritable())
      {
        HttpContent content = new DefaultHttpContent(_buffer);
        _ctx.writeAndFlush(content);
        _buffer = Unpooled.compositeBuffer(1024);
        _readHandle.read(_bufferSize);
      }
    }
  }
}
