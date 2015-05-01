package com.linkedin.r2.transport.http.client;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.transport.http.common.HttpConstants;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
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

  private BufferedReader _currentReader;
  private static final int MAX_BUFFERED_CHUNKS = 3;

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
    _currentReader = new BufferedReader(ctx, MAX_BUFFER_SIZE);
    request.getEntityStream().setReader(_currentReader);
  }

  @Override
  public void flush(ChannelHandlerContext ctx)
      throws Exception
  {
    if (_currentReader == null)
    {
      throw new IllegalStateException("_currentReader is null");
    }
    _currentReader.flush();
  }

//  @Override
//  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception
//  {
//    if (_currentReader != null)
//    {
//      _currentReader.writeIfPossible();
//    }
//    ctx.fireChannelWritabilityChanged();
//  }



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
      _bufferSize = bufferSize;
      _ctx = ctx;
    }

    public void onInit(ReadHandle rh)
    {
      _readHandle = rh;
      // signal the Writer that we can accept _bufferSize chunks
      _readHandle.request(_bufferSize);
    }

    public void onDataAvailable(final ByteString data)
    {
      if (_ctx.executor().inEventLoop())
      {
        appendData(data);
        writeIfPossible();
      }
      else
      {
        _ctx.executor().execute(new Runnable()
        {
          @Override
          public void run()
          {
            appendData(data);
            writeIfPossible();
          }
        });
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

    //!!! following methods must be called within _ctx#executor()
    private void flush()
    {
      _readHandle.read(_bufferSize);
    }

    private void appendData(ByteString data)
    {
      ByteBuf channelBuffer = Unpooled.wrappedBuffer(data.asByteBuffer());
      _buffer.addComponent(channelBuffer);
      _buffer.writerIndex(_buffer.writerIndex() + channelBuffer.readableBytes());
    }

    private void writeIfPossible()
    {
      while (_buffer.readableBytes() > 0 && _ctx.channel().isWritable())
      {
        final int bytesWritten = _buffer.readableBytes();
        HttpContent content = new DefaultHttpContent(_buffer);
        _ctx.writeAndFlush(content).addListener(new ChannelFutureListener()
        {
          @Override
          public void operationComplete(ChannelFuture future)
              throws Exception
          {
            // data have been written out, we can tell the writer that we can accept one more data chunk
            _readHandle.request(1);
          }
        });
        _buffer = Unpooled.compositeBuffer(1024);
      }
    }
  }
}
