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

  private BufferedReader _currentReader;
  private static final int MAX_BUFFERED_CHUNKS = 10;

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
    _currentReader = new BufferedReader(ctx, MAX_BUFFERED_CHUNKS);
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
    private final int _bufferSize;
    private final ChannelHandlerContext _ctx;
    private volatile ReadHandle _readHandle;
    private final Queue<ByteString> _buffers;


    BufferedReader(ChannelHandlerContext ctx, int bufferSize)
    {
      _bufferSize = bufferSize;
      _ctx = ctx;
      _buffers = new LinkedDeque<ByteString>();
    }

    public void onInit(ReadHandle rh)
    {
      _readHandle = rh;
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
      _readHandle = null;
      _ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
    }

    public void onError(Throwable e)
    {
      _readHandle = null;
      _ctx.fireExceptionCaught(e);
    }

    //!!! following methods must be called within _ctx#executor()
    private void flush()
    {
      _readHandle.request(_bufferSize);
    }

    private void appendData(ByteString data)
    {
      _buffers.add(data);
    }

    private void writeIfPossible()
    {
      while (_buffers.size() > 0)
      {
        ByteString buf = _buffers.poll();
        HttpContent content = new DefaultHttpContent(Unpooled.wrappedBuffer(buf.asByteBuffer()));
        _ctx.writeAndFlush(content).addListener(new ChannelFutureListener()
        {
          @Override
          public void operationComplete(ChannelFuture future)
              throws Exception
          {
            if (_readHandle != null)
            {
              _readHandle.request(1);
            }
          }
        });
      }
    }
  }
}
