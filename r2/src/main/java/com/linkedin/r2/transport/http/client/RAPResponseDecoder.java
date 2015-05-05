package com.linkedin.r2.transport.http.client;


import com.linkedin.data.ByteString;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.message.rest.StreamResponseBuilder;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.r2.transport.http.common.HttpConstants;
import com.linkedin.r2.util.Timeout;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;
import static io.netty.handler.codec.http.HttpHeaders.isTransferEncodingChunked;
import static io.netty.handler.codec.http.HttpHeaders.removeTransferEncodingChunked;


/* package private */ class RAPResponseDecoder extends SimpleChannelInboundHandler<HttpObject>
{
  private static final FullHttpResponse CONTINUE =
      new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE, Unpooled.EMPTY_BUFFER);

  private static final int BUFFER_HIGH_WATER_MARK = 16 * 1024;
  private static final int BUFFER_LOW_WATER_MARK = 8 * 1024;

  private final long _maxContentLength;
  private final long _requestTimeout;

  private TimeoutBufferedWriter _chunkedMessageWriter;

  RAPResponseDecoder(long requestTimeout, long maxContentLength)
  {
    _requestTimeout = requestTimeout;
    _maxContentLength = maxContentLength;
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, HttpObject msg) throws Exception
  {
    if (msg instanceof HttpResponse)
    {
      HttpResponse m = (HttpResponse) msg;
      if (is100ContinueExpected(m))
      {
        ctx.writeAndFlush(CONTINUE).addListener(new ChannelFutureListener()
        {
          @Override
          public void operationComplete(ChannelFuture future)
              throws Exception
          {
            if (!future.isSuccess())
            {
              ctx.fireExceptionCaught(future.cause());
            }
          }
        });
      }
      if (!m.getDecoderResult().isSuccess())
      {
        ctx.fireExceptionCaught(m.getDecoderResult().cause());
        return;
      }
      // remove chunked encoding.
      if (isTransferEncodingChunked(m))
      {
        removeTransferEncodingChunked(m);
      }
      final TimeoutBufferedWriter writer = new TimeoutBufferedWriter(ctx, _maxContentLength,
          BUFFER_HIGH_WATER_MARK, BUFFER_LOW_WATER_MARK, _requestTimeout);
      EntityStream entityStream = EntityStreams.newEntityStream(writer);
      _chunkedMessageWriter = writer;
      StreamResponseBuilder builder = new StreamResponseBuilder();
      builder.setStatus(m.getStatus().code());

      for (Map.Entry<String, String> e : m.headers())
      {
        if (e.getKey().equalsIgnoreCase(HttpConstants.RESPONSE_COOKIE_HEADER_NAME))
        {
          builder.addCookie(e.getValue());
        }
        else
        {
          builder.unsafeAddHeaderValue(e.getKey(), e.getValue());
        }
      }

      ctx.fireChannelRead(builder.build(entityStream));
    }
    else if (msg instanceof HttpContent)
    {
      HttpContent chunk = (HttpContent) msg;
      TimeoutBufferedWriter currentWriter = _chunkedMessageWriter;
      // Sanity check
      if (currentWriter == null)
      {
        throw new IllegalStateException(
            "received " + HttpContent.class.getSimpleName() +
                " without " + HttpResponse.class.getSimpleName());
      }

      final boolean last;
      if (!chunk.getDecoderResult().isSuccess())
      {
        last = true;
      }
      else
      {
        last = chunk instanceof LastHttpContent;
      }
      if (last)
      {
        _chunkedMessageWriter = null;
        ctx.fireChannelRead(ChannelPoolHandler.CHANNEL_RELEASE_SIGNAL);
      }

      currentWriter.processHttpChunk(chunk);
    }
    else
    {
      // something must be wrong, but let's proceed so that
      // handler after us has a chance to process it.
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception
  {
    if (_chunkedMessageWriter != null)
    {
      _chunkedMessageWriter.fail(new ClosedChannelException());
      _chunkedMessageWriter = null;
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
  {
    if (_chunkedMessageWriter != null)
    {
      _chunkedMessageWriter.fail(cause);
      _chunkedMessageWriter = null;
    }
    ctx.fireExceptionCaught(cause);
  }

  /**
   * A buffered writer that stops reading from socket if buffered bytes is larger than high water mark
   * and resumes reading from socket if buffered bytes is smaller than low water mark.
   */
  private class TimeoutBufferedWriter implements Writer
  {
    private final ChannelHandlerContext _ctx;
    private final long _maxContentLength;
    private final int _highWaterMark;
    private final int _lowWaterMark;
    private WriteHandle _wh;
    private Timeout<Runnable> _timeout;
    private volatile boolean _lastChunkReceived = false;
    private int _totalBytesWritten = 0;
    private int _bufferedBytes = 0;
    private final List<ByteString> _buffer = new LinkedList<ByteString>();
    private final long _requestTimeout;

    TimeoutBufferedWriter(final ChannelHandlerContext ctx, long maxContentLength,
                          int highWaterMark, int lowWaterMark,
                          final long requestTimeout)
    {
      _ctx = ctx;
      _maxContentLength = maxContentLength;
      _highWaterMark = highWaterMark;
      _lowWaterMark = lowWaterMark;

      // schedule a timeout to close the channel and inform use
      Runnable timeoutTask = new Runnable()
      {
        @Override
        public void run()
        {
          Exception ex = new TimeoutException("Not receiving any chunk after timeout of " + requestTimeout + "ms");
          ctx.fireExceptionCaught(ex);
          fail(ex);

          _chunkedMessageWriter = null;
        }
      };
      _timeout = new Timeout<Runnable>(ctx.executor(), requestTimeout, TimeUnit.MILLISECONDS, timeoutTask);
      _timeout.addTimeoutTask(timeoutTask);
      _requestTimeout = requestTimeout;
    }

    @Override
    public void onInit(WriteHandle wh)
    {
      _wh = wh;
    }

    @Override
    public void onWritePossible()
    {
      if (_ctx.executor().inEventLoop())
      {
        doWrite();
      }
      else
      {
        _ctx.executor().execute(new Runnable()
        {
          @Override
          public void run()
          {
            doWrite();
          }
        });
      }
    }

    public void processHttpChunk(HttpContent chunk) throws TooLongFrameException
    {
      Runnable timeoutTask = _timeout.getItem();

      if (!chunk.getDecoderResult().isSuccess())
      {
        fail(chunk.getDecoderResult().cause());
        _chunkedMessageWriter = null;
      }
      else if (chunk.content().readableBytes() + _totalBytesWritten > _maxContentLength)
      {
        TooLongFrameException ex = new TooLongFrameException("HTTP content length exceeded " + _maxContentLength +
            " bytes.");
        fail(ex);
        _chunkedMessageWriter = null;
        throw ex;
      }
      else
      {
        if (chunk.content().isReadable())
        {
          ByteBuf rawData = chunk.content();
          InputStream is = new ByteBufInputStream(rawData);
          final ByteString data;
          try
          {
            data = ByteString.read(is, rawData.readableBytes());
          }
          catch (IOException ex)
          {
            fail(ex);
            return;
          }
          _buffer.add(data);
          _bufferedBytes += data.length();
          if (_bufferedBytes > _highWaterMark && _ctx.channel().config().isAutoRead())
          {
            // stop reading from socket because we buffered too much
            _ctx.channel().config().setAutoRead(false);
          }
        }
        if (chunk instanceof LastHttpContent)
        {
          _lastChunkReceived = true;
        }
        if (_wh != null)
        {
          doWrite();
        }
      }
      if (!_lastChunkReceived)
      {
        _timeout = new Timeout<Runnable>(_ctx.executor(), _requestTimeout, TimeUnit.MILLISECONDS, timeoutTask);
        _timeout.addTimeoutTask(timeoutTask);
      }
    }

    public void fail(Throwable ex)
    {
      _timeout.getItem();
      if (_wh != null)
      {
        _wh.error(new RemoteInvocationException(ex));
      }
    }

    private void doWrite()
    {
      while(_wh.remaining() > 0)
      {
        if (!_buffer.isEmpty())
        {
          ByteString data = _buffer.remove(0);
          _wh.write(data);
          _bufferedBytes -= data.length();
          _totalBytesWritten += data.length();
          if (!_ctx.channel().config().isAutoRead() && _bufferedBytes < _lowWaterMark)
          {
            // resume reading from socket
            _ctx.channel().config().setAutoRead(true);
          }
        }
        else
        {
          if (_lastChunkReceived)
          {
            _wh.done();
          }
          break;
        }
      }
    }
  }
}
