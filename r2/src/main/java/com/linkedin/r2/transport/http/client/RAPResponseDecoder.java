package com.linkedin.r2.transport.http.client;

import com.linkedin.data.ByteString;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.message.rest.StreamResponseBuilder;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.r2.transport.http.common.HttpConstants;
import com.linkedin.r2.util.Timeout;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMessage;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.jboss.netty.channel.Channels.succeededFuture;
import static org.jboss.netty.channel.Channels.write;
import static org.jboss.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;

/**
* @author Zhenkai Zhu
*/
/* package private */ class RAPResponseDecoder extends SimpleChannelUpstreamHandler
{
  private static final ChannelBuffer CONTINUE = ChannelBuffers.copiedBuffer(
      "HTTP/1.1 100 Continue\r\n\r\n", CharsetUtil.US_ASCII);

  private static final int BUFFER_HIGH_WATER_MARK = 16 * 1024;
  private static final int BUFFER_LOW_WATER_MARK = 8 * 1024;

  private final long _maxContentLength;
  private final int _requestTimeout;
  private final ScheduledExecutorService _scheduler;

  private TimeoutBufferedWriter _chunkedMessageWriter;

  RAPResponseDecoder(ScheduledExecutorService scheduler, int requestTimeout, long maxContentLength)
  {
    _scheduler = scheduler;
    _requestTimeout = requestTimeout;
    _maxContentLength = maxContentLength;
  }

  public void messageReceived(
      ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {

      Object msg = e.getMessage();

      if (msg instanceof HttpResponse)
      {
        HttpResponse m = (HttpResponse) msg;

        if (is100ContinueExpected(m))
        {
          write(ctx, succeededFuture(ctx.getChannel()), CONTINUE.duplicate());
        }

        final boolean isChunked = m.isChunked();
        EntityStream entityStream;
        if (isChunked)
        {
          // A chunked message - remove 'Transfer-Encoding' header,
          // initialize the cumulative buffer, and wait for incoming chunks.
          List<String> encodings = m.getHeaders(HttpHeaders.Names.TRANSFER_ENCODING);
          encodings.remove(HttpHeaders.Values.CHUNKED);
          if (encodings.isEmpty())
          {
            m.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
          }
          ChannelBuffer buf = ChannelBuffers.dynamicBuffer(e.getChannel().getConfig().getBufferFactory());
          final TimeoutBufferedWriter writer = new TimeoutBufferedWriter(buf, ctx, _maxContentLength,
              BUFFER_HIGH_WATER_MARK, BUFFER_LOW_WATER_MARK, _scheduler, _requestTimeout);
          entityStream = EntityStreams.newEntityStream(writer);
          _chunkedMessageWriter = writer;
        }
        else
        {
          // this is not chunked and full entity is already available and in memory
          ChannelBuffer buf = m.getContent();
          byte[] array = new byte[buf.readableBytes()];
          buf.readBytes(array);
          ByteStringWriter writer = new ByteStringWriter(ByteString.copy(array));
          entityStream = EntityStreams.newEntityStream(writer);
        }

        StreamResponseBuilder builder = new StreamResponseBuilder();
        HttpResponseStatus status = m.getStatus();
        builder.setStatus(status.getCode());

        for (Map.Entry<String, String> entry : m.getHeaders())
        {
          if (entry.getKey().equalsIgnoreCase(HttpConstants.RESPONSE_COOKIE_HEADER_NAME))
          {
            builder.addCookie(entry.getValue());
          } else
          {
            builder.unsafeAddHeaderValue(entry.getKey(), entry.getValue());
          }
        }

        Channels.fireMessageReceived(ctx,
            builder.build(entityStream),
             e.getRemoteAddress());

        if (!isChunked)
        {
          Channels.fireMessageReceived(ctx, HttpNettyClient.CHANNEL_RELEASE_SIGNAL, e.getRemoteAddress());
        }
      }
      else if (msg instanceof HttpChunk)
      {
        TimeoutBufferedWriter currentWriter = _chunkedMessageWriter;
        // Sanity check
        if (currentWriter == null)
        {
          throw new IllegalStateException(
              "received " + HttpChunk.class.getSimpleName() +
                  " without " + HttpMessage.class.getSimpleName());
        }

        HttpChunk chunk = (HttpChunk) msg;
        if (chunk.isLast())
        {
          // TODO [ZZ]: what to do with HttpChunkTrailer? We don't support it as we've already fired up StreamResponse
          _chunkedMessageWriter = null;
          Channels.fireMessageReceived(ctx, HttpNettyClient.CHANNEL_RELEASE_SIGNAL, e.getRemoteAddress());
        }

        currentWriter.processHttpChunk(chunk);
      }
      else
      {
        ctx.sendUpstream(e);
      }
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    if (_chunkedMessageWriter != null)
    {
      TimeoutBufferedWriter writer = _chunkedMessageWriter;
      _chunkedMessageWriter = null;
      writer.fail(new IllegalStateException("Channel closed while receiving the entity."));
    }
    super.channelClosed(ctx, e);
  }

  @Override
  public void exceptionCaught(
      ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
  {
    if (_chunkedMessageWriter != null)
    {
      TimeoutBufferedWriter writer = _chunkedMessageWriter;
      _chunkedMessageWriter = null;
      writer.fail(new IllegalStateException("Exception caught while receiving the entity.", e.getCause()));
    }
    super.exceptionCaught(ctx, e);
  }

  /**
   * A buffered writer that stops reading from socket if buffered bytes is larger than high water mark
   * and resumes reading from socket if buffered bytes is smaller than low water mark.
   */
  private class TimeoutBufferedWriter implements Writer
  {
    private final ChannelBuffer _buffer;
    private final ChannelHandlerContext _ctx;
    private final long _maxContentLength;
    private final int _highWaterMark;
    private final int _lowWaterMark;
    private final Object _lock;
    private WriteHandle _wh;
    private Timeout<Runnable> _timeout;
    private volatile boolean _lastChunkReceived = false;
    private int _totalBytesWritten = 0;
    private final byte[] _bytes;
    private final ScheduledExecutorService _scheduler;
    private final int _requestTimeout;

    TimeoutBufferedWriter(ChannelBuffer buffer, final ChannelHandlerContext ctx, long maxContentLength,
                          int highWaterMark, int lowWaterMark, ScheduledExecutorService scheduler,
                          final int requestTimeout)
    {
      _buffer = buffer;
      _ctx = ctx;
      _maxContentLength = maxContentLength;
      _highWaterMark = highWaterMark;
      _lowWaterMark = lowWaterMark;
      _lock = new Object();
      _bytes = new byte[4096];

      // schedule a timeout to close the channel and inform use
      Runnable timeoutTask = new Runnable()
      {
        @Override
        public void run()
        {
          Exception ex = new TimeoutException("Not receiving any chunk after timeout of " + requestTimeout + "ms");
          fail(ex);
          _chunkedMessageWriter = null;
          Channels.fireExceptionCaught(ctx, ex);
        }
      };
      _timeout = new Timeout<Runnable>(scheduler, requestTimeout, TimeUnit.MILLISECONDS, timeoutTask);
      _timeout.addTimeoutTask(timeoutTask);
      _scheduler = scheduler;
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
      // we need synchronized because doWrite may be invoked by EntityStream's event thread or
      // netty thread
      synchronized (_lock)
      {
        doWrite();
      }
    }

    public void processHttpChunk(HttpChunk httpChunk) throws TooLongFrameException
    {
      Runnable timeoutTask = _timeout.getItem();

      // we need synchronized because doWrite may be invoked by EntityStream's event thread or
      // netty thread
      synchronized (_lock)
      {
        _buffer.writeBytes(httpChunk.getContent());

        if (_buffer.readableBytes() + _totalBytesWritten > _maxContentLength)
        {
          TooLongFrameException ex = new TooLongFrameException(
              "HTTP content length exceeded " + _maxContentLength +
                  " bytes.");

          fail(ex);
          _chunkedMessageWriter = null;
          Channels.fireExceptionCaught(_ctx, ex);
        }

        if (_buffer.readableBytes() > _highWaterMark && _ctx.getChannel().isReadable())
        {
          // stop reading from socket because we buffered too much
          _ctx.getChannel().setReadable(false);
        }

        if (httpChunk.isLast())
        {
          _lastChunkReceived = true;
        }

        if (_wh != null)
        {
          doWrite();
        }
      }

      if (!httpChunk.isLast())
      {
        _timeout = new Timeout<Runnable>(_scheduler, _requestTimeout, TimeUnit.MILLISECONDS, timeoutTask);
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

    // this method does not block
    private void doWrite()
    {
      while (_wh.remaining() > 0)
      {
        int dataLen = Math.min(_wh.remaining(), Math.min(_bytes.length, _buffer.readableBytes()));
        if (dataLen == 0)
        {
          if (_lastChunkReceived)
          {
            _wh.done();
          }
          break;
        }
        else
        {
          _buffer.readBytes(_bytes, 0, dataLen);
          _wh.write(ByteString.copy(_bytes, 0, dataLen));
          _totalBytesWritten += dataLen;
          if (!_ctx.getChannel().isReadable() && _buffer.readableBytes() < _lowWaterMark)
          {
            // resume reading from socket
            _ctx.getChannel().setReadable(true);
          }
        }
      }
      _buffer.discardReadBytes();
    }
  }
}
