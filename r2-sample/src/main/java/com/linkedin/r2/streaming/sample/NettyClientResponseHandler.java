package com.linkedin.r2.streaming.sample;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMessage;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class handles the streamed response; it's adapted from the HttpChunkAggregator.
 *
 * @author Zhenkai Zhu
 */
public class NettyClientResponseHandler extends SimpleChannelUpstreamHandler
{
  private BufferedWriter _writer;

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
      throws Exception
  {

    Object msg = e.getMessage();

    if (msg instanceof HttpMessage)
    {
      HttpMessage m = (HttpMessage) msg;

      HttpResponse nettyResponse = (HttpResponse) msg;
      RestResponseBuilder builder = new RestResponseBuilder();
      HttpResponseStatus status = nettyResponse.getStatus();
      builder.setStatus(status.getCode());

      for (Map.Entry<String, String> header : nettyResponse.getHeaders())
      {
        builder.unsafeAddHeaderValue(header.getKey(), header.getValue());
      }

      ChannelBuffer buf = m.isChunked() ? ChannelBuffers.dynamicBuffer(e.getChannel().getConfig().getBufferFactory())
                                        : m.getContent();

      BufferedWriter writer = new BufferedWriter(buf, ctx, 512 * 1024, 128 * 1024);
      EntityStream entityStream = EntityStreams.newEntityStream(writer);
      if (!m.isChunked())
      {
        // done for this response
        writer.setLastChunkReceived();
      }
      else
      {
        // keep a reference to the writer of the unfinished stream
        _writer = writer;
      }

      Channels.fireMessageReceived(ctx, builder.build(entityStream), e.getRemoteAddress());
    }
    else if (msg instanceof HttpChunk)
    {
      BufferedWriter currentWriter = _writer;

      // Sanity check
      if (currentWriter == null) {
        throw new IllegalStateException(
            "received " + HttpChunk.class.getSimpleName() +
                " without " + HttpMessage.class.getSimpleName());
      }
      HttpChunk chunk = (HttpChunk) msg;
      if (chunk.isLast())
      {
        currentWriter.setLastChunkReceived();
        // no longer need to reference the writer of finished stream
        _writer = null;
      }
      currentWriter.processHttpChunk(chunk);
    }
    else {
      // Neither HttpMessage or HttpChunk
      ctx.sendUpstream(e);
    }
  }

  /**
   * A buffered writer that stops reading from socket if buffered bytes is larger than high water mark
   * and resumes reading from socket if buffered bytes is smaller than low water mark.
   *
   */
  private static class BufferedWriter implements Writer
  {
    final private ChannelBuffer _buffer;
    final private ChannelHandlerContext _ctx;
    final private int _highWaterMark;
    final private int _lowWaterMark;
    final private Object _lock;
    private WriteHandle _wh;
    private volatile boolean _lastChunkReceived = false;
    private boolean _isDone = false;
    final private byte[] _bytes;

    BufferedWriter(ChannelBuffer buffer, ChannelHandlerContext ctx, int highWaterMark, int lowWaterMark)
    {
      _buffer = buffer;
      _ctx = ctx;
      _highWaterMark = highWaterMark;
      _lowWaterMark = lowWaterMark;
      _lock = new Object();
      _bytes = new byte[4096];
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

    public void setLastChunkReceived()
    {
      _lastChunkReceived = true;
    }

    public void processHttpChunk(HttpChunk httpChunk)
    {
      // we need synchronized because doWrite may be invoked by EntityStream's event thread or
      // netty thread
      synchronized (_lock)
      {
        _buffer.writeBytes(httpChunk.getContent());

        if (_buffer.readableBytes() > _highWaterMark && _ctx.getChannel().isReadable())
        {
          // stop reading from socket because we buffered too much
          _ctx.getChannel().setReadable(false);
        }

        doWrite();
      }
    }

    // this method does not block
    private void doWrite()
    {
      while (!_isDone && _wh.remainingCapacity() > 0)
      {
        int dataLen = Math.min(_wh.remainingCapacity(), Math.min(_bytes.length, _buffer.readableBytes()));
        if (dataLen == 0)
        {
          if (_lastChunkReceived)
          {
            _wh.done();
            _isDone = true;
          }
          break;
        }
        else
        {
          _buffer.readBytes(_bytes, 0, dataLen);
          _wh.write(ByteString.copy(_bytes, 0, dataLen));
          if (!_ctx.getChannel().isReadable() && _buffer.readableBytes() < _lowWaterMark)
          {
            // resume reading from socket
            _ctx.getChannel().setReadable(true);
          }
        }
      }
    }
  }
}

