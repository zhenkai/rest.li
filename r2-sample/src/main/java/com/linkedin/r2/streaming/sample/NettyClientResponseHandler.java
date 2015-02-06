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

      _writer = new BufferedWriter(buf, ctx, 512 * 1024, 128 * 1024);
      EntityStream entityStream = EntityStreams.newEntityStream(_writer);
      if (!m.isChunked())
      {
        _writer.setLastChunkRead();
      }

      Channels.fireMessageReceived(ctx, builder.build(entityStream), e.getRemoteAddress());
    }
    else if (msg instanceof HttpChunk)
    {
      HttpChunk chunk = (HttpChunk) msg;
      _writer.processHttpChunk(chunk);
    }
    else {
      // Neither HttpMessage or HttpChunk
      ctx.sendUpstream(e);
    }
  }

  private static class BufferedWriter implements Writer
  {
    final private ChannelBuffer _buffer;
    final private ChannelHandlerContext _ctx;
    final private int _highWaterMark;
    final private int _lowWaterMark;
    final private Object _lock;
    private WriteHandle _wh;
    private int _chunkSize;
    private boolean _lastChunkRead = false;
    private boolean _isDone = false;
    private int _allowedChunks = 0;
    final private byte[] bytes = new byte[_chunkSize];

    BufferedWriter(ChannelBuffer buffer, ChannelHandlerContext ctx, int highWaterMark, int lowWaterMark)
    {
      _buffer = buffer;
      _ctx = ctx;
      _highWaterMark = highWaterMark;
      _lowWaterMark = lowWaterMark;
      _lock = new Object();
    }

    @Override
    public void onInit(WriteHandle wh, int chunkSize)
    {
      _wh = wh;
      _chunkSize = chunkSize;
    }

    @Override
    public void onWritePossible(int chunkNum)
    {
      synchronized (_lock)
      {
        _allowedChunks += chunkNum;
        doWrite();
      }
    }

    public void setLastChunkRead()
    {
      _lastChunkRead = true;
    }

    public void processHttpChunk(HttpChunk httpChunk)
    {
      synchronized (_lock)
      {
        _buffer.writeBytes(httpChunk.getContent());
        if (httpChunk.isLast())
        {
          _lastChunkRead = true;
        }

        if (_buffer.readableBytes() > _highWaterMark && _ctx.getChannel().isReadable())
        {
          // stop reading from socket because we buffered too much
          _ctx.getChannel().setReadable(false);
        }

        doWrite();
      }
    }

    private void doWrite()
    {
      while (!_isDone && _allowedChunks > 0)
      {
        int dataLen = Math.min(_chunkSize, _buffer.readableBytes());
        if (dataLen == 0)
        {
          if (_lastChunkRead)
          {
            _wh.done();
            _isDone = true;
          }
          break;
        }
        else
        {
          _buffer.readBytes(bytes, 0, dataLen);
          _wh.write(ByteString.copy(bytes, 0, dataLen));
          _allowedChunks--;
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

