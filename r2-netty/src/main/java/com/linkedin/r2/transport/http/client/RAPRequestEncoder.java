package com.linkedin.r2.transport.http.client;

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.entitystream.ReadHandle;
import com.linkedin.r2.message.entitystream.Reader;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;

/**
 * This encoder encodes StreamRequest to Netty's HttpRequest.
 *
 * @author Zhenkai Zhu
 */
/** package private */class RAPRequestEncoder extends ChannelDuplexHandler
{
  private static final int MAX_BUFFERED_CHUNKS = 10;
  // this threshold is to mitigate the effect of the inter-play of Nagle's algorithm & Delayed ACK
  // when sending requests with small entity
  private static final int FLUSH_THRESHOLD = R2Constants.DEFAULT_DATA_CHUNK_SIZE;
  private volatile BufferedReader _currentReader;

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
  {
    if (msg instanceof StreamRequest)
    {
      StreamRequest request = (StreamRequest) msg;
      HttpRequest nettyRequest = NettyRequestAdapter.toNettyRequest(request);
      ctx.write(nettyRequest, promise);
      _currentReader = new BufferedReader(ctx, MAX_BUFFERED_CHUNKS, FLUSH_THRESHOLD);
      request.getEntityStream().setReader(_currentReader);
    }
    else
    {
      _currentReader = null;
      ctx.write(msg, promise);
    }
  }

  @Override
  public void flush(ChannelHandlerContext ctx)
      throws Exception
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

  /**
   * A reader that has pipelining/buffered reading
   *
   * Buffering is actually done by Netty; we just enforce the upper bound of the buffering
   */
  private class BufferedReader implements Reader
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
      _currentReader = null;
      _ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
    }

    public void onError(Throwable e)
    {
      _currentReader = null;
      _ctx.fireExceptionCaught(e);
    }

    private void flush()
    {
      _readHandle.request(_maxBufferedChunks);
    }
  }
}
