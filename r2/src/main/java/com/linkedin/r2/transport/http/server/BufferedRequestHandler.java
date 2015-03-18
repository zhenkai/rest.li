package com.linkedin.r2.transport.http.server;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Zhenkai Zhu
 */
/* package private */ class BufferedRequestHandler implements ReadListener, Writer
{
  private final ByteBuffer _buffer;
  private final byte[] _tmpBuf;
  private final ServletInputStream _is;
  private final AbstractR2Servlet.WrappedAsyncContext _ctx;
  private final Object _lock = new Object();
  private WriteHandle _wh;

  BufferedRequestHandler(int bufferSize, ServletInputStream is, AbstractR2Servlet.WrappedAsyncContext ctx)
  {
    _tmpBuf = new byte[4096];
    _buffer = ByteBuffer.allocate(bufferSize);
    _is = is;
    _ctx = ctx;
  }

  public void onDataAvailable() throws IOException
  {
    synchronized (_lock)
    {
      while (!_is.isFinished() && _is.isReady() && _buffer.hasRemaining())
      {
        readIfPossible();
        writeIfPossible();
      }
    }
  }

  public void onAllDataRead() throws IOException
  {
    if (!_ctx.getCompleted().compareAndSet(false, true))
    {
      // the response side has completed, so we can complete ctx
      _ctx.getCtx().complete();
    }
    synchronized (_lock)
    {
      writeIfPossible();
    }
  }

  public void onError(Throwable t)
  {
    _wh.error(t);
  }

  public void onInit(final WriteHandle wh)
  {
    _wh = wh;
  }

  public void onWritePossible()
  {
    synchronized (_lock)
    {
      while (true)
      {
        writeIfPossible();
        try
        {
          boolean newDataRead = readIfPossible();
          if (_wh.remainingCapacity() == 0 || !newDataRead)
          {
            break;
          }
        }
        catch (Exception ex)
        {
          _wh.error(ex);
        }

      }
    }
  }

  private boolean readIfPossible() throws IOException
  {
    boolean read = false;
    while (!_is.isFinished() && _is.isReady() && _buffer.hasRemaining())
    {
      int maxLen = Math.min(_buffer.remaining(), _tmpBuf.length);
      int actualLen = _is.read(_tmpBuf, 0, maxLen);
      _buffer.put(_tmpBuf, 0, actualLen);
      read = true;
    }
    return read;
  }

  private boolean writeIfPossible()
  {
    boolean written = false;
    _buffer.flip();
    if (_is.isFinished() && !_buffer.hasRemaining())
    {
      _wh.done();
    }
    while (_wh.remainingCapacity() > 0 && _buffer.hasRemaining())
    {
      int maxLen = Math.min(_tmpBuf.length, Math.min(_wh.remainingCapacity(), _buffer.remaining()));
      _buffer.get(_tmpBuf, 0, maxLen);
      _wh.write(ByteString.copy(_tmpBuf, 0, maxLen));
      written = true;
    }
    _buffer.compact();
    return written;
  }
}
