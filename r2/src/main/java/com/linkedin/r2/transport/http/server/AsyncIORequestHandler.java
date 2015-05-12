package com.linkedin.r2.transport.http.server;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;

import javax.servlet.AsyncContext;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Zhenkai Zhu
 */
/* package private */ class AsyncIORequestHandler implements ReadListener, Writer
{
  private final ServletInputStream _is;
  private final AsyncContext _ctx;
  private final AtomicBoolean _otherDirectionFinished;
  private final Object _lock = new Object();
  private WriteHandle _wh;

  // for debug
  private int _count = 0;

  AsyncIORequestHandler(ServletInputStream is, AsyncContext ctx, AtomicBoolean otherDirectionFinished)
  {
    _is = is;
    _ctx = ctx;
    _otherDirectionFinished = otherDirectionFinished;
  }

  @Override
  public void onDataAvailable() throws IOException
  {
    synchronized (_lock)
    {
      if (_wh != null)
      {
        writeIfPossible();
      }
    }
  }

  @Override
  public void onAllDataRead() throws IOException
  {
    synchronized (_lock)
    {
      if (_wh != null)
      {
        writeIfPossible();
      }
    }
    if (!_otherDirectionFinished.compareAndSet(false, true))
    {
      // the other direction finished, we can complete
      _ctx.complete();
    }
  }

  @Override
  public void onError(Throwable t)
  {
    _wh.error(t);
  }

  @Override
  public void onInit(final WriteHandle wh)
  {
    synchronized (_lock)
    {
      _wh = wh;
    }
    //_is.setReadListener(this);
  }

  @Override
  public void onWritePossible()
  {
    synchronized (_lock)
    {
      writeIfPossible();
    }
  }

  @Override
  public void onAbort(Throwable ex)
  {
    // TODO [ZZ]: do something smarter?
    throw new IllegalStateException("Exception thrown by request processing code", ex);
  }

  private void writeIfPossible()
  {
    byte[] buf = new byte[4096];
    try
    {
      while (_is.isReady() && _wh.remaining() > 0)
      {
        int bytesRead = _is.read(buf);
        _count += bytesRead;
        if (bytesRead < 0)
        {
          _wh.done();
          break;
        }
        else
        {
          _wh.write(ByteString.copy(buf, 0, bytesRead));
        }
      }
    }
    catch (IOException ex)
    {
      _wh.error(new Exception("Bytes read: " + _count, ex));
    }
  }
}
