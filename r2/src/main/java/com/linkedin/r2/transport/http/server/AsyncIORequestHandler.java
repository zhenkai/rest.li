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
/* package private */ class AsyncIORequestHandler implements ReadListener, Writer
{
  private final ServletInputStream _is;
  private final Object _lock = new Object();
  private WriteHandle _wh;

  // for debug
  private int _count = 0;

  AsyncIORequestHandler(ServletInputStream is)
  {
    _is = is;
  }

  public void onDataAvailable() throws IOException
  {
    synchronized (_lock)
    {
      writeIfPossible();
    }
  }

  public void onAllDataRead() throws IOException
  {
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
    _is.setReadListener(this);
  }

  public void onWritePossible()
  {
    synchronized (_lock)
    {
      writeIfPossible();
    }
  }

  private void writeIfPossible()
  {
    byte[] buf = new byte[4096];
    try
    {
      while (_is.isReady() && _wh.remainingCapacity() > 0)
      {
        int maxLen = Math.min(buf.length, _wh.remainingCapacity());
        int bytesRead = _is.read(buf, 0, maxLen);
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
