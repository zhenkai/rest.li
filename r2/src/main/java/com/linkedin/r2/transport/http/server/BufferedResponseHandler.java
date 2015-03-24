package com.linkedin.r2.transport.http.server;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Zhenkai Zhu
 */
/* package private */class BufferedResponseHandler implements WriteListener, Reader
{
  private final ByteBuffer _buffer;
  private final Object _lock = new Object();
  private final ServletOutputStream _os;
  private final AsyncContext _ctx;
  private ReadHandle _rh;
  private volatile boolean _allDataRead = false;
  private final AtomicBoolean _completed = new AtomicBoolean(false);

  BufferedResponseHandler(int bufferSize, ServletOutputStream os, AsyncContext ctx)
  {
    _buffer = ByteBuffer.allocate(bufferSize);
    _os = os;
    _ctx = ctx;
  }

  // for some reason onWritePossible would be invoked for the first time after doWrite had finished
  // that is isReady returned true and we did writing and finished; isReady never returned false;
  // and then onWritePossible got invoked
  public void onWritePossible() throws IOException
  {
    synchronized (_lock)
    {
      doWrite();
    }
  }

  public void onError(final Throwable t)
  {
    throw new RuntimeException(t);
  }

  public void onDataAvailable(final ByteString data)
  {
    synchronized (_lock)
    {
      byte[] tmpBuf = data.copyBytes();
      _buffer.put(tmpBuf);
      try
      {
        doWrite();
      }
      catch (Exception ex)
      {
        throw new RuntimeException(ex);
      }
    }
  }

  public void onDone()
  {
    synchronized (_lock)
    {
      _allDataRead = true;
      try
      {
        doWrite();
      }
      catch (Exception ex)
      {
        throw new RuntimeException(ex);
      }
    }
  }

  public void onInit(final ReadHandle rh)
  {
    _rh = rh;
    _rh.read(_buffer.capacity());
    _os.setWriteListener(this);
  }

  private void doWrite() throws IOException
  {
    _buffer.flip();
    while(_os.isReady() && _buffer.hasRemaining())
    {
      int bytesNum = _buffer.remaining();
      byte[] tmpBuf = new byte[bytesNum];
      _buffer.get(tmpBuf);
      _os.write(tmpBuf);
      _rh.read(bytesNum);
    }
    if (_allDataRead && _os.isReady() && !_buffer.hasRemaining())
    {
      if (_completed.compareAndSet(false, true))
      {
        _ctx.complete();
      }
    }
    _buffer.compact();
  }

}
