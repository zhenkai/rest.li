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
/* package private */class AsyncIOResponseHandler implements WriteListener, Reader
{
  private final ByteBuffer _buffer;
  private final Object _lock = new Object();
  private final ServletOutputStream _os;
  private final AsyncContext _ctx;
  private ReadHandle _rh;
  private volatile boolean _allDataRead = false;
  private final AtomicBoolean _completed = new AtomicBoolean(false);
  private final AtomicBoolean _otherDirectionFinished;

  // for debug
  private int _count = 0;

  AsyncIOResponseHandler(int bufferSize, ServletOutputStream os, AsyncContext ctx, AtomicBoolean otherDirectionFinished)
  {
    _buffer = ByteBuffer.allocate(bufferSize);
    _os = os;
    _ctx = ctx;
    _otherDirectionFinished = otherDirectionFinished;
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
    throw new RuntimeException("Bytes written so far: " + (_count / 1024 / 1024) + " MB", t);
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
    byte[] tmpBuf = new byte[4096];
    int totalWritten = 0;
    while(_os.isReady() && _buffer.hasRemaining())
    {
      int bytesNum = Math.min(tmpBuf.length, _buffer.remaining());
      _buffer.get(tmpBuf, 0, bytesNum);
      _os.write(tmpBuf, 0, bytesNum);
      totalWritten += bytesNum;
    }
    if (_allDataRead && _os.isReady() && !_buffer.hasRemaining())
    {
      if (_completed.compareAndSet(false, true))
      {
        if (!_otherDirectionFinished.compareAndSet(false, true))
        {
          // other direction finished
          _ctx.complete();
        }
      }
    }
    _buffer.compact();
    _count += totalWritten;
    _rh.read(totalWritten);

  }

}
