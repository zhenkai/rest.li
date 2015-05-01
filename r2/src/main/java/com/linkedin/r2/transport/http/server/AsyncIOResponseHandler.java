package com.linkedin.r2.transport.http.server;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Zhenkai Zhu
 */
/* package private */class AsyncIOResponseHandler implements WriteListener, Reader
{
  private final Object _lock = new Object();
  private final ServletOutputStream _os;
  private final AsyncContext _ctx;
  private ReadHandle _rh;
  private volatile boolean _allDataRead = false;
  private final AtomicBoolean _writeCouldStart = new AtomicBoolean(false);
  private final AtomicBoolean _completed = new AtomicBoolean(false);
  private final AtomicBoolean _otherDirectionFinished;
  private final int _maxBufferedChunks;
  private final List<ByteString> _buffer = new LinkedList<ByteString>();

  // for debug
  private int _count = 0;

  AsyncIOResponseHandler(int maxBufferedChunks, ServletOutputStream os, AsyncContext ctx, AtomicBoolean otherDirectionFinished)
  {
    _os = os;
    _ctx = ctx;
    _otherDirectionFinished = otherDirectionFinished;
    _maxBufferedChunks = maxBufferedChunks;
  }

  public void onWritePossible() throws IOException
  {
    if (_writeCouldStart.compareAndSet(false, true))
    {
      _rh.request(_maxBufferedChunks);
    }
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
      _buffer.add(data);
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
    _os.setWriteListener(this);
  }

  private void doWrite() throws IOException
  {
    int totalWritten = 0;
    while(_os.isReady() && !_buffer.isEmpty())
    {
      ByteString data = _buffer.remove(0);
      data.write(_os);
      totalWritten += data.length();
      _rh.request(1);
    }
    if (_allDataRead && _os.isReady() && _buffer.isEmpty())
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
    _count += totalWritten;

  }

}
