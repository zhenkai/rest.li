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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Zhenkai Zhu
 */
/* package private */class AsyncIOResponseHandler implements WriteListener, Reader
{
  private final ServletOutputStream _os;
  private final AsyncContext _ctx;
  private ReadHandle _rh;
  private volatile boolean _allDataRead = false;
  private final AtomicBoolean _completed = new AtomicBoolean(false);
  private final AtomicBoolean _otherDirectionFinished;

  // for debug
  private int _count = 0;

  AsyncIOResponseHandler(ServletOutputStream os, AsyncContext ctx, AtomicBoolean otherDirectionFinished)
  {
    _os = os;
    _ctx = ctx;
    _otherDirectionFinished = otherDirectionFinished;
  }

  public void onWritePossible() throws IOException
  {
    _rh.request(1);
    tryFinish();
  }

  public void onError(final Throwable t)
  {
    throw new RuntimeException("Bytes written so far: " + (_count / 1024 / 1024) + " MB", t);
  }

  public void onDataAvailable(final ByteString data)
  {
    try
    {
      // we can directly write because we requested one chunk only if it's ready to write, and isReady() guarantees
      // the next write to be ok
      doWrite(data);
    }
    catch (IOException ex)
    {
      throw new RuntimeException(ex);
    }
    if(_os.isReady())
    {
      _rh.request(1);
    }
  }

  public void onDone()
  {
    _allDataRead = true;
    tryFinish();
  }

  public void onInit(final ReadHandle rh)
  {
    _rh = rh;
    _os.setWriteListener(this);
  }

  private void doWrite(ByteString data) throws IOException
  {
    data.write(_os);
    _count += data.length();
  }

  private void tryFinish()
  {
    if (_allDataRead && _os.isReady())
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

  }

}
