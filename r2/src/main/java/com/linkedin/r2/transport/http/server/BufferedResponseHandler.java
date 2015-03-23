package com.linkedin.r2.transport.http.server;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Zhenkai Zhu
 */
/* package private */class BufferedResponseHandler implements WriteListener, Reader
{
  private NoCopyByteArrayOutputStream _bufferStream;
  private final Object _lock = new Object();
  private final ServletOutputStream _os;
  private final AsyncContext _ctx;
  private final int _bufferSize;
  private ReadHandle _rh;
  private boolean _shouldComplete = false;

  BufferedResponseHandler(int bufferSize, ServletOutputStream os, AsyncContext ctx)
  {
    _bufferSize = bufferSize;
    _bufferStream = new NoCopyByteArrayOutputStream(bufferSize);
    _os = os;
    _ctx = ctx;
  }

  public void onWritePossible() throws IOException
  {
    synchronized (_lock)
    {
      if (_bufferStream.getCount() > 0)
      {
        _os.write(_bufferStream.getBuffer(), 0, _bufferStream.getCount());
        _bufferStream = new NoCopyByteArrayOutputStream(_bufferSize);
        _rh.read(_bufferStream.getCount());
      }

      if (_shouldComplete && _os.isReady())
      {
        _ctx.complete();
      }
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
      try
      {
        if (_os.isReady())
        {
          _os.write(data.copyBytes());
          _rh.read(data.length());
        }
        else
        {
          _bufferStream.write(data.copyBytes());
        }
      }
      catch (IOException ex)
      {
        throw new RuntimeException(ex);
      }
    }
  }

  public void onDone()
  {
    synchronized (_lock)
    {
      if (_bufferStream.getCount() == 0 && _os.isReady())
      {
        _ctx.complete();
      }
      else
      {
        _shouldComplete = true;
      }
    }
  }

  public void onInit(final ReadHandle rh)
  {
    _rh = rh;
    _os.setWriteListener(this);
    _rh.read(_bufferSize);
  }

  private static class NoCopyByteArrayOutputStream extends ByteArrayOutputStream
  {
    NoCopyByteArrayOutputStream(int size)
    {
      super(size);
    }

    int getCount()
    {
      return super.count;
    }

    byte[] getBuffer()
    {
      return super.buf;
    }

  }
}
