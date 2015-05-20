package com.linkedin.r2.transport.http.server;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This example writer deals with Synchronous IO, which is the case for Servlet API 3.0 & Jetty 8
 *
 * This Writer reads from ServletOutputStream and writes to the EntityStream of a RestRequest.
 *
 * @author Zhenkai Zhu
 */
public class AsyncCtxSyncIOHandler extends SyncIOHandler
{
  private final AtomicBoolean _completed = new AtomicBoolean(false);
  private final AsyncContext _ctx;
  private boolean _requestReadStarted = false;
  private boolean _responseWriteStarted = false;
  private boolean _inLoop = false;

  public AsyncCtxSyncIOHandler(ServletInputStream is, ServletOutputStream os, AsyncContext ctx, int bufferCapacity, long timeout)
  {
    super(is, os, bufferCapacity, timeout);
    _ctx = ctx;
  }

  @Override
  protected synchronized boolean shouldContinue()
  {
    boolean shouldContinue =  (_requestReadStarted && !requestReadFinished()
        || (_responseWriteStarted && !responseWriteFinished()));

    if (!shouldContinue)
    {
      _inLoop = false;
    }
    return shouldContinue;
  }

  public synchronized void startWritingResponse()
  {
    _responseWriteStarted = true;
  }

  public synchronized void startReadingRequest()
  {
    _requestReadStarted = true;
  }

  @Override
  public void loop() throws ServletException, IOException
  {
    synchronized (this)
    {
      if (_inLoop)
      {
        return;
      }
      else
      {
        _inLoop = true;
      }
    }
    super.loop();
    if (requestReadFinished() && responseWriteFinished())
    {
      if (_completed.compareAndSet(false, true))
      {
        _ctx.complete();
      }
    }
  }
}
