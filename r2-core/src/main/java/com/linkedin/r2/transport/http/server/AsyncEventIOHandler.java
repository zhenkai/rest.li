package com.linkedin.r2.transport.http.server;

import com.linkedin.r2.message.stream.entitystream.ReadHandle;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import java.io.IOException;

/**
 * This example writer deals with Synchronous IO, which is the case for Servlet API 3.0 & Jetty 8
 *
 * This Writer reads from ServletOutputStream and writes to the EntityStream of a RestRequest.
 *
 * @author Zhenkai Zhu
 */
public class AsyncEventIOHandler extends SyncIOHandler
{
  private final AbstractAsyncR2StreamServlet.WrappedAsyncContext _ctx;
  private volatile boolean _responseWriteStarted = false;
  private boolean _inLoop = false;

  public AsyncEventIOHandler(ServletInputStream is, ServletOutputStream os, AbstractAsyncR2StreamServlet.WrappedAsyncContext ctx, int bufferCapacity)
  {
    super(is, os, bufferCapacity, Integer.MAX_VALUE);
    _ctx = ctx;
  }

  @Override
  protected boolean shouldContinue()
  {
    boolean shouldContinue =  !requestReadFinished()
        || (_responseWriteStarted && !responseWriteFinished());

    if (!shouldContinue)
    {
      synchronized (this)
      {
        // check again in synchronized block to make sure we can exit safely
        shouldContinue =  !requestReadFinished()
            || (_responseWriteStarted && !responseWriteFinished());

        if (!shouldContinue)
        {
          _inLoop = false;
        }
      }
    }
    return shouldContinue;
  }

  public void exitLoop()
  {
    super.exitLoop();
  }

  @Override
  public void onInit(ReadHandle rh)
  {
    synchronized (this)
    {
      _responseWriteStarted = true;
    }
    super.onInit(rh);
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
      _ctx.complete();
    }
  }
}
