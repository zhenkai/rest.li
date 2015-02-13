package com.linkedin.r2.streaming.sample;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;

import javax.servlet.ServletInputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * This example writer deals with Synchronous IO, which is the case for Servlet API 3.0 & Jetty 8
 *
 * This Writer reads from ServletOutputStream and writes to the EntityStream of a RestRequest.
 *
 * @author Zhenkai Zhu
 */
public class SyncIOBufferedWriter implements Writer
{
  final private ServletInputStream _is;
  final private Object _signal;
  final private byte[] _bytes;
  private WriteHandle _wh;

  public SyncIOBufferedWriter(ServletInputStream is)
  {
    _is = is;
    _bytes = new byte[4096];
    _signal = new Object();
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    _wh = wh;
  }

  @Override
  public void onWritePossible()
  {
    // signal that write is possible so Jetty thread can continue writing
    // if it's currently waiting
    _signal.notify();
  }

  public void readFromServletInputStream()
  {

    try
    {
      while (true)
      {
        if (_wh.remainingCapacity() == 0)
        {
          _signal.wait();
        }

        int bytesToRead = Math.min(_wh.remainingCapacity(), _bytes.length);

        int bytesRead = _is.read(_bytes, 0, bytesToRead);

        if (bytesRead < 0)
        {
          _wh.done();
          break;
        }
        else
        {
          _wh.write(ByteString.copy(_bytes, 0, bytesRead));
        }
      }
    }
    catch (Exception e)
    {
      _wh.error(e);
    }
  }

}
