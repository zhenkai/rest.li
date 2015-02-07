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
  private byte[] _bytes;
  private WriteHandle _wh;

  public SyncIOBufferedWriter(ServletInputStream is)
  {
    _is = is;
    _signal = new Object();
  }

  @Override
  public void onInit(WriteHandle wh, int chunkSize)
  {
    _wh = wh;
    _bytes = new byte[chunkSize];
  }

  @Override
  public void onWritePossible()
  {
    _signal.notify();
  }

  public void readFromServletInputStream()
  {

    try
    {
      while (true)
      {
        int dataLen = _is.read(_bytes);

        if (dataLen < 0)
        {
          _wh.done();
          break;
        }
        else
        {
          if (!_wh.isWritable())
          {
            _signal.wait();
          }
          _wh.write(ByteString.copy(_bytes, 0, dataLen));
        }
      }
    }
    catch (Exception e)
    {
      _wh.error(e);
    }
  }

}
