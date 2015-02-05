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
  final Semaphore _sem;
  private WriteHandle _wh;
  private int _chunkSize;

  final private static ByteString DONE = ByteString.copy(new byte[1]);
  final private static ByteString ERROR = ByteString.copy(new byte[1]);

  public SyncIOBufferedWriter(ServletInputStream is)
  {
    _is = is;
    _sem = new Semaphore(0);
  }

  @Override
  public void onInit(WriteHandle wh, int chunkSize)
  {
    _wh = wh;
    _chunkSize = chunkSize;
  }

  @Override
  public void onWritePossible()
  {
  }

  public void readFromServletInputStream()
  {
    byte[] bytes = new byte[_chunkSize];
    try
    {
      while (true)
      {
        int dataLen = _is.read(bytes);

        if (dataLen < 0)
        {
          _wh.done();
          break;
        }
        else
        {
          ByteString data = ByteString.copy(bytes, 0, dataLen);
          if (_wh.isWritable())
          {
            _wh.write(data);
          }
          else
          {
            _sem.acquire();
          }
        }
      }
    }
    catch (Exception e)
    {
      _wh.error(e);
    }
  }

}
