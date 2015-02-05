package com.linkedin.r2.streaming.sample;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import java.io.IOException;
import java.util.concurrent.Semaphore;

/**
 * This example writer deals with Synchronous IO, which is the case for Servlet API 3.0 & Jetty 8
 *
 * This Writer reads from ServletOutputStream and writes to the EntityStream of a RestRequest.
 *
 * @author Zhenkai Zhu
 */
public class SyncIOWriter implements Writer
{
  final private ServletInputStream _is;
  private WriteHandle _wh;
  final private Semaphore _capacity;
  private int _chunkSize;

  public SyncIOWriter(ServletInputStream is)
  {
    _is = is;
    _capacity = new Semaphore(0);
  }

  @Override
  public void onInit(WriteHandle wh, int chunkSize)
  {
    _wh = wh;
    _chunkSize = chunkSize;
  }

  @Override
  public void onWritePossible(int chunkNum)
  {
    _capacity.release(chunkNum);
  }

  public void readFromServletInputStream()
  {
    byte[] bytes = new byte[_chunkSize];
    try
    {
      while (true)
      {
        _capacity.acquire();
        int dataLen = _is.read(bytes);

        if (dataLen < 0)
        {
          _wh.done();
          break;
        }
        else
        {
          _wh.write(ByteString.copy(bytes, 0, dataLen));
        }
      }
    }
    catch (Exception e)
    {
      _wh.error(e);
    }
  }

}
