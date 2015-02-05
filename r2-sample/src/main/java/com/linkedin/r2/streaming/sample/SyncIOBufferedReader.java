package com.linkedin.r2.streaming.sample;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This example reader deals with Synchronous IO, which is the case for Servlet API 3.0 & Jetty 8
 *
 * This Reader reads from the EntityStream of a RestResponse and writes to ServletOutputStream.
 *
 * @author Zhenkai Zhu
 */
public class SyncIOBufferedReader implements Reader
{
  final private static ByteString DONE = ByteString.copy(new byte[1]);
  final private static ByteString ERROR = ByteString.copy(new byte[1]);

  final private ServletOutputStream _os;
  final private BlockingQueue<ByteString> _queue;
  final private int _permittedChunks;

  private ReadHandle _readHandle;
  private Throwable _e;

  SyncIOBufferedReader(ServletOutputStream os, int permittedChunks)
  {
    _os = os;
    _permittedChunks = permittedChunks;
    _queue = new LinkedBlockingDeque<ByteString>();
  }

  public void writeToServletOutputStream() throws ServletException, IOException, InterruptedException
  {
    while (true)
    {
      ByteString data = _queue.take();

      if (data == DONE)
      {
        _os.close();
        break;
      }
      else if (data == ERROR)
      {
        // we've streamed part of the response... now throw and let the other end timeout... ?
        throw new ServletException("Error reading data from response entity stream", _e);
      }

      data.write(_os);

      // finished writing data to ServletOutputStream, now signal writer we want one more chunk of data
      _readHandle.read(1);
    }
  }

  public void onInit(ReadHandle rh)
  {
    _readHandle = rh;
    // signal writer that we can accept number of _permittedChunks chunks
    _readHandle.read(_permittedChunks);
  }

  public void onDataAvailable(ByteString data)
  {
    // just add to unbounded blocking queue, no blocking.
    // the queue is actually bounded, because WriteHandle guarantees that writer cannot
    // write more than reader permitted.
    _queue.add(data);
  }

  public void onDone()
  {
    _queue.add(DONE);
  }

  public void onError(Throwable e)
  {
    _queue.add(ERROR);
    _e = e;
  }
}
