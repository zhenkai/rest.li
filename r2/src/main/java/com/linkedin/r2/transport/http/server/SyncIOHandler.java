package com.linkedin.r2.transport.http.server;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This example writer deals with Synchronous IO, which is the case for Servlet API 3.0 & Jetty 8
 *
 * This Writer reads from ServletOutputStream and writes to the EntityStream of a RestRequest.
 *
 * @author Zhenkai Zhu
 */
public class SyncIOHandler implements Writer, Reader
{
  final private ServletInputStream _is;
  final private ServletOutputStream _os;
  final private int _bufferCapacity;
  final private BlockingQueue<Event> _eventQueue;
  private WriteHandle _wh;
  private ReadHandle _rh;
  private boolean _requestReadFinished = false;
  private boolean _responseWriteFinished = false;
  private int _firstByte;

  public SyncIOHandler(ServletInputStream is, ServletOutputStream os, int bufferCapacity)
  {
    _is = is;
    _os = os;
    _bufferCapacity = bufferCapacity;
    _eventQueue = new LinkedBlockingDeque<Event>();
    _eventQueue.add(new ReadFirstByteOfRequestEvent());
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    _wh = wh;
  }

  @Override
  public void onWritePossible()
  {
    _eventQueue.add(WriteRequestPossibleEvent.create());
  }

  @Override
  public void onInit(ReadHandle rh)
  {
    _rh = rh;
    _rh.read(_bufferCapacity);
  }

  @Override
  public void onDataAvailable(ByteString data)
  {
    _eventQueue.add(new ResponseDataAvailableEvent(data));
  }

  @Override
  public void onDone()
  {
    _eventQueue.add(FullResponseReceivedEvent.create());
  }

  @Override
  public void onError(Throwable e)
  {
    _eventQueue.add(new ResponseDataErrorEvent(e));
  }

  public void loop() throws ServletException, IOException
  {
    while(shouldContinue())
    {

      Event event;
      try
      {
        event = _eventQueue.take();
      }
      catch (Exception ex)
      {
        throw new ServletException(ex);
      }

      if (event instanceof ReadFirstByteOfRequestEvent)
      {
        _firstByte = _is.read();
        if (_firstByte < 0)
        {
          _requestReadFinished = true;
        }
      }
      else if (event instanceof ResponseDataAvailableEvent)
      {
        ByteString data = ((ResponseDataAvailableEvent) event).getData();
        data.write(_os);
        _rh.read(data.length());
      }
      else if (event instanceof WriteRequestPossibleEvent)
      {
        byte[] buf = new byte[4096];
        while(_wh.remaining() > 0)
        {
          int len = Math.min(buf.length, _wh.remaining());
          int actualLen;
          try
          {
            if (_firstByte >= 0)
            {
              actualLen = _is.read(buf, 1, len - 1) + 1;
              buf[0] = (byte) _firstByte;
              _firstByte = -1;
            }
            else
            {
              actualLen = _is.read(buf, 0, len);
            }
          }
          catch (IOException ex)
          {
            _wh.error(ex);
            throw ex;
          }
          if (actualLen < 0)
          {
            _wh.done();
            _requestReadFinished = true;
            break;
          }
          _wh.write(ByteString.copy(buf, 0, actualLen));
        }
      }
      else if (event instanceof FullResponseReceivedEvent)
      {
        _os.close();
        _responseWriteFinished = true;
      }
      else if (event instanceof ResponseDataErrorEvent)
      {
        throw new ServletException(((ResponseDataErrorEvent) event).getCause());
      }
    }
  }

  protected boolean shouldContinue()
  {
    return !_responseWriteFinished || !_requestReadFinished;
  }

  protected boolean responseWriteFinished()
  {
    return _responseWriteFinished;
  }

  protected boolean requestReadFinished()
  {
    return _requestReadFinished;
  }

  private interface Event
  {

  }

  private static class WriteRequestPossibleEvent implements Event
  {
    private static final WriteRequestPossibleEvent EVENT = new WriteRequestPossibleEvent();

    private WriteRequestPossibleEvent() {}

    static WriteRequestPossibleEvent create()
    {
      return EVENT;
    }
  }

  private static class FullResponseReceivedEvent implements Event
  {
    private static final FullResponseReceivedEvent EVENT = new FullResponseReceivedEvent();
    private FullResponseReceivedEvent() {}

    static FullResponseReceivedEvent create()
    {
      return EVENT;
    }
  }

  private static class ResponseDataAvailableEvent implements Event
  {
    private final ByteString _data;

    ResponseDataAvailableEvent(ByteString data)
    {
      _data = data;
    }

    ByteString getData()
    {
      return _data;
    }
  }

  private static class ResponseDataErrorEvent implements Event
  {
    private final Throwable _cause;

    ResponseDataErrorEvent(Throwable cause)
    {
      _cause = cause;
    }

    Throwable getCause()
    {
      return _cause;
    }
  }

  private static class ReadFirstByteOfRequestEvent implements Event
  {
  }
}
