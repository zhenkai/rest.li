package com.linkedin.r2.transport.http.server;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.AbortedException;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.linkedin.r2.filter.R2Constants.DEFAULT_DATA_CHUNK_SIZE;

/**
 * This writer deals with Synchronous IO, which is the case for Servlet API 3.0 & Jetty 8
 *
 * This Writer reads from ServletInputStream and writes to the EntityStream of a request; and reads from
 * the EntityStream of a response and writes into ServletOutputStream.
 *
 * @author Zhenkai Zhu
 */
public class SyncIOHandler implements Writer, Reader
{
  private final ServletInputStream _is;
  private final ServletOutputStream _os;
  private final int _maxBufferedChunks;
  private final BlockingQueue<Event> _eventQueue;
  private volatile WriteHandle _wh;
  private volatile ReadHandle _rh;
  private boolean _forceExit;
  private boolean _requestReadFinished;
  private boolean _responseWriteFinished;
  private final long _timeout;

  public SyncIOHandler(ServletInputStream is, ServletOutputStream os, int maxBufferedChunks, long timeout)
  {
    _is = is;
    _os = os;
    _maxBufferedChunks = maxBufferedChunks;
    _eventQueue = new LinkedBlockingDeque<Event>();
    _requestReadFinished = false;
    _responseWriteFinished = false;
    _forceExit = false;
    _timeout = timeout;
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    _wh = wh;
  }

  @Override
  public void onWritePossible()
  {
    _eventQueue.add(Event.WriteRequestPossibleEvent);
  }

  @Override
  public void onAbort(Throwable ex)
  {
    _eventQueue.add(new Event(EventType.WriteRequestAborted, ex));
  }

  @Override
  public void onInit(ReadHandle rh)
  {
    _rh = rh;
    _rh.request(_maxBufferedChunks);
  }

  @Override
  public void onDataAvailable(ByteString data)
  {
    _eventQueue.add(new Event(EventType.ResponseDataAvailable, data));
  }

  @Override
  public void onDone()
  {
    _eventQueue.add(Event.FullResponseReceivedEvent);
  }

  @Override
  public void onError(Throwable e)
  {
    _eventQueue.add(new Event(EventType.ResponseDataError, e));
  }

  public void loop() throws ServletException, IOException
  {
    final long startTime = System.currentTimeMillis();
    byte[] buf = new byte[DEFAULT_DATA_CHUNK_SIZE];

    while(shouldContinue() && !_forceExit)
    {
      Event event;
      try
      {
        long timeSpent = System.currentTimeMillis() - startTime;
        long maxWaitTime = timeSpent < _timeout ? _timeout - timeSpent : 0;
        event = _eventQueue.poll(maxWaitTime, TimeUnit.MILLISECONDS);
        if (event == null)
        {
          throw new TimeoutException("Timeout after " + _timeout + " milliseconds.");
        }
      }
      catch (Exception ex)
      {
        throw new ServletException(ex);
      }

      switch (event.getEventType())
      {
        case ResponseDataAvailable:
        {
          ByteString data =  (ByteString) event.getData();
          data.write(_os);
          _rh.request(1);
          break;
        }
        case WriteRequestPossible:
        {
          while (_wh.remaining() > 0)
          {
            int actualLen = _is.read(buf);

            if (actualLen < 0)
            {
              _wh.done();
              _requestReadFinished = true;
              break;
            }
            _wh.write(ByteString.copy(buf, 0, actualLen));
          }
          break;
        }
        case FullResponseReceived:
        {
          _os.close();
          _responseWriteFinished = true;
          break;
        }
        case ResponseDataError:
        {
          //throw new ServletException((Throwable)event.getData());
          _os.close();
          _responseWriteFinished = true;
          break;

        }
        case WriteRequestAborted:
        {
          if (event.getData() instanceof AbortedException)
          {
            // reader cancels, we'll drain the stream on behalf of reader
            // we don't directly drain it here because we'd like to give other events
            // some opportunities to be executed; e.g. return an error response
            _eventQueue.add(Event.DrainRequestEvent);
          }
          else
          {
            // reader throws, which it shouldn't do, we cannot do much
            // TODO: do we want to be smarter and return server error response?
            throw new ServletException((Throwable)event.getData());
          }
          break;
        }
        case DrainRequest:
        {
          for (int i = 0; i < 10; i++)
          {
            int actualLen = _is.read(buf);
            if (actualLen < 0)
            {
              _requestReadFinished = true;
              break;
            }
          }
          if (!_requestReadFinished)
          {
            // add self back to event queue and give others a chance to run
            _eventQueue.add(Event.DrainRequestEvent);
          }
          break;
        }
        case ForceExit:
        {
          _forceExit = true;
          break;
        }
        default:
          throw new IllegalStateException("Unknown event type:" + event.getEventType());
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

  protected void exitLoop()
  {
    _eventQueue.add(Event.ForceExitEvent);
  }

  private static enum  EventType
  {
    WriteRequestPossible,
    WriteRequestAborted,
    DrainRequest,
    FullResponseReceived,
    ResponseDataAvailable,
    ResponseDataError,
    ForceExit,
  }

  private static class Event
  {
    private final EventType _eventType;
    private final Object _data;

    static final Event WriteRequestPossibleEvent = new Event(EventType.WriteRequestPossible);
    static final Event FullResponseReceivedEvent = new Event(EventType.FullResponseReceived);
    static final Event DrainRequestEvent = new Event(EventType.DrainRequest);
    static final Event ForceExitEvent = new Event(EventType.ForceExit);

    Event(EventType eventType)
    {
      this(eventType, null);
    }

    Event(EventType eventType, Object data)
    {
      _eventType = eventType;
      _data = data;
    }

    EventType getEventType()
    {
      return _eventType;
    }

    Object getData()
    {
      return _data;
    }
  }
}
