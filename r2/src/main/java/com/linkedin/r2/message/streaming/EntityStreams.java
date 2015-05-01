package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A class consists exclusively of static methods to deal with EntityStream {@link com.linkedin.r2.message.streaming.EntityStream}
 *
 * @author Zhenkai Zhu
 */
public final class EntityStreams
{
  private EntityStreams() {}

  public static EntityStream emptyStream()
  {
    return new EmptyStreamImpl();
  }

  // this is a work around to deal with case where user code doesn't read the empty stream (e.g. just check the
  // response code and does not read the response EntityStream
  public static boolean isEmptyStream(EntityStream entityStream)
  {
    return entityStream instanceof EmptyStreamImpl;
  }

  /**
   * The method to create a new EntityStream with a writer for the stream
   *
   * @param writer the writer for the stream who would provide the data
   * @return an instance of EntityStream
   */
  public static EntityStream newEntityStream(Writer writer)
  {
    return new EntityStreamImpl(writer);
  }

  private enum State
  {
    UNINITIALIZED,
    ACTIVE,
    FINISHED,
  }

  private static class EntityStreamImpl implements EntityStream
  {
    private final Writer _writer;
    private final Object _lock;
    private List<Observer> _observers;
    private Reader _reader;
    //private int _remaining;

    private final AtomicInteger _remaining;
    private boolean _notifyWritePossible;
    private AtomicReference<State> _state;

    EntityStreamImpl(Writer writer)
    {
      _writer = writer;
      _lock = new Object();
      _observers = new ArrayList<Observer>();
      _remaining = new AtomicInteger(0);
      _notifyWritePossible = true;
      _state = new AtomicReference<State>(State.UNINITIALIZED);
    }

    public void addObserver(Observer o)
    {
      synchronized (_lock)
      {
        checkInit();
        _observers.add(o);
      }
    }

    public void setReader(Reader r)
    {
      synchronized (_lock)
      {
        checkInit();
        _state.set(State.ACTIVE);
        _reader = r;
        _observers = Collections.unmodifiableList(_observers);
      }

      final WriteHandle wh = new WriteHandleImpl();
      _writer.onInit(wh);

      final ReadHandle rh = new ReadHandleImpl();
      _reader.onInit(rh);
    }

    private class WriteHandleImpl implements WriteHandle
    {
      @Override
      public void write(final ByteString data)
      {
//        synchronized (_lock)
//        {
//          if (_state.get() == State.FINISHED)
//          {
//            throw new IllegalStateException("Attempting to write after done or error of WriteHandle is invoked");
//          }
//
//
//          if (--_remaining < 0)
//          {
//            throw new IllegalArgumentException("Attempt to write when remaining is 0");
//          }
//        }
        if (_state.get() == State.FINISHED)
        {
          throw new IllegalStateException("Attempting to write after done or error of WriteHandle is invoked");
        }

        if (_remaining.decrementAndGet() < 0)
        {
          throw new IllegalStateException("Attempt to write when remaining is 0");
        }

        for (Observer observer : _observers)
        {
          observer.onDataAvailable(data);
        }
        _reader.onDataAvailable(data);
      }

      @Override
      public void done()
      {
        if (_state.compareAndSet(State.ACTIVE, State.FINISHED))
        {
          for (Observer observer : _observers)
          {
            observer.onDone();
          }
          _reader.onDone();
        }
      }

      @Override
      public void error(final Throwable e)
      {
        if (_state.compareAndSet(State.ACTIVE, State.FINISHED))
        {
          for (Observer observer : _observers)
          {
            observer.onError(e);
          }
          _reader.onError(e);
        }
      }

      @Override
      public int remaining()
      {
        synchronized (_lock)
        {
          int remaining = _remaining.get();
          if (remaining == 0)
          {
            _notifyWritePossible = true;
          }
          return remaining;
        }
      }
    }

    private class ReadHandleImpl implements ReadHandle
    {
      @Override
      public void request(final int chunkNum)
      {
        boolean needNotify = false;
        synchronized (_lock)
        {
          _remaining.addAndGet(chunkNum);
          // overflow
          if (_remaining.get() < 0)
          {
            _remaining.set(Integer.MAX_VALUE);
          }

          // notify the writer if needed
          if (_notifyWritePossible)
          {
            needNotify = true;
            _notifyWritePossible = false;
          }
        }

        if (needNotify)
        {
          _writer.onWritePossible();
        }
      }
    }

    private void checkInit()
    {
      if (_state.get() != State.UNINITIALIZED)
      {
        throw new IllegalStateException("EntityStream had already been initialized and can no longer accept Observers or Reader");
      }
    }
  }

  private static class EmptyStreamImpl extends EntityStreamImpl
  {
    EmptyStreamImpl()
    {
      super(new EmptyWriter());
    }

    private static class EmptyWriter implements Writer
    {
      private WriteHandle _wh;
      @Override
      public void onInit(WriteHandle wh)
      {
        _wh = wh;
      }

      @Override
      public void onWritePossible()
      {
        _wh.done();
      }
    }
  }
}
