package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.r2.util.StickyEventExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A class consists exclusively of static methods to deal with EntityStream {@link com.linkedin.r2.message.streaming.EntityStream}
 *
 * @author Zhenkai Zhu
 */
public final class EntityStreams
{
  private EntityStreams() {}

  // StickEventExecutor executes events with the same key in the same thread in order.
  // We use it here to ensure the events for the same EntityStream is executed in the same thread in order.
  final private static StickyEventExecutor _executor = new StickyEventExecutor("R2-EntityStreams-Executor", 256, 20000);

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

  private static class EntityStreamImpl implements EntityStream
  {
    final private Writer _writer;
    final private Lock _lock;
    private List<Observer> _observers;
    private Reader _reader;
    private boolean _initialized;
    private Semaphore _capacity;
    private int _chunkSize;

    EntityStreamImpl(Writer writer)
    {
      _writer = writer;
      _lock = new ReentrantLock();
      _observers = new ArrayList<Observer>();
      _initialized = false;
      _capacity = new Semaphore(0);
      _chunkSize = 0;
    }

    public void addObserver(Observer o)
    {
      try
      {
        _lock.lock();
        checkInit();
        _observers.add(o);
      }
      finally
      {
        _lock.unlock();
      }
    }

    public void setReader(Reader r, final int chunkSize)
    {
      try
      {
        _lock.lock();
        checkInit();
        _reader = r;
        _chunkSize = chunkSize;
        _initialized = true;
        _observers = Collections.unmodifiableList(_observers);
      }
      finally
      {
        _lock.unlock();
      }

      Runnable notifyWriterInit = new Runnable()
      {
        @Override
        public void run()
        {
          final WriteHandle wh = new WriteHandleImpl();
          _writer.onInit(wh, chunkSize);
        }
      };
      dispatch(notifyWriterInit);

      Runnable notifyReaderInit = new Runnable()
      {
        @Override
        public void run()
        {
          final ReadHandle rh = new ReadHandleImpl();
          _reader.onInit(rh);
        }
      };
      dispatch(notifyReaderInit);
    }

    private class WriteHandleImpl implements WriteHandle
    {
      @Override
      public void write(final ByteString data)
      {

        if(!_capacity.tryAcquire())
        {
          throw new IllegalStateException("There is no permits left for write.");
        }

        if (data.length() > _chunkSize)
        {
          throw new IllegalArgumentException("Data length " + data.length() +
              " is larger than the desired chunk size: " + _chunkSize);
        }

        Runnable notifyReadPossible = new Runnable()
        {
          @Override
          public void run()
          {
            for(Observer observer: _observers)
            {
              observer.onDataAvailable(data);
            }
            _reader.onDataAvailable(data);
          }
        };
        dispatch(notifyReadPossible);
      }

      @Override
      public void done()
      {
        Runnable notifyDone = new Runnable()
        {
          @Override
          public void run()
          {
            for(Observer observer: _observers)
            {
              observer.onDone();
            }
            _reader.onDone();
          }
        };
        dispatch(notifyDone);
      }

      @Override
      public void error(final Throwable e)
      {
        Runnable notifyError = new Runnable()
        {
          @Override
          public void run()
          {
            for(Observer observer: _observers)
            {
              observer.onError(e);
            }
            _reader.onError(e);
          }
        };
        dispatch(notifyError);
      }
    }

    private class ReadHandleImpl implements ReadHandle
    {
      @Override
      public void read(final int chunkNum)
      {
        Runnable notifyWritePossible = new Runnable()
        {
          @Override
          public void run()
          {
            _capacity.release(chunkNum);
            _writer.onWritePossible(chunkNum);
          }
        };
        dispatch(notifyWritePossible);
      }
    }

    private void dispatch(Runnable runnable)
    {
      // identify the event with the hashCode of this EntityStream
      StickyEventExecutor.Event event = new StickyEventExecutor.Event(hashCode(), runnable);
      _executor.dispatch(event);
    }

    private void checkInit()
    {
      if (_initialized)
      {
        throw new IllegalStateException("EntityStream had already been initialized and can no longer accept Observers or Reader");
      }
    }
  }
}
