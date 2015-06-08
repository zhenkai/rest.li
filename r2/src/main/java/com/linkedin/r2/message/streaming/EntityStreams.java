package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.r2.util.StickyEventExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
  final private static StickyEventExecutor _executor;

  static
  {
    _executor =new StickyEventExecutor("R2-EntityStreams-Executor", 256, 20000);
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        _executor.shutdown();
      }
    }, "R2-EntityStreams-Executor.shutdown"));
  }

  /**
   * The method to create a new EntityStream with a writer for the stream
   *
   * @param writer the writer for the stream who would provide the data
   * @return an instance of EntityStream
   */
  public static EntityStream newEntityStream(Writer writer)
  {
    return new EntityStreamImpl(writer, null);
  }

  /**
   * The method to create a new EntityStream with a writer for the stream and a specific streamId.
   *
   * This is usually used when the new EntityStream is to be linked to to an existing EntityStream so that the events
   * for the two EntityStreams will be processed by the same event thread (to simplify event code).
   * For example, see CipherProxy.
   *
   * @param writer the writer for the stream who would provide the data
   * @param upStream the existing EntityStream where the source of data is from
   * @return a new instance of EntityStream
   */
  public static EntityStream newEntityStream(Writer writer, EntityStream upStream)
  {
    return new EntityStreamImpl(writer, upStream.hashCode());
  }

  private static class EntityStreamImpl implements EntityStream
  {
    final private Writer _writer;
    final private Integer _overrideKey;
    final private Object _lock;
    private List<Observer> _observers;
    private Reader _reader;
    private boolean _initialized;
    // maintains the allowed capacity which is controlled by reader
    private Semaphore _capacity;
    final private AtomicBoolean _notifyWritePossible;
    private int _chunkSize;

    EntityStreamImpl(Writer writer, Integer overrideKey)
    {
      _writer = writer;
      _overrideKey = overrideKey;
      _lock = new Object();
      _observers = new ArrayList<Observer>();
      _initialized = false;
      _capacity = new Semaphore(0);
      _notifyWritePossible = new AtomicBoolean(true);
      _chunkSize = 0;
    }

    public void addObserver(Observer o)
    {
      synchronized (_lock)
      {
        checkInit();
        _observers.add(o);
      }
    }

    public void setReader(Reader r, final int chunkSize)
    {
      synchronized (_lock)
      {
        checkInit();
        _reader = r;
        _chunkSize = chunkSize;
        _initialized = true;
        _observers = Collections.unmodifiableList(_observers);
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

        // Writer tries to try when the reader didn't request more data
        if(!_capacity.tryAcquire())
        {
          throw new IllegalStateException("Trying to write when WriteHandle is not writable.");
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

      @Override
      public boolean isWritable()
      {
        if (_capacity.availablePermits() > 0)
        {
          return true;
        }
        else
        {
          // According to the Writer.onWritePossible contract, we need to notify writer
          // when it's writable again.
          _notifyWritePossible.set(true);
          return false;
        }
      }
    }

    private class ReadHandleImpl implements ReadHandle
    {
      @Override
      public void read(final int chunkNum)
      {
        _capacity.release(chunkNum);

        // notify the writer if needed
        if (_notifyWritePossible.compareAndSet(true, false))
        {

          Runnable notifyWritePossible = new Runnable()
          {
            @Override
            public void run()
            {
              {
                _writer.onWritePossible();
              }
            }
          };
          dispatch(notifyWritePossible);
        }
      }
    }

    private void dispatch(Runnable runnable)
    {
      // identify the event with the hashCode of this EntityStream unless an overrideKey has been provided
      int key = _overrideKey == null ? hashCode() : _overrideKey;
      StickyEventExecutor.Event event = new StickyEventExecutor.Event(key, runnable);
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
