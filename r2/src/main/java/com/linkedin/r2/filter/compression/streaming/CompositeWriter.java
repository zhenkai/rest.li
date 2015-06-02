package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.DrainReader;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import java.util.Arrays;
import java.util.Iterator;


/**
 * @author Ang Xu
 */
public class CompositeWriter implements Writer
{
  private Iterator<EntityStream> _entityStreams;

  private WriteHandle _wh;

  private int _outstanding;
  private boolean _aborted = false;

  private ReadHandle _currentRh;
  private ReaderImpl _reader = new ReaderImpl();

  public CompositeWriter(EntityStream... entityStreams)
  {
    this(Arrays.asList(entityStreams));
  }

  public CompositeWriter(Iterable<EntityStream> entityStreams)
  {
    _entityStreams = entityStreams.iterator();
    _outstanding = 0;
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    _wh = wh;
    EntityStream stream = _entityStreams.next();
    stream.setReader(_reader);
  }

  @Override
  public void onWritePossible()
  {
    _outstanding = _wh.remaining();
    _currentRh.request(_outstanding);
  }

  @Override
  public void onAbort(Throwable e)
  {
    _aborted = true;
    _currentRh.request(Integer.MAX_VALUE);
    while(_entityStreams.hasNext())
    {
      EntityStream stream = _entityStreams.next();
      stream.setReader(new DrainReader());
    }

  }

  private class ReaderImpl implements Reader
  {
    @Override
    public void onInit(ReadHandle rh)
    {
      _currentRh = rh;
      if (_outstanding > 0)
      {
        _currentRh.request(_outstanding);
      }
    }

    @Override
    public void onDataAvailable(ByteString data)
    {
      if (!_aborted)
      {
        _wh.write(data);
        _outstanding--;
        int diff = _wh.remaining() - _outstanding;
        if (diff > 0)
        {
          _currentRh.request(diff);
          _outstanding += diff;
        }
      }
    }

    @Override
    public void onDone()
    {
      if (!_aborted)
      {
        if (_entityStreams.hasNext())
        {
          EntityStream stream = _entityStreams.next();
          stream.setReader(this);
        }
        else
        {
          _wh.done();
        }
      }
    }

    @Override
    public void onError(Throwable e)
    {
      _wh.error(e);
      while(_entityStreams.hasNext())
      {
        EntityStream stream = _entityStreams.next();
        stream.setReader(new DrainReader());
      }
    }
  }
}
