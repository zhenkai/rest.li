/*
   Copyright (c) 2012 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/* $Id$ */
package com.linkedin.r2.message;


import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Abstract implementation of the {@link Message} interface.
 *
 * @author Chris Pettitt
 * @version $Revision$
 */
public abstract class BaseMessage implements Message
{
  private final EntityStream _entityStream;
  private final Object _lock = new Object();
  private volatile ByteString _body;

  /**
   * Construct a new instance with the specified body (entity).
   *
   * @param body the {@link ByteString} body to be used as the entity for this message.
   */
  public BaseMessage(ByteString body)
  {
    assert body != null;
    _body = body;
    _entityStream = EntityStreams.newEntityStream(new ByteStringWriter(_body));
  }

  @Override
  public ByteString getEntity()
  {
    if (_body == null)
    {
      synchronized (_lock)
      {
        if (_body == null)
        {
          BlockingReader reader = new BlockingReader();
          _entityStream.setReader(reader);
          _body = reader.get();
        }
      }
    }

    return _body;
  }

  public BaseMessage(EntityStream stream)
  {
    _entityStream = stream;
    _body = null;
  }

  @Override
  public EntityStream getEntityStream()
  {
    return _entityStream;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o)
    {
      return true;
    }

    if (!(o instanceof BaseMessage))
    {
      return false;
    }

    BaseMessage that = (BaseMessage) o;

    if (_body != null)
    {
      return _body.equals(that._body);
    }
    else
    {
      return that._body == null;
    }
  }

  @Override
  public int hashCode()
  {
    if (_body != null)
    {
      return _body.hashCode();
    }

    return _entityStream.hashCode();
  }

  private static class ByteStringWriter implements Writer
  {
    final ByteString _content;
    private int _offset;
    private WriteHandle _wh;

    ByteStringWriter(ByteString content)
    {
      _content = content;
      _offset = 0;
    }

    public void onInit(WriteHandle wh)
    {
      _wh = wh;
    }

    public void onWritePossible(int bytesNum)
    {
      if (_offset <= _content.length())
      {
        int bytesToWrite = Math.min(bytesNum, _content.length() - _offset);
        _wh.write(_content.slice(_offset, bytesToWrite));
        _offset += bytesToWrite;
        if (_offset == _content.length())
        {
          _wh.done();
        }
      }
    }
  }

  private static class BlockingReader implements Reader
  {
    final private CountDownLatch _latch = new CountDownLatch(1);
    final private AtomicReference<Throwable> _error = new AtomicReference<Throwable>();
    final private ByteArrayOutputStream _outputStream = new ByteArrayOutputStream();

    private ReadHandle _rh;

    public void onInit(ReadHandle rh)
    {
      _rh = rh;
      _rh.read(Integer.MAX_VALUE);
    }

    public void onReadPossible(ByteString data)
    {
      try
      {
        data.write(_outputStream);
      }
      catch (Exception ex)
      {
        _error.set(ex);
        _latch.countDown();
        throw new RuntimeException("Read entity failed: ", ex);
      }
    }

    public void onDone()
    {
      _latch.countDown();
    }

    public void onError(Throwable ex)
    {
      _error.set(ex);
      _latch.countDown();
    }

    public ByteString get()
    {
      try
      {
        while(_error.get() == null)
        {
          _latch.await(5000, TimeUnit.MILLISECONDS);
        }
      }
      catch (InterruptedException ex)
      {
        _error.set(ex);
      }

      if (_error.get() != null)
      {
        throw new RuntimeException("Read entity failed: ", _error.get());
      }

      // two copies! But this is needed to make ByteString immutable
      return ByteString.copy(_outputStream.toByteArray());
    }
  }
}
