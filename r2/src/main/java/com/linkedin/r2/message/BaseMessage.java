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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


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

  /**
   * This method lazy-init _body and returns it
   *
   * This is trying is mimic the old behavior because previously calling getEntity() multiple
   * times is allowed and very cheap, although I didn't find any use case for that.
   * If that's not required, we could remove _lock & _body and just creates the ByteString from
   * the EntityStream.
   *
   * @return the whole entity
   */
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
          _entityStream.setReader(reader, 4096);
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

    // This is to mimic the old behavior
    // we consider two messages as equal if they all have whole entity and entities equal
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
    // if _body is not null, we need to use _body to calc hash because the relationship between equals & hashCode
    if (_body != null)
    {
      return _body.hashCode();
    }

    return _entityStream.hashCode();
  }

  /**
   * A private writer that produce content based on the ByteString.
   */
  private static class ByteStringWriter implements Writer
  {
    final ByteString _content;
    private int _offset;
    private WriteHandle _wh;
    private int _chunkSize;

    ByteStringWriter(ByteString content)
    {
      _content = content;
      _offset = 0;
    }

    public void onInit(WriteHandle wh, int chunkSize)
    {
      _wh = wh;
      _chunkSize = chunkSize;
    }

    public void onWritePossible()
    {
      while(_wh.isWritable() && _offset < _content.length())
      {
        int bytesToWrite = Math.min(_chunkSize, _content.length() - _offset);
        _wh.write(_content.slice(_offset, bytesToWrite));
        _offset += bytesToWrite;
        if (_offset == _content.length())
        {
          _wh.done();
        }
      }
    }
  }

  /**
   * A private reader that reads everything in the EntityStream and dumps into a ByteString
   */
  private static class BlockingReader implements Reader
  {
    final private CountDownLatch _latch = new CountDownLatch(1);
    final private NoCopyByteArrayOutputStream _outputStream = new NoCopyByteArrayOutputStream();
    private volatile Throwable _error;


    private ReadHandle _rh;

    public void onInit(ReadHandle rh)
    {
      _rh = rh;
      _rh.read(Integer.MAX_VALUE);
    }

    public void onDataAvailable(ByteString data)
    {
      try
      {
        data.write(_outputStream);
      }
      catch (Exception ex)
      {
        _error = ex;
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
      _error = ex;
      _latch.countDown();
    }

    public ByteString get()
    {
      try
      {
        while(_error == null)
        {
          _latch.await(5000, TimeUnit.MILLISECONDS);
        }
      }
      catch (InterruptedException ex)
      {
        _error = ex;
      }

      if (_error != null)
      {
        throw new RuntimeException("Read entity failed: ", _error);
      }

      // commons-io 2.5 ByteArrayOutputStream has toInputStream() method; the returned stream is backed
      // by buffers of this stream, avoiding memory allocation and copy
      // but commons-io 2.5 is still SNAPSHOT version in their website, so we hacked our own

      try
      {
        return ByteString.read(_outputStream.toInputStream(), _outputStream.size());
      }
      catch (IOException ex)
      {
        // shouldn't happen
        throw new RuntimeException(ex);
      }
    }
  }

  private static class NoCopyByteArrayOutputStream extends ByteArrayOutputStream
  {
    public synchronized InputStream toInputStream()
    {
      return new ByteArrayInputStream(super.buf, 0, super.count);
    }
  }
}
