package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;



/**
 * An {@link InputStream} backed by an {@link com.linkedin.r2.message.streaming.EntityStream}.
 * Note: access to {@link InputStream} APIs are not thread-safe!
 *
 * @author Ang Xu
 */
public class BufferedReaderInputStream extends InputStream implements Reader
{
  private static final int CAPACITY = 3;
  private static final ByteString EOF = ByteString.copy(new byte[1]);

  private final BlockingQueue<ByteString> _buffers = new ArrayBlockingQueue<ByteString>(CAPACITY);

  private volatile boolean _readFinished = false;
  private volatile Throwable _throwable = null;

  private byte[] _buffer = null;
  private int _readIndex = 0;
  private ReadHandle _rh;

  /********* InputStream Impl *********/

  @Override
  public int read() throws IOException
  {

    if (_throwable != null)
    {
      throw new IOException(_throwable);
    }
    else if (done())
    {
      return -1;
    }
    else if (_buffer != null) // fast path
    {
      int b = _buffer[_readIndex++] & 0xff;
      if (_readIndex >= _buffer.length)
      {
        _buffer = null;
        _readIndex = 0;
      }
      return b;
    }

    try
    {
      // if we reach here, it means there is no bytes available at this moment
      // and we haven't finished reading EntityStream yet. So we have to block
      // waiting for more bytes.
      final ByteString data = _buffers.take();
      if (data != EOF)
      {
        _buffer = data.copyBytes();
        _rh.request(1);
      }
    }
    catch (InterruptedException ex)
    {
      _throwable = ex;
    }
    // recursively call read() with the belief that
    // it should return immediately this time.
    return read();
  }

  @Override
  public int available() throws IOException
  {
    int avail = _buffer == null ? 0 : _buffer.length - _readIndex;
    for (ByteString b : _buffers)
    {
      avail += b.length();
    }
    return avail;
  }

  /**
   * Returns true if we have finished reading the backing EntityStream
   * and all buffered bytes have been read.
   */
  private boolean done()
  {
    return _readFinished && _buffer == null && _buffers.isEmpty();
  }

  /********* Reader Impl *********/

  @Override
  public void onInit(ReadHandle rh)
  {
    _rh = rh;
    _rh.request(CAPACITY);
  }

  @Override
  public void onDataAvailable(ByteString data)
  {
    _buffers.add(data);
  }

  @Override
  public void onDone()
  {
    _readFinished = true;
    // signal waiters that are waiting on _buffers.take().
    _buffers.add(EOF);
  }

  @Override
  public void onError(Throwable e)
  {
    _throwable = e;
    // signal waiters that are waiting on _buffers.take().
    _buffers.add(EOF);
    //TODO: abort the backing EntityStream
  }
}
