/*
   Copyright (c) 2015 LinkedIn Corp.

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

package com.linkedin.r2.filter.compression.streaming;


import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.r2.util.LinkedDeque;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Deque;
import org.iq80.snappy.SnappyOutputStream;

/**
 * This class pipes a {@link com.linkedin.r2.message.streaming.EntityStream} to
 * a different {@link com.linkedin.r2.message.streaming.EntityStream} in which
 * the data is compressed.
 *
 * @author Ang Xu
 */
abstract class StreamingDeflater implements Reader, Writer
{
  private final int _threshold;
  private ReadHandle _rh;
  private WriteHandle _wh;
  private OutputStream _out;
  private BufferedWriterOutputStream _writerOutputStream;

  public StreamingDeflater(int threshold)
  {
    _threshold = threshold;
  }

  @Override
  public void onInit(ReadHandle rh)
  {
    _rh = rh;
  }

  @Override
  public void onDataAvailable(ByteString data)
  {
    try
    {
      _out.write(data.copyBytes());
      if (_writerOutputStream.needMore())
      {
        _rh.request(1);
      }
    }
    catch (IOException e)
    {
      _wh.error(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onDone()
  {
    try
    {
      _out.close();
    }
    catch (IOException e)
    {
      _wh.error(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onError(Throwable e)
  {
    _wh.error(e);
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    try
    {
      _wh = wh;
      _writerOutputStream = new BufferedWriterOutputStream();

      if (_threshold > 0)
      {
        _out = new DelayedCompressionOutputStream(_writerOutputStream, _threshold)
        {
          @Override
          OutputStream compressionOutputStream(OutputStream out)
              throws IOException
          {
            return createOutputStream(out);
          }
        };
      }
      else
      {
        _out = createOutputStream(_writerOutputStream);
      }
    }
    catch (IOException e)
    {
      _wh.error(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onWritePossible()
  {
    _writerOutputStream.writeIfPossible();
  }

  @Override
  public void onAbort(Throwable e)
  {
    _wh.error(e);
    //TODO: read out remaining data from ReadHandle
  }

  abstract protected OutputStream createOutputStream(OutputStream out) throws IOException;

  private class BufferedWriterOutputStream extends OutputStream
  {
    private static final int BUF_SIZE = 4096;

    private final Deque<ByteString> _data = new LinkedDeque<ByteString>();
    private final byte[] _buffer = new byte[BUF_SIZE];
    private int _writeIndex = 0;
    private boolean _done = false;

    @Override
    public void write(int b) throws IOException
    {
      _buffer[_writeIndex++] = (byte) b;
      if (_writeIndex == BUF_SIZE)
      {
        _data.add(ByteString.copy(_buffer));
        _writeIndex = 0;
        writeIfPossible();
      }
    }

    @Override
    public void close() throws IOException
    {
      if (_writeIndex > 0) // flush remaining bytes
      {
        _data.add(ByteString.copy(_buffer, 0, _writeIndex));
      }
      _done = true;
      writeIfPossible();
    }

    public void writeIfPossible()
    {
      while(_wh.remaining() > 0)
      {
        if (_data.isEmpty())
        {
          if (_done)
          {
            _wh.done();
          }
          else
          {
            _rh.request(1);
          }
          return;
        }
        else
        {
          _wh.write(_data.poll());
        }
      }
    }

    public boolean needMore()
    {
      return _wh.remaining() > 0 && _data.isEmpty();
    }
  }
}
