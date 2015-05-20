package com.linkedin.r2.filter.compression.streaming;

import java.io.IOException;
import java.io.OutputStream;


/**
 * An {@link OutputStream} which delays compression until the buffer threshold is reached.
 * If {@link #close()} is called prior to threshold being reached, the original(uncompressed)
 * bytes will be written to the destination {@link OutputStream}.
 *
 * @author Ang Xu
 */
public abstract class DelayedCompressionOutputStream extends OutputStream
{
  private final OutputStream _dest;
  private final int _threshold;
  private final byte[] _buffer;
  private int _writeIndex;

  private OutputStream _compression;

  public DelayedCompressionOutputStream(OutputStream dest, int threshold)
  {
    _dest = dest;
    _threshold = threshold;
    _buffer = new byte[threshold];
  }

  @Override
  public void write(int b) throws IOException
  {
    if (_writeIndex < _threshold)
    {
      _buffer[_writeIndex++] = (byte)b;
    }
    else if (_writeIndex == _threshold)
    {
      _compression = compressionOutputStream(_dest);
      _compression.write(_buffer);
      _compression.write(b);
      _writeIndex ++;
    }
    else
    {
      _compression.write(b);
      _writeIndex ++;
    }
  }

  @Override
  public void close() throws IOException
  {
    if (_writeIndex <= _threshold)
    {
      _dest.write(_buffer, 0, _writeIndex);
      _dest.close();
    }
    else
    {
      _compression.close();
    }
  }

  abstract OutputStream compressionOutputStream(OutputStream out) throws IOException;
}
