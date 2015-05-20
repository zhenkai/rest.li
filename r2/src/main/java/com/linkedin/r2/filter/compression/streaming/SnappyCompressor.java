package com.linkedin.r2.filter.compression.streaming;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Executor;
import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;


/**
 * @author Ang Xu
 */
public class SnappyCompressor extends AbstractCompressor
{
  private final Executor _executor;
  private final int _threshold;

  public SnappyCompressor(Executor executor, int threshold)
  {
    _executor = executor;
    _threshold = threshold;
  }

  @Override
  public String getContentEncodingName()
  {
    return "snappy-stream";
  }

  @Override
  protected StreamingInflater createInflater()
  {
    return new StreamingInflater(_executor)
    {
      @Override
      protected InputStream createInputStream(InputStream in) throws IOException
      {
        return new SnappyInputStream(in);
      }
    };
  }

  @Override
  protected StreamingDeflater createDeflater()
  {
    return new StreamingDeflater()
    {
      @Override
      protected OutputStream createOutputStream(OutputStream out) throws IOException
      {
        return new DelayedCompressionOutputStream(out, _threshold)
        {
          @Override
          OutputStream compressionOutputStream(OutputStream outputStream) throws IOException
          {
            return new SnappyOutputStream(outputStream);
          }
        };
      }
    };
  }
}
