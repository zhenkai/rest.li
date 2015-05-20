package com.linkedin.r2.filter.compression.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Executor;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;


/**
 * @author Ang Xu
 */
public class Bzip2Compressor extends AbstractCompressor
{
  private final Executor _executor;
  private final int _threshold;

  public Bzip2Compressor(Executor executor, int threshold)
  {
    _executor = executor;
    _threshold = threshold;
  }

  @Override
  public String getContentEncodingName()
  {
    return "bzip2";
  }

  @Override
  protected StreamingInflater createInflater()
  {
    return new StreamingInflater(_executor)
    {
      @Override
      protected InputStream createInputStream(InputStream in) throws IOException
      {
        return new BZip2CompressorInputStream(in);
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
          OutputStream compressionOutputStream(OutputStream outputStream)
              throws IOException
          {
            return new BZip2CompressorOutputStream(outputStream);
          }
        };
      }
    };
  }

}
