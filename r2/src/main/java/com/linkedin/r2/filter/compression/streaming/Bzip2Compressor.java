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

  public Bzip2Compressor(Executor executor)
  {
    _executor = executor;
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
        return new BZip2CompressorOutputStream(out);
      }
    };
  }

}
