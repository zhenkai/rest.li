package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.r2.filter.compression.CompressionException;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;


/**
 * @author Ang Xu
 */
public abstract class AbstractCompressor implements StreamingCompressor
{
  @Override
  public EntityStream inflate(EntityStream input)
  {
    StreamingInflater inflater = createInflater();
    input.setReader(inflater);
    return EntityStreams.newEntityStream(inflater);
  }

  @Override
  public EntityStream deflate(EntityStream input)
      throws CompressionException
  {
    StreamingDeflater deflater = createDeflater();
    input.setReader(deflater);
    return EntityStreams.newEntityStream(deflater);
  }

  abstract protected StreamingInflater createInflater();
  abstract protected StreamingDeflater createDeflater();
}
