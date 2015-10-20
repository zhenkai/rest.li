package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.r2.message.stream.entitystream.EntityStream;


/**
 * @author Ang Xu
 */
public class NoopCompressor implements StreamingCompressor
{
  @Override
  public String getContentEncodingName()
  {
    return StreamEncodingType.IDENTITY.getHttpName();
  }

  @Override
  public EntityStream inflate(EntityStream input)
  {
    return input;
  }

  @Override
  public EntityStream deflate(EntityStream input)
  {
    return input;
  }
}
