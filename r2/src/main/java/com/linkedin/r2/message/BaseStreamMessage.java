package com.linkedin.r2.message;

import com.linkedin.r2.message.streaming.EntityStream;

/**
 * @author Zhenkai Zhu
 */
public abstract class BaseStreamMessage implements StreamMessage
{
  private final EntityStream _entityStream;

  public BaseStreamMessage(EntityStream entityStream)
  {
    _entityStream = entityStream;
  }

  @Override
  public EntityStream getEntityStream()
  {
    return _entityStream;
  }
}
