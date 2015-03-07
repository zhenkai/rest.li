package com.linkedin.r2.message;

import com.linkedin.r2.message.streaming.EntityStream;

/**
 * @author Zhenkai Zhu
 */
public interface StreamMessage
{

  /**
   * Returns the EntityStream for this message. The entity stream can only be read once (i.e. Message
   * does not keep a copy of the whole entity).
   *
   * @return the EntityStream of this message.
   */
  EntityStream getEntityStream();
}
