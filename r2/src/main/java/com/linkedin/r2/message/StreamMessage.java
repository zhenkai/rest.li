package com.linkedin.r2.message;

import com.linkedin.r2.message.streaming.EntityStream;

/**
 * An object that represents a stream message, either request or response.
 */
public interface StreamMessage
{
  /**
   * returns the EntityStream for this message.
   * @return the EntityStream of this message.
   */
  EntityStream getEntityStream();

  /**
   * Returns a {@link StreamMessageBuilder}, which provides a means of constructing a new message using
   * this message as a starting point. Changes made with the builder are not reflected by this
   * message instance.
   * @return the builder of this message.
   */
  StreamMessageBuilder<? extends StreamMessageBuilder<?>> builder();
}
