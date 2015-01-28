package com.linkedin.r2.message;

import com.linkedin.r2.message.streaming.EntityStream;

/**
 * An object that builds new messages (rest/rpc, request/response).
 *
 * @author Zhenkai Zhu
 */
public interface StreamMessageBuilder<B extends StreamMessageBuilder<B>>
{
  /**
   * Constructs an {@link StreamMessage} using the settings configured in this builder and the supplied EntityStream.
   * Subsequent changes to this builder will not change the underlying message.
   *
   * @return a StreamMessage from the settings in this builder and the supplied EntityStream
   */
  StreamMessage build(EntityStream stream);

  /**
   * Similar to {@link #build}, but the returned StreamMessage is in canonical form.
   *
   * @return a StreamMessage from the settings in this builder and the supplied EntityStream
   */
  StreamMessage buildCanonical(EntityStream stream);
}
