package com.linkedin.r2.message;

import com.linkedin.r2.message.streaming.EntityStream;

public interface StreamMessageBuilder<B extends StreamMessageBuilder<B>>
{
  StreamMessage build(EntityStream stream);
  StreamMessage buildCanonical(EntityStream stream);
}
