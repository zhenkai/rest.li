package com.linkedin.r2.message.streaming;

import com.linkedin.r2.message.rest.RequestHead;

/**
 * @author Zhenkai Zhu
 */
public final class StreamDeciders
{
  public static StreamDecider alwaysStream()
  {
    return ALWAYS_STREAM;
  }

  public static StreamDecider noStream()
  {
    return NO_STREAM;
  }


  private static final StreamDecider ALWAYS_STREAM = new StreamDecider()
  {
    @Override
    public StreamDecision decide(RequestHead head)
    {
      return StreamDecision.STREAM_ENTITY;
    }
  };

  private static final StreamDecider NO_STREAM = new StreamDecider()
  {
    @Override
    public StreamDecision decide(RequestHead head)
    {
      return StreamDecision.FULL_ENTITY;
    }
  };

  private StreamDeciders(){}
}
