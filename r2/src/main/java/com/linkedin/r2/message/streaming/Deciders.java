package com.linkedin.r2.message.streaming;

import com.linkedin.r2.message.rest.RequestHeaders;

/**
 * @author Zhenkai Zhu
 */
public final class Deciders
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
    public StreamDecision decide(RequestHeaders headers)
    {
      return StreamDecision.STREAM_ENTITY;
    }
  };

  private static final StreamDecider NO_STREAM = new StreamDecider()
  {
    @Override
    public StreamDecision decide(RequestHeaders headers)
    {
      return StreamDecision.FULL_ENTITY;
    }
  };

  private Deciders(){}
}
