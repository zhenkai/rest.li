package com.linkedin.r2.message.streaming;

import com.linkedin.r2.message.rest.RestHeaders;

/**
 * @author Zhenkai Zhu
 */
public final class Deciders
{
  @SuppressWarnings("unchecked")
  public static <T extends RestHeaders> Decider<T> yesDecider()
  {
    return (Decider<T>) YES_DECIDER;
  }

  @SuppressWarnings("unchecked")
  public static <T extends RestHeaders> Decider<T> noDecider()
  {
    return (Decider<T>) NO_DECIDER;
  }

  private static final Decider YES_DECIDER = new Decider<RestHeaders>()
  {
    @Override
    public boolean shouldStream(RestHeaders headers)
    {
      return true;
    }
  };

  private static final Decider NO_DECIDER = new Decider<RestHeaders>()
  {
    @Override
    public boolean shouldStream(RestHeaders headers)
    {
      return false;
    }
  };

  private Deciders(){}
}
