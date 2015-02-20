package com.linkedin.r2.message.streaming;


import com.linkedin.r2.message.rest.RequestHeaders;

/**
 * @author Zhenkai Zhu
 */
public interface StreamDecider
{
  StreamDecision decide(RequestHeaders headers);

  public enum StreamDecision
  {
    STREAM_ENTITY,
    FULL_ENTITY
  }
}
