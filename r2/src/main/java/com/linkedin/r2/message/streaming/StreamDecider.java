package com.linkedin.r2.message.streaming;


import com.linkedin.r2.message.rest.RequestHead;

/**
 * @author Zhenkai Zhu
 */
public interface StreamDecider
{
  StreamDecision decide(RequestHead requestHead);

  public enum StreamDecision
  {
    STREAM_ENTITY,
    FULL_ENTITY
  }
}
