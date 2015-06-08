package com.linkedin.r2.message.rest;

/**
 * @author Zhenkai Zhu
 */
public interface ResponseHead extends RestHead
{

  /**
   * Returns the status for this response.
   *
   * @return the status for this response
   * @see com.linkedin.r2.message.rest.RestStatus
   */
  int getStatus();
}
