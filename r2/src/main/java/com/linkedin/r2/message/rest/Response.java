package com.linkedin.r2.message.rest;

import com.linkedin.r2.message.rest.RestMessage;

/**
 * @author Zhenkai Zhu
 */
public interface Response extends RestMessage
{

  /**
   * Returns the status for this response.
   *
   * @return the status for this response
   * @see com.linkedin.r2.message.rest.RestStatus
   */
  int getStatus();
}
