package com.linkedin.r2.message;

/**
 * This interface represents the basic contract for response.
 *
 * @see com.linkedin.r2.message.rest.RestResponse
 * @see com.linkedin.r2.message.stream.StreamResponse
 *
 * @author Zhenkai Zhu
 */
public interface Response extends MessageHeaders
{

  /**
   * Returns the status for this response.
   *
   * @return the status for this response
   * @see com.linkedin.r2.message.rest.RestStatus
   */
  int getStatus();
}
