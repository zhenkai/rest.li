package com.linkedin.r2.message;

import java.net.URI;

/**
 * This interface represents basic contract for request.
 *
 * @see com.linkedin.r2.message.rest.RestRequest
 * @see com.linkedin.r2.message.stream.StreamRequest
 * @author Zhenkai Zhu
 */
public interface Request extends MessageHeaders
{
  /**
   * Returns the REST method for this request.
   *
   * @return the REST method for this request
   * @see com.linkedin.r2.message.rest.RestMethod
   */
  String getMethod();

  /**
   * Returns the URI for this request.
   *
   * @return the URI for this request
   */
  URI getURI();
}
