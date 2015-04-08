package com.linkedin.r2.message.rest;

import com.linkedin.r2.message.rest.RestMessage;

import java.net.URI;

/**
 * @author Zhenkai Zhu
 */
public interface Request extends RestMessage
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
